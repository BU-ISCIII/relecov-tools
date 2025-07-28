import rich.console
import json
import pandas as pd
import sys
import os
import ftplib
import time
import relecov_tools.utils
from datetime import datetime
from relecov_tools.config_json import ConfigJson
from relecov_tools.base_module import BaseModule

from ena_upload.ena_upload import extract_targets
from ena_upload.ena_upload import run_construct
from ena_upload.ena_upload import construct_submission
from ena_upload.ena_upload import send_schemas
from ena_upload.ena_upload import process_receipt
from ena_upload.ena_upload import update_table
from ena_upload.ena_upload import update_table_simple

pd.options.mode.chained_assignment = None

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class EnaUpload(BaseModule):
    def __init__(
        self,
        user=None,
        password=None,
        center=None,
        ena_json=None,
        template_path=None,
        dev=None,
        action=None,
        metadata_types=None,
        upload_fastq=None,
        output_dir=None,
    ):
        super().__init__(output_dir=output_dir, called_module="upload-to-ena")
        if user is None:
            self.user = relecov_tools.utils.prompt_text(
                msg="Enter your username defined in ENA"
            )
        else:
            self.user = user
        if password is None:
            self.passwd = relecov_tools.utils.prompt_password(
                msg="Enter your password to ENA"
            )
        else:
            self.passwd = password
        if center is None:
            self.center = relecov_tools.utils.prompt_text(msg="Enter your center name")
        else:
            self.center = center
        if ena_json is None:
            self.source_json_file = relecov_tools.utils.prompt_path(
                msg="Select the ENA json file to upload"
            )
        else:
            self.source_json_file = ena_json
        if not os.path.exists(self.source_json_file):
            self.log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"[red]json data file {self.source_json_file} does not exist")
            sys.exit(1)
        if template_path is None:
            self.template_path = relecov_tools.utils.prompt_path(
                msg="Select the folder containing ENA templates"
            )
        # e.g. template_folder = "/home/user/github_repositories/relecov-tools/relecov_tools/assets/ena_templates"
        else:
            self.template_path = template_path
        if not os.path.exists(self.template_path):
            stderr.print("[red]Error: ENA template folder does not exist")
            sys.exit(1)
        if dev is None:
            self.dev = relecov_tools.utils.prompt_yn_question(
                msg="Do you want to test upload data?"
            )
        else:
            self.dev = dev
        if action is None:
            self.action = relecov_tools.utils.prompt_selection(
                msg="Select the action to upload to ENA",
                choices=["ADD", "MODIFY", "CANCEL", "RELEASE"],
            )
        elif action.upper() not in ["ADD", "MODIFY", "CANCEL", "RELEASE"]:
            stderr.print(f"[red] Action '{action}' not supported")
            sys.exit(1)
        else:
            self.action = action.upper()

        if output_dir is None:
            self.output_dir = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the xml files"
            )
        else:
            self.output_dir = output_dir

        self.upload_fastq_files = upload_fastq

        all_metadata_types = ["study", "run", "experiment", "sample"]
        if metadata_types is None:
            # If not specified, all metadata xmls are generated and submitted
            self.metadata_types = all_metadata_types
        else:
            self.metadata_types = metadata_types.split(",")
            if not all(xml in all_metadata_types for xml in self.metadata_types):
                wrong_types = [
                    xml for xml in self.metadata_types if xml not in all_metadata_types
                ]
                self.log.error("Unsupported metadata xml types: " + str(wrong_types))
                stderr.print(f"[red]Unsupported metadata xml types: {wrong_types}")
                sys.exit(1)

        config_json = ConfigJson()
        self.config_json = config_json
        self.checklist = self.config_json.get_configuration("upload_to_ena")[
            "checklist"
        ]
        with open(self.source_json_file, "r") as fh:
            json_data = json.loads(fh.read())
            self.json_data = json_data
        batch_id = self.get_batch_id_from_data(self.json_data)
        self.set_batch_id(batch_id)
        if self.dev:
            self.url = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"
        else:
            self.url = "https://www.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"

    def table_formatting(self, schemas_dataframe_raw, source):
        """Some fields in the dataframe need special formatting"""
        formated_df = schemas_dataframe_raw[source]
        formated_df.insert(3, "status", self.action)
        formated_df.rename(
            columns={
                (str(source) + "_alias"): "alias",
                (str(source) + "_title"): "title",
            },
            inplace=True,
        )
        if self.action in ["CANCEL", "MODIFY", "RELEASE"]:
            formated_df.rename(
                columns={"ena_" + str(source) + "_accession": "accession"}, inplace=True
            )
        if source == "study":
            formated_df = formated_df.drop_duplicates(subset=["alias"])
        if source == "sample":
            formated_df.insert(4, "ENA_CHECKLIST", self.checklist)
        """
        file_name and file_checksum are fields with a structure like this:
        file_nameR1--file_nameR2 / file_checksumR1--file_checksumR2
        The run table needs a row for each strand, so these fields are splitted
        """
        if source == "run":
            formated_df["file_name"] = formated_df["file_name"].str.split("--")
            formated_df = formated_df.explode("file_name").reset_index(drop=True)
            formated_df["file_checksum"] = [
                x.split("--")[0] if index % 2 == 0 else x.split("--")[1]
                for index, x in enumerate(formated_df["file_checksum"])
            ]

        return formated_df

    def dataframes_from_json(self, json_data):
        """The xml is built using a dictionary of dataframes as a base structure"""
        source_options = self.metadata_types
        schemas_dataframe = {}
        schemas_dataframe_raw = {}
        acces_fields = self.config_json.get_topic_data(
            "upload_to_ena", "accession_fields"
        )
        filtered_access_fields = [
            fd for fd in acces_fields if any(source in fd for source in source_options)
        ]
        all_missing_accessions = []
        if self.action in ["CANCEL", "MODIFY", "RELEASE"]:
            for source in source_options:
                missing_accessions = [
                    samp["sample_title"]
                    for samp in json_data
                    for fd in filtered_access_fields
                    if (source in fd and fd not in samp.keys())
                ]
                if missing_accessions:
                    self.log.error(
                        "Found samples in json without proper ena accessions"
                    )
                    stderr.print(f"[red]Found samples missing {source} accession ids:")
                all_missing_accessions.extend(missing_accessions)
            if all_missing_accessions:
                stderr.print("Not committed samples:\n", all_missing_accessions)

        for source in source_options:
            source_topic = "_".join(["df", source, "fields"])
            source_fields = self.config_json.get_topic_data(
                "upload_to_ena", source_topic
            )
            if self.action in ["CANCEL", "MODIFY", "RELEASE"]:
                source_fields.append(str("ena_" + source + "_accession"))
            source_dict = {
                field: [
                    sample[field]
                    for sample in json_data
                    if sample["sample_title"] not in all_missing_accessions
                ]
                for field in source_fields
            }
            schemas_dataframe_raw[source] = pd.DataFrame.from_dict(source_dict)
            schemas_dataframe[source] = self.table_formatting(
                schemas_dataframe_raw, source
            )
        return schemas_dataframe

    def save_tables(self, schemas_dataframe, date):
        """Save the dataframes into csv files"""
        stderr.print(f"Saving dataframes in {self.output_dir}")
        for source, table in schemas_dataframe.items():
            table_name = str(self.output_dir + source + date + "_table.csv")
            table.to_csv(table_name, sep=",")

    def update_json(self, updated_schemas_df, json_data):
        access_dict = {}
        updated_json_data = json_data.copy()
        for source, table in updated_schemas_df.items():
            access_list = [x for x in table["accession"]]
            access_dict[source] = access_list
            """run accessions are duplicated for R1/R2 so they need to be removed"""
            if source == "run":
                del access_dict[source][1::2]
        for source, acclist in access_dict.items():
            accession_field_name = str("ena_" + source + "_accession")
            for sample, accession in zip(updated_json_data, acclist):
                sample[accession_field_name] = accession
        return updated_json_data

    def xml_submission(self, json_data, schemas_dataframe, batch_index=None):
        """The metadata is submitted in an xml format"""
        schema_targets = extract_targets(self.action, schemas_dataframe)

        tool = self.config_json.get_configuration("upload_to_ena")["tool"]

        if self.action in ["ADD", "MODIFY"]:
            schema_xmls = run_construct(
                self.template_path, schema_targets, self.center, self.checklist, tool
            )
            submission_xml = construct_submission(
                self.template_path,
                self.action,
                schema_xmls,
                self.center,
                self.checklist,
                tool,
            )
        elif self.action in ["CANCEL", "RELEASE"]:
            # when CANCEL/RELEASE, only the accessions are needed
            # schema_xmls is only used to record the following 'submission'
            schema_xmls = {}
            submission_xml = construct_submission(
                self.template_path,
                self.action,
                schema_targets,
                self.center,
                self.checklist,
                tool,
            )
        schema_xmls["submission"] = submission_xml

        stderr.print(f"\nProcessing submission to ENA server: {self.url}")

        receipt = send_schemas(schema_xmls, self.url, self.user, self.passwd).text
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        date = str(datetime.now().strftime("%Y%m%d-%H%M%S"))
        receipt_name = "receipt_" + date + ".xml"
        receipt_dir = os.path.join(self.output_dir, receipt_name)
        stderr.print(f"Printing receipt to {receipt_dir}")

        with open(f"{receipt_dir}", "w") as fw:
            fw.write(receipt)
        try:
            schema_update = process_receipt(receipt.encode("utf-8"), self.action)
        except ValueError:
            self.log.error("There was an ERROR during submission:")
            sys.exit(receipt)
        if str(self.action) in ["ADD", "MODIFY"]:
            updated_schemas_df = update_table(
                schemas_dataframe, schema_targets, schema_update
            )
        else:
            updated_schemas_df = update_table_simple(
                schemas_dataframe, schema_targets, self.action
            )

        updated_json = self.update_json(updated_schemas_df, json_data)
        if batch_index is None:
            suffix = str("_" + date + ".json")
        else:
            suffix = str("_" + date + "_batch" + str(batch_index) + ".json")
        updated_json_name = (
            os.path.splitext(os.path.basename(self.source_json_file))[0] + suffix
        )
        relecov_tools.utils.write_json_to_file(updated_json, updated_json_name)

        self.save_tables(updated_schemas_df, date)
        return

    def fastq_submission(self, json_data, max_retries=3, retry_delay=5):
        """The fastq files are submitted apart from the metadata"""
        stderr.print("Submitting fastq files")
        json_dataframe = pd.DataFrame(json_data)
        file_paths = {}
        file_paths_r2 = {}
        for path in json_dataframe["r1_fastq_filepath"]:
            file_paths[os.path.basename(path)] = os.path.abspath(path)

        for path in json_dataframe["r2_fastq_filepath"]:
            file_paths_r2[os.path.basename(path)] = os.path.abspath(path)

        file_paths.update(file_paths_r2)

        connection_retries = 0
        while connection_retries < 3:
            try:
                session = ftplib.FTP(
                    "webin2.ebi.ac.uk", self.user, self.passwd, timeout=60
                )
                session.login(self.user, self.passwd)
                break
            except ftplib.all_errors as e:
                stderr.print(
                    f"Connection attempt {connection_retries+1} failed: {e}. Retrying..."
                )
                connection_retries += 1
                time.sleep(retry_delay)
        self.upload_files_with_retries(session, file_paths, max_retries, retry_delay)

        try:
            session.quit()
        except ftplib.all_errors as e:
            stderr.print(f"ERROR: Could not close FTP session properly: {e}")

    def upload_files_with_retries(
        self, session, file_paths, max_retries=3, retry_delay=5
    ):
        for filename, path in file_paths.items():
            stderr.print(f"Uploading path: {path} with filename: {filename}")
            retries = 0
            while retries < max_retries:
                try:
                    # Reopen file in each retry
                    with open(path, "rb") as file:
                        response = session.storbinary(f"STOR {filename}", file)

                        # Verify if the upload was successful (using response code)
                        if response.startswith("226"):
                            stderr.print(f"File {filename} uploaded successfully.")
                            break
                        else:
                            raise ftplib.error_perm(f"Unexpected response: {response}")
                except (
                    ftplib.error_temp,
                    ftplib.error_perm,
                    ftplib.socket.error,
                    ftplib.error_proto,
                ) as e:
                    # Error handling related to FTP
                    retries += 1
                    stderr.print(f"FTP error: {e}. Retry {retries}/{max_retries}")
                    # Reopen connection if failed due to timeout or temporary error
                    if (
                        isinstance(e, ftplib.error_temp)
                        or "timed out" in str(e).lower()
                    ):
                        stderr.print("Reinitializing FTP connection...")
                        try:
                            session.quit()
                        except Exception:
                            pass
                        time.sleep(retry_delay)

                        connection_retries = 0
                        while connection_retries < 3:
                            try:
                                session = ftplib.FTP(
                                    "webin2.ebi.ac.uk",
                                    self.user,
                                    self.passwd,
                                    timeout=60,
                                )
                                session.login(self.user, self.passwd)
                                break
                            except ftplib.all_errors as e:
                                stderr.print(
                                    f"Connection attempt {connection_retries+1} failed: {e}. Retrying..."
                                )
                                connection_retries += 1
                                time.sleep(retry_delay)
                        if connection_retries == 3:
                            stderr.print(f"Failed to reconnect after {3} attempts.")
                            break
                except Exception as e:
                    # Handling of any other unexpected errors
                    retries += 1
                    stderr.print(
                        f"Unexpected error: {e}. Retry {retries}/{max_retries}"
                    )
                    time.sleep(retry_delay)
            else:
                stderr.print(
                    f"Failed to upload {filename} after {max_retries} retries."
                )
                session.quit()

    def large_json_upload(self, json_data):
        """
        Split large json into smaller jsons of maximum size 20
        due to limitations in submissions to ENA's API
        """
        ena_api_limit = 20
        number_of_batchs = len(range(0, len(json_data), ena_api_limit))
        stderr.print(f"Splitting the json data in {number_of_batchs} batchs...")
        for index in range(0, len(json_data), ena_api_limit):
            batch_index = json_data[index : index + ena_api_limit]
            stderr.print(f"[blue]Processing batch {batch_index}...")
            self.standard_upload(batch_index)
        return

    def standard_upload(self, json_data, batch_index=None):
        """Create the required files and upload to ENA"""
        schemas_dataframe = self.dataframes_from_json(json_data)
        stderr.print("[blue]Successfull creation of dataframes")
        if self.upload_fastq_files:
            self.fastq_submission(json_data)
        stderr.print("Preparing xml files for submission...")
        self.xml_submission(json_data, schemas_dataframe, batch_index)
        return

    def upload(self):
        """Handle the data and upload it to ENA"""
        if len(self.json_data) <= 50:
            self.standard_upload(self.json_data)
        else:
            stderr.print("[yellow]Json is too large to be submitted. Splitting it...")
            self.large_json_upload(self.json_data)
        stderr.print("[green] Finished execution")
        return
