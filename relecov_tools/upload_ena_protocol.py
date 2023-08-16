# from distutils.errors import LibError
import logging

# from re import template

# from pyparsing import col
import xml.etree.ElementTree as ET
import rich.console
import json


import pandas as pd


import sys
import os

import ftplib
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

from ena_upload.ena_upload import extract_targets
from ena_upload.ena_upload import run_construct
from ena_upload.ena_upload import construct_submission
from ena_upload.ena_upload import send_schemas
from ena_upload.ena_upload import process_receipt
from ena_upload.ena_upload import update_table
from ena_upload.ena_upload import update_table_simple

import site

pd.options.mode.chained_assignment = None

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

class EnaUpload:
    def __init__(
        self,
        user=None,
        passwd=None,
        center=None,
        source_json=None,
        template_path=None,
        dev=None,
        action=None,
        accession=None,
        output_path=None,
    ):
        if user is None:
            self.user = relecov_tools.utils.prompt_text(
                msg="Enter your username defined in ENA"
            )
        else:
            self.user = user
        if passwd is None:
            self.passwd = relecov_tools.utils.prompt_password(
                msg="Enter your password to ENA"
            )
        else:
            self.passwd = passwd
        if center is None:
            self.center = relecov_tools.utils.prompt_text(msg="Enter your center name")
        else:
            self.center = center
        if source_json is None:
            self.source_json_file = relecov_tools.utils.prompt_path(
                msg="Select the ENA json file to upload"
            )
        else:
            self.source_json_file = source_json
        if template_path is None:
            self.template_path = relecov_tools.utils.prompt_path(
                msg="Select the folder containing ENA templates"
            )
        #template_folder = "/home/user/git_repositories/relecov-tools/relecov_tools/templates"
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
        if accession == "empty":
            self.accession = relecov_tools.utils.prompt_yn_question(
                msg="Select the accession number for the submission"
            )
        elif accession == "false":
            self.accession = False
        else:
            self.accession = accession
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
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the xml files"
            )
        else:
            self.output_path = output_path

        config_json = ConfigJson()
        self.config_json = config_json

        self.checklist = self.config_json.get_configuration("ENA_fields")["checklist"]

        if not os.path.isfile(self.source_json_file):
            log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"[red]json data file {self.source_json_file} does not exist")
            sys.exit(1)
                
        with open(self.source_json_file, "r") as fh:
            json_data = json.loads(fh.read())
            self.json_data = json_data

        if self.dev:
            self.url = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"
        else:
            self.url = "https://www.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"

    def table_formatting(self, schemas_dataframe, source):
        """Some fields in the dataframe need special formatting"""
        formated_df = schemas_dataframe[source]
        formated_df.insert(3, "status", self.action)
        formated_df.rename(
            columns = {(str(source)+"_alias"):"alias",
                    (str(source)+"_title"):"title"},
            inplace = True
        )
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
                x[1].split("--")[0] if x[0]%2 == 0 else x[1].split("--")[1]
                for x in enumerate(formated_df["file_checksum"])
                ]
        if source == "study":
            formated_df = formated_df.drop_duplicates(subset=["alias"])
            stderr.print("study table:", formated_df)

        if isinstance(self.accession, str):
            formated_df["accession"] = self.accession

        schemas_dataframe[source] = formated_df
        return schemas_dataframe

    def dataframes_from_json(self, json_data):
        """The xml is built using a dictionary of dataframes as a base structure"""
        source_options = ["study", "sample", "run", "experiment"]
        schemas_dataframe_raw = {}

        for source in source_options:
            source_topic = "_".join(["df",source,"fields"])
            source_fields = self.config_json.get_topic_data("ENA_fields", source_topic)
            source_dict = {field: [sample[field] for sample in json_data]
                            for field in source_fields}
            schemas_dataframe_raw[source] = pd.DataFrame.from_dict(source_dict)
            schemas_dataframe = self.table_formatting(schemas_dataframe_raw, source)

        return schemas_dataframe
    
    def save_tables(self, schemas_dataframe):
        """Save the dataframes into csv files"""
        stderr.print(f"Saving dataframes in {self.output_path}")
        for source, table in schemas_dataframe.items():
            table_name = str(self.output_path+source+"_table.csv")
            table.to_csv(table_name, sep=",")

    
    def xml_submission(self, schemas_dataframe):
        """The metadata is submitted in an xml format"""

        schema_targets = extract_targets(self.action, schemas_dataframe)

        tool = self.config_json.get_configuration("ENA_fields")["tool"]

        if self.action in ["ADD", "MODIFY"]:
            schema_xmls = run_construct(
                self.template_path, schema_targets, self.center, self.checklist, tool
            )
            submission_xml = construct_submission(
                self.template_path, self.action, schema_xmls, self.center, self.checklist, tool
            )
        elif self.action in ['CANCEL', 'RELEASE']:
        # when CANCEL/RELEASE, only the accessions are needed
        # schema_xmls is only used to record the following 'submission'
            schema_xmls = {}    
            submission_xml = construct_submission(self.template_path, self.action,
                                    schema_targets, self.center, self.checklist, tool)

        schema_xmls["submission"] = submission_xml

        """Tree writes an xml file for the run fields"""
        """tree = ET.parse(schema_xmls["run"])
        tree.write(schema_xmls["run"])"""

        print(f"\nSubmitting XMLs to ENA server: {self.url}")
        receipt = send_schemas(schema_xmls, self.url, self.user, self.passwd).text
        if not os.path.exists(self.output_path):
            os.mkdir(self.output_path)
        receipt_dir = os.path.join(self.output_path, "receipt.xml")
        print(f"Printing receipt to {receipt_dir}")

        with open(f"{receipt_dir}", "w") as fw:
                fw.write(receipt)
        try:
            schema_update = process_receipt(receipt.encode("utf-8"), self.action)
        except ValueError:
            log.error("There was an ERROR during submission:")
            sys.exit(receipt)

        if str(self.action) in ["ADD", "MODIFY"]:
            schemas_dataframe = update_table(
                schemas_dataframe, schema_targets, schema_update
            )
        else:
            schemas_dataframe = update_table_simple(schemas_dataframe,
                                               schema_targets,
                                               self.action)
        self.save_tables(schemas_dataframe)
        return
    
    def fastq_submission(self, json_data):
        """The fastq files are submitted apart from the metadata"""
        stderr.print(f"Submitting fastq files")
        json_dataframe = pd.DataFrame(json_data)
        file_paths = {}
        file_paths_r2 = {}
        for path in json_dataframe["r1_fastq_filepath"]:
            file_paths[os.path.basename(path)] = os.path.abspath(path)

        for path in json_dataframe["r2_fastq_filepath"]:
            file_paths_r2[os.path.basename(path)] = os.path.abspath(path)

        file_paths.update(file_paths_r2)

        session = ftplib.FTP("webin2.ebi.ac.uk", self.user, self.passwd)
        for filename, path in file_paths.items():
            print("Uploading path " + path + " and filename: " + filename)
            try:
                file = open(path, "rb")  # file to send
                g = session.storbinary(f"STOR {filename}", file)
                print(g)  # send the file
                file.close()  # close file and FTP
            except BaseException as err:
                print(f"ERROR: {err}")
                # print("ERROR: If your connection times out at this stage, it propably is because of a firewall that is in place. FTP is used in passive mode and connection will be opened to one of the ports: 40000 and 50000.")
        g2 = session.quit()
        print(g2)

        return

    def upload(self):
        """Create the required files and upload to ENA"""
        schemas_dataframe = self.dataframes_from_json(self.json_data)
        self.xml_submission(schemas_dataframe)
        self.fastq_submission(self.json_data)

        stderr.print(f"[green] Finished execution")
        return
