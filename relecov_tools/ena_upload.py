import logging
import rich.console
import json

# import paramiko
import pandas as pd
import sys
import os
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

from ena_upload.ena_upload import extract_targets
from ena_upload.ena_upload import submit_data
from ena_upload.ena_upload import run_construct
from ena_upload.ena_upload import construct_submission
from ena_upload.ena_upload import send_schemas
from ena_upload.ena_upload import process_receipt
from ena_upload.ena_upload import update_table

# from ena_upload.ena_upload import save_update
import site

template_path = os.path.join(site.getsitepackages()[0], "ena_upload", "templates")


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
        dev=None,
        customized_project=None,
        action=None,
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
        if dev is None:
            self.dev = relecov_tools.utils.prompt_yn_question(
                msg="Do you want to test upload data?"
            )
        else:
            self.dev = dev
        if customized_project is None:
            self.customized_project = None
        else:
            self.customized_project = customized_project
        if action is None:
            self.action = relecov_tools.utils.prompt_selection(
                msg="Select the action to upload to ENA",
                choices=["add", "modify", "cancel", "release"],
            )
        else:
            self.action = action.upper()
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the xml files"
            )
        else:
            self.output_path = output_path

        if not os.path.isfile(self.source_json_file):
            log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"[red]json data file {self.source_json_file} does not exist")
            sys.exit(1)
        with open(self.source_json_file, "r") as fh:
            self.json_data = json.loads(fh.read())

    def convert_input_json_to_ena(self):
        """Split the input ena json, in samples and runs json"""
        pass

    def create_structure_to_ena(self):
        """Convert json to dataframe required by ena-upload-cli package"""

        esquema = self.source_json_file
        fh_esquema = open(esquema)
        esquema_json = json.load(fh_esquema)
        fh_esquema.close()

        df_schemas = pd.DataFrame(esquema_json)

        # df_transposed = df_schemas.T
        df_transposed = df_schemas
        df_study = df_transposed[
            ["study_alias", "study_title", "study_type", "study_abstract"]
        ]
        # df_study.columns.values[0] = "alias"
        # df_study.columns.values[1] = "title"

        df_study = df_study.rename(columns={"study_alias": "alias"})
        df_study = df_study.rename(columns={"study_title": "title"})
        df_study.insert(3, "status", self.action)
        df_samples = df_transposed[
            [
                "sample_name",
                "sample_title",
                "taxon_id",
                "collection_date",
                "geographic_location_(country_and/or_sea)",
                "host_common_name",
                "host_scientific_name",
                "host_sex",
                "scientific_name",
                "collector_name",
                "collecting_institution",
                "isolate",
            ]
        ]
        # df_samples.columns.values[0] = "alias"
        # df_samples.columns.values[1] = "title"

        df_samples = df_samples.rename(columns={"sample_name": "alias"})
        df_samples = df_samples.rename(columns={"sample_title": "title"})
        df_samples.insert(3, "status", self.action)
        config_json = ConfigJson()
        checklist = config_json.get_configuration("checklist")
        df_samples.insert(4, "ENA_CHECKLIST", checklist)

        df_run = df_transposed[
            [
                "experiment_alias",
                "sequence_file_R1_fastq",
                "sequence_file_R2_fastq",
                "r1_fastq_filepath",
                "r2_fastq_filepath",
                "file_type",
                "fastq_r1_md5",
                "fastq_r2_md5",
            ]
        ]
        df_run.insert(3, "status", self.action)
        df_run.insert(4, "alias", df_run["experiment_alias"])
        df_run.insert(4, "file_name", df_run["sequence_file_R1_fastq"])

        df_experiments = df_transposed[
            [
                "experiment_alias",
                "study_title",
                "study_alias",
                "sample_name",
                "library_name",
                "library_strategy",
                "library_source",
                "library_selection",
                "library_layout",
                "instrument_model",
            ]
        ]
        df_experiments.insert(3, "status", self.action)
        df_experiments.insert(4, "alias", df_experiments["experiment_alias"])

        # df_experiments.columns.values[0] = "title"
        # df_experiments.columns.values[1] = "sample_alias"

        df_experiments = df_experiments.rename(columns={"study_title": "title"})
        df_experiments = df_experiments.rename(columns={"sample_name": "sample_alias"})

        ena_config = config_json.get_configuration("ENA_configuration")
        schema_dataframe = {}
        schema_dataframe["sample"] = df_samples
        schema_dataframe["run"] = df_run
        schema_dataframe["experiment"] = df_experiments
        schema_targets = extract_targets(self.action, schema_dataframe)

        if ena_config["study_id"] is not None:
            schema_dataframe["study"] = df_study

        if self.action == "ADD" or self.action == "add":
            file_paths = {}

            for path in df_run["r1_fastq_filepath"]:
                file_paths[os.path.basename(path)] = os.path.abspath(path)

            # submit data to webin ftp server

            chec = submit_data(file_paths, self.passwd, self.user)
            print(chec)

            # when ADD/MODIFY,
            # requires source XMLs for 'run', 'experiment', 'sample', 'experiment'
            # schema_xmls record XMLs for all these schema and following 'submission'

            tool = config_json.get_configuration("tool")

            print(checklist)
            schema_xmls = run_construct(
                template_path, schema_targets, self.center, checklist, tool
            )

            # submission_xml = construct_submission(template_path, self.action, schema_xmls, self.center, checklist, tool)
            submission_xml = construct_submission(
                template_path, self.action, schema_xmls, self.center, checklist, tool
            )
            schema_xmls["submission"] = submission_xml

            if self.dev:
                url = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"
            else:
                url = "https://www.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"

            print(f"\nSubmitting XMLs to ENA server: {url}")
            receipt = send_schemas(schema_xmls, url, self.user, self.passwd).text
            print("Printing receipt to ./receipt.xml")

            with open("receipt.xml", "w") as fw:
                fw.write(receipt)
            try:
                schema_update = process_receipt(receipt.encode("utf-8"), self.action)
            except ValueError:
                log.error("There was an ERROR during submission:")
                sys.exit(receipt)

            if self.action in ["ADD", "MODIFY"] or self.action in ["add", "modify"]:
                schema_dataframe = update_table(
                    schema_dataframe, schema_targets, schema_update
                )

            # save updates in new tables
            # save_update(schema_tables, schema_dataframe)

    def upload(self):
        """Create the required files and upload to ENA"""
        self.convert_input_json_to_ena()
        self.create_structure_to_ena()
