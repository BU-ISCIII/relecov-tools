import logging
import rich.console
import json

import pandas as pd
import sys
import os
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson


from ena_upload.ena_upload import extract_targets
from ena_upload.ena_upload import submit_data
from ena_upload.ena_upload import run_construct
from ena_upload.ena_upload import construct_submission


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
        # check if data is given when adding a 'run' table

    def convert_input_json_to_ena(self):
        """Split the input ena json, in samples and runs json"""
        pass

    def create_structure_to_ena(self):
        """Convert json to dataframe required by ena-upload-cli package"""
        # schema_dataframe = {}

        # config_json = ConfigJson()

        esquema = self.source_json_file
        fh_esquema = open(esquema)
        esquema_json = json.load(fh_esquema)
        fh_esquema.close()

        df_schemas = pd.DataFrame.from_dict(esquema_json, orient="index")
        df_transposed = df_schemas.T
        df_study = df_transposed[["study_alias", "study_title", "study_type"]]
        df_study.insert(3, "status", self.action)
        df_samples = df_transposed[
            [
                "sample_name",
                "tax_id",
                "sample_description",
                "collection_date",
                "geographic_location_(country_and/or_sea)",
                "host_common_name",
                "host_gender",
                "host_scientific_name",
                "isolate",
            ]
        ]
        df_samples.insert(3, "status", self.action)
        df_run = df_transposed[
            [
                "experiment_alias",
                "sequence_file_R1_fastq",
                "sequence_file_R2_fastq",
                "r1_fastq_filepath",
                "r2_fastq_filepath",
            ]
        ]
        df_run.insert(3, "status", self.action)

        df_experiments = df_transposed[
            [
                "experiment_alias",
                "study_title",
                "sample_name",
                "library_strategy",
                "library_source",
                "library_selection",
                "library_layout",
                "instrument_platform",
                "instrument_model",
            ]
        ]
        df_experiments.insert(3, "status", self.action)

        schema_dataframe = {}
        schema_dataframe["study"] = df_study
        schema_dataframe["samples"] = df_samples
        schema_dataframe["run"] = df_run
        schema_dataframe["experiments"] = df_experiments

        schema_targets = extract_targets(self.action, schema_dataframe)

        if self.action == "ADD" or self.action == "add":
            file_paths = {}

            for path in df_run["r1_fastq_filepath"]:
                file_paths[os.path.basename(path)] = os.path.abspath(path)

            # submit data to webin ftp server
            if self.dev:
                print(
                    "No files will be uploaded, remove `--no_data_upload' argument to perform upload."
                )

            else:

                submit_data(file_paths, self.passwd, self.user)

            # when ADD/MODIFY,
            # requires source XMLs for 'run', 'experiment', 'sample', 'experiment'
            # schema_xmls record XMLs for all these schema and following 'submission'

            # No me está funcionando con el absolute path
            base_path = os.path.abspath(os.path.dirname(__file__))
            template_path = os.path.join(base_path, "templates")
            config_json = ConfigJson()
            tool = config_json.get_configuration("tool")
            checklist = config_json.get_configuration("checklist")
            import pdb

            pdb.set_trace()
            schema_xmls = run_construct(
                template_path, schema_targets, self.center, checklist, tool
            )

            submission_xml = construct_submission(
                template_path, self.action, schema_xmls, self.center, checklist, tool
            )

    def upload(self):
        """Create the required files and upload to ENA"""
        self.convert_input_json_to_ena()
        self.create_structure_to_ena()
