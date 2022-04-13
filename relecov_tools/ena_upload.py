import os
import logging
import rich.console
import json

import pandas as pd
import sys
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from ena_upload.ena_upload import extract_targets

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
        # schema_dataframe = {}

        config_json = ConfigJson()

        map_to_upload = config_json.get_topic_data("json_schemas", "ena_schema")

        map_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", map_to_upload
        )
        fh = open(map_file)
        map_structure_json = json.load(fh)
        fh.close()

        esquema = self.source_json_file
        fh_esquema = open(esquema)
        esquema_json = json.load(fh_esquema)
        fh_esquema.close()
        # lista = ["study", "runs", "samples", "experiments"]

        # llaves = esquema_json.keys()

        df = pd.DataFrame.from_dict(esquema_json, orient="index")
        df_transposed = df.T
        df_study = df_transposed[["study_alias", "study_title", "study_type"]]
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
        df_runs = df_transposed[
            ["experiment_alias", "sequence_file_R1_fastq", "sequence_file_R2_fastq"]
        ]
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

        extract_targets(self.action, df_study)

        import pdb

        pdb.set_trace()

    def upload(self):
        """Create the required files and upload to ENA"""
        self.convert_input_json_to_ena()
        self.create_structure_to_ena()

        # df_study = pd.DataFrame.from_dict(data["study"])
        # df_samples = pd.DataFrame.from_dict(data["samples"])
        # df_runs = pd.DataFrame.from_dict(data["runs"])
        # df_experiments = pd.DataFrame.from_dict(data["experiments"])
