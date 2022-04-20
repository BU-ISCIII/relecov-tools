from bdb import set_trace
import os
import logging
import rich.console
import json

import pandas as pd
import sys
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from ena_upload.ena_upload import extract_targets
from ena_upload.ena_upload import check_filenames
from ena_upload.ena_upload import check_file_checksum
from ena_upload.ena_upload import get_md5
from ena_upload.ena_upload import get_taxon_id
from ena_upload.ena_upload import get_scientific_name

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
        filename=None,
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
        # check if data is given when adding a 'run' table

        if filename is None:
            self.filename = relecov_tools.utils.prompt_path(
                msg="Oops, requires data for submitting RUN object"
            )
        else:
            self.filename = filename
        """
        else:
            # validate if given data is file
            for path in filename:
                if not os.path.isfile(path):
                    msg = f"Oops, the file {path} does not exist"
                    log.error(msg)
        """

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
            ["experiment_alias", "file_name", "sequence_file_R2_fastq"]
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
        """
        if not schema_targets:
            stderr.print(
                f"[red] There is no table submitted having at least one row with {self.action} as action in the status column."
            )
            sys.exit(1)
        """

        if self.action == "ADD" or self.action == "add":
            # when adding run object
            # update schema_targets wit md5 hash
            # submit data
            if "run" in schema_targets:
                # a dictionary of filename:file_path
                df = schema_targets["run"]
                file_paths = {}
                if self.filename:
                    for path in self.source_json_file:
                        file_paths[os.path.basename(path)] = os.path.abspath(path)
                # check if file names identical between command line and table
                # if not, system exits
                check_filenames(file_paths, df_run)
                # generate MD5 sum if not supplied in table
                if file_paths and not check_file_checksum(df):
                    print("No valid checksums found, generate now...", end=" ")
                    file_md5 = {
                        filename: get_md5(path) for filename, path in file_paths.items()
                    }
                    df_experiments.insert(1, "status", self.action)
                    import pdb

                    pdb.set_trace()
                    # update schema_targets wih md5 hash
                    md5 = df["file_name"].apply(lambda x: file_md5[x]).values
                    # SettingWithCopyWarning causes false positive
                    # e.g at df.loc[:, 'file_checksum'] = md5
                    pd.options.mode.chained_assignment = None
                    df.loc[:, "file_checksum"] = md5
                    print("done.")

    def upload(self):
        """Create the required files and upload to ENA"""
        self.convert_input_json_to_ena()
        self.create_structure_to_ena()
