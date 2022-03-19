import os
import logging
import rich.console
import json
import pandas as pd
import sys
from ena_upload import ena_upload
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

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
        project=None,
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
        if project is None:
            self.project = None
        else:
            self.project = project
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
        """Convert json to dataframe required by ena-upload-cli package"""
        schema_dataframe = {}
        config_json = ConfigJson()
        map_to_upload = config_json.get_configuration("mapping_upload_ena")
        map_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", map_to_upload
        )
        fh = open(map_file)
        map_json = json.load(fh)
        fh.close()

        for xml_file in map_json["xml_files"]:
            if self.project is not None and xml_file in self.project:
                pass
            elif config_json.get_configuration(xml_file):
                df = pd.DataFrame.from_dict(
                    config_json.get_configuration(xml_file), orient="index"
                )
            else:
                pass
            df = ena_upload.check_columns(df, xml_file, self.action, self.dev, False)
            schema_dataframe[xml_file] = df
        return schema_dataframe

    def upload_files_to_ena(self):
        """Create the required files and upload to ENA"""
        # df = {}
        self.convert_input_json_to_ena()
        # df_study = pd.DataFrame.from_dict(data["study"])
        # df_samples = pd.DataFrame.from_dict(data["samples"])
        # df_runs = pd.DataFrame.from_dict(data["runs"])
        # df_experiments = pd.DataFrame.from_dict(data["experiments"])
