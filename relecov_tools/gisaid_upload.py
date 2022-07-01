import logging

# from pyparsing import col
import rich.console
import json

import pandas as pd
import sys
import os

# import ftplib
import relecov_tools.utils

# from relecov_tools.config_json import ConfigJson


# import site


log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class GisaidUpload:
    def __init__(
        self,
        user=None,
        passwd=None,
        source_json=None,
        customized_project=None,
        action=None,
        output_path=None,
        metadata=None,
    ):
        if user is None:
            self.user = relecov_tools.utils.prompt_text(
                msg="Enter your username defined in GISAID"
            )
        else:
            self.user = user
        #Add proxy settings: username:password@proxy:port (optional)
        if passwd is None:
            self.passwd = relecov_tools.utils.prompt_password(
                msg="Enter your password to GISAID"
            )
        else:
            self.passwd = passwd
        if source_json is None:
            self.source_json_file = relecov_tools.utils.prompt_path(
                msg="Select the GISAID json file to upload"
            )
        else:
            self.source_json_file = source_json
        if customized_project is None:
            self.customized_project = None
        else:
            self.customized_project = customized_project
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the log files"
            )
        else:
            self.output_path = output_path
        if metadata is None:
            self.metadata = relecov_tools.utils.prompt_path(
                msg="Select metadata json file"
            )
        else:
            self.metadata = metadata
        if not os.path.isfile(self.source_json_file):
            log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"[red]json data file {self.source_json_file} does not exist")
            sys.exit(1)
        with open(self.source_json_file, "r") as fh:
            self.json_data = json.loads(fh.read())

    def convert_input_json_to_ena(self):
        """Split the input ena json, in samples and runs json"""
        pass

    # Metadatos

    def metadata_to_csv(self):
        data = relecov_tools.utils.read_json_file(self.metadata)
        df_data = pd.DataFrame(data)
        df_data.to_csv("meta_gisaid.csv")

    # Sequences
    # Unificar en multifasta
    # Cambiar headers/id


    # Upload
    # Subir con cli3
    # Token
    # Opción de configurar proxy

    # def upload(self):
    # """Create the required files and upload to ENA"""
    # self.convert_input_json_to_ena()
    # self.create_structure_to_ena()
