import os
import logging
import rich.console
import json

# import pandas as pd
import sys

# import ena_upload
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class EnaUpload:
    def __init__(self, source_json=None, output_path=None, action=None):
        if source_json is None:
            self.source_json_file = relecov_tools.utils.prompt_source_path()
        else:
            self.source_json_file = source_json
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_destination_path()
        else:
            self.output_path = output_path
        if action is None:
            self.action = "ADD"
        else:
            self.action = action
        if not os.path.isfile(self.source_json_file):
            log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"json data file {self.source_json_file} does not exist")
            sys.exit(1)
        with open(self.source_json_file, "r") as fh:
            self.json_data = json.loads(fh.read())

    def upload_files_to_ena(self):
        """Create the required files and upload to ENA"""
        # df = {}

        # df_study = pd.DataFrame.from_dict(data["study"])
        # df_samples = pd.DataFrame.from_dict(data["samples"])
        # df_runs = pd.DataFrame.from_dict(data["runs"])
        # df_experiments = pd.DataFrame.from_dict(data["experiments"])
