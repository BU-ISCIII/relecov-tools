#!/usr/bin/env python
import sys
import os

import logging
import rich.console
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
import relecov_tools.json_schema

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class RelecovMetadata:
    def __init__(
        self,
        user=None,
        passwd=None,
        json_file=None,
        iskylims_url=None,
        relecov_url=None,
    ):
        config_json = ConfigJson()
        if json_file is None:
            json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file which have the data to map"
            )
        if not os.path.isfile(json_file):
            log.error("json data file %s does not exist ", json_file)
            stderr.print(f"[red] json data file {json_file} does not exist")
            sys.exit(1)
        self.json_data = relecov_tools.utils.read_json_file(json_file)
        self.json_file = json_file

    def split_json():
        """Split the information in the input json to populate iSkyLIMS and
        Relecov platform
        """
        pass

    def store_data(self):
        """Collect data from json file and split them to store data in iSkyLIMS
        amd in Relecov Platform
        """
        split_data = self.split_json()
