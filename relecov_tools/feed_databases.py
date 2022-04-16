#!/usr/bin/env python
import sys
import os

import logging
import rich.console
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.rest_api import RestApi
import relecov_tools.json_schema

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class FeedDatabases:
    def __init__(
        self,
        user=None,
        passwd=None,
        json_file=None,
        iskylims_url=None,
        relecov_url=None,
    ):
        self.config_json = ConfigJson()
        if json_file is None:
            self.config_json = ConfigJson()
            json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file which have the data to map"
            )
        if not os.path.isfile(json_file):
            log.error("json data file %s does not exist ", json_file)
            stderr.print(f"[red] json data file {json_file} does not exist")
            sys.exit(1)
        self.json_data = relecov_tools.utils.read_json_file(json_file)
        self.json_file = json_file
        # Get iSkyLIMS settings
        self.iskylims_settings = self.config_json.get_topic_data("external_url", "iskylims")
        self.iskylims_server = self.iskylims_settings['server']
        self.iskylims_url = self.iskylims_settings['url']
        if iskylims_url:
            split = iskylims_url.split("/")
            self.iskylims_server = split[0] + "/"
            self.iskylims_url = "/".join(split[1:]) + "/"

        self.relecov_settings = self.config_json.get_topic_data("external_url", "relecov")
        self.relecov_server = self.relecov_settings['server']
        self.relecov_url = self.relecov_settings['url']
        if relecov_url:
            split = relecov_url.split("/")
            self.relecov_server = split[0] + "/"
            self.relecov_url = "/".join(split[1:])

        self.iskylims_rest_api = RestApi(self.iskylims_server, self.iskylims_url)
        self.relecov_rest_api = RestApi(self.relecov_server, self.relecov_url)

    def fetch_sample_project_fields(self):
        import pdb; pdb.set_trace()
        s_project = self.iskylims_rest_api.get_request(self.iskylims_settings["url_project_fields"], "project", self.iskylims_settings["project_name"])
        if not s_project:
            return False

    def get_fields_sample(self):
        """Get from configuration file the mandatory fields for Sample"""
        sample_fields = {}
        sample_fields["iskylims_s_fields"] = self.config_json.get_configuration("iskylims_sample_fields")
        sample_fields["relecov_fields"] = self.config_json.get_configuration("relecov_fields")
        req_data = self.fetch_sample_project_fields()
        if not req_data:
            stderr.print("[red] Unable to fetch fields for sample project ")
            log.error("Unable to fetch fields for sample project")
            sys.exit(1)
        sample_fields["iskylims_s_proj_fields"] = req_data
        return sample_fields

    def split_json(self):
        """Split the information in the input json to populate iSkyLIMS and
        Relecov platform
        """
        sample_fields = self.get_fields_sample()


    def store_data(self):
        """Collect data from json file and split them to store data in iSkyLIMS
        amd in Relecov Platform
        """
        split_data = self.split_json()
