#!/usr/bin/env python
import sys
import os
import jsonschema
import json
import logging
import rich.console
from jsonschema import Draft202012Validator
import time

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.rest_api import RestApi

# import relecov_tools.json_schema

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
        schema=None,
        iskylims_url=None,
        relecov_url=None,
    ):
        if user is None:
            user = relecov_tools.utils.prompt_text(
                msg="Enter username for iskylims and relecov_plaform"
            )
        self.user = user
        if passwd is None:
            passwd = relecov_tools.utils.prompt_text(
                msg="Enter credential password for iskylims and relecov_plaform"
            )
        self.passwd = passwd
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
        if schema is None:
            schema = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                self.config_json.get_topic_data("json_schemas", "relecov_schema"),
            )
        else:
            if os.path.isfile(schema):
                log.error("Relecov schema file %s does not exists", schema)
                stderr.print("[red] Relecov schema " + schema + "does not exists")
                sys.exit(1)
        rel_schema_json = relecov_tools.utils.read_json_file(schema)
        try:
            Draft202012Validator.check_schema(rel_schema_json)
        except jsonschema.ValidationError:
            log.error("Schema does not fulfil Draft 202012 Validation ")
            stderr.print("[red] Schema does not fulfil Draft 202012 Validation")
            sys.exit(1)
        self.schema = rel_schema_json

        # Get iSkyLIMS settings
        self.iskylims_settings = self.config_json.get_topic_data(
            "external_url", "iskylims"
        )
        self.iskylims_server = self.iskylims_settings["server"]
        self.iskylims_url = self.iskylims_settings["url"]
        if iskylims_url:
            split = iskylims_url.split("/")
            self.iskylims_server = split[0] + "/"
            self.iskylims_url = "/".join(split[1:]) + "/"

        self.relecov_settings = self.config_json.get_topic_data(
            "external_url", "relecov"
        )
        self.relecov_server = self.relecov_settings["server"]
        self.relecov_url = self.relecov_settings["url"]
        if relecov_url:
            split = relecov_url.split("/")
            self.relecov_server = split[0] + "/"
            self.relecov_url = "/".join(split[1:])

        self.iskylims_rest_api = RestApi(self.iskylims_server, self.iskylims_url)
        self.relecov_rest_api = RestApi(self.relecov_server, self.relecov_url)

    def mapping_sample_project_fields(self, s_project_fields):
        """Map the label defined in the sample project fields with the name
        in json schema based on label
        """
        label_prop_dict = {}
        map_list = {}  # key is the label,  value the property
        for prop, values in self.schema["properties"].items():
            try:
                label_prop_dict[values["label"]] = prop
            except KeyError:
                continue
        for field in s_project_fields:
            try:
                map_list[field] = label_prop_dict[field]
            except KeyError as e:
                log.error("Unable to map in the schema the label %s", e)
                stderr.print("[red] Unable to map in the schema the label ", e)
                sys.exit(1)
        return map_list

    def get_fields_sample(self):
        """Get from configuration file the mandatory fields for Sample"""
        sample_fields = {}
        sample_fields["iskylims_s_fields"] = self.config_json.get_configuration(
            "iskylims_sample_fields"
        )
        sample_fields["relecov_fields"] = self.config_json.get_configuration(
            "relecov_fields"
        )

        s_project = self.iskylims_rest_api.get_request(
            self.iskylims_settings["url_project_fields"],
            "project",
            self.iskylims_settings["project_name"],
        )
        if "ERROR" in s_project:
            log.error(
                "Unable to get parameters. Received error code %s", s_project["ERROR"]
            )
            stderr.print(
                "[red] Unable to fetch data. Received error ", s_project["ERROR"]
            )
            sys.exit(1)
        s_project_fields = [x["sampleProjectFieldName"] for x in s_project["DATA"]]

        sample_fields["iskylims_s_fields"].update(
            self.mapping_sample_project_fields(s_project_fields)
        )

        return sample_fields

    def update_databases(self, sample_fields):
        """Split the information in the input json to populate iSkyLIMS and
        Relecov platform
        """
        for sample in self.json_data:
            iskylims_data = {}
            for label, value in sample_fields["iskylims_s_fields"].items():
                try:
                    iskylims_data[label] = sample[value]
                except KeyError as e:
                    log.error("Found %s when mapping data for sending to iSkyLIMS")
                    stderr.print(f"[red]  {e} not found in mapping")
                    sys.exit(1)
            # add fxxed valuurs before sending request
            iskylims_data["patientCore"] = ""
            iskylims_data["sampleLocation"] = ""
            iskylims_data["onlyRecorded"] = "Yes"
            iskylims_data["sampleProject"] = self.iskylims_settings["project_name"]
            result = self.iskylims_rest_api.post_request(
                json.dumps(iskylims_data),
                {"user": self.user, "pass": self.passwd},
                self.iskylims_settings["store_samples"],
            )
            if "ERROR" in result:
                if result["ERROR"] == "Server not available":
                    # retry to connect to server
                    for i in range(10):
                        # wait 5 sec before resending the request
                        time.sleep(5)
                        result = self.iskylims_rest_api.post_request(
                            json.dumps(iskylims_data),
                            {"user": self.user, "pass": self.passwd},
                            self.iskylims_settings["store_samples"],
                        )
                        if "ERROR" not in result:
                            break
                    if i == 9 and "ERROR" in result:
                        log.error("Unable to sent the request to iSkyLIMS")
                        stderr.print("[red] Unable to sent the request to iSkyLIMS")
                        sys.exit(1)
                else:
                    log.error("Request to iSkyLIMS was not accepted")
                    stderr.print(
                        f"[red] Error {result['ERROR']} when sending request to iSkyLIMS "
                    )
                    sys.exit(1)

            log.info(
                "stored data in iskylims for sample %s", iskylims_data["sampleName"]
            )
            # send request to releco-platform
            relecov_data = {}
            for label, value in sample.items():
                if label not in sample_fields["iskylims_s_fields"]:
                    relecov_data[label] = value

            result = self.relecov_rest_api.post_request(
                json.dumps(iskylims_data),
                {"user": self.user, "pass": self.passwd},
                self.relecov_settings["store_samples"],
            )
        return

    def store_data(self):
        """Collect data from json file and split them to store data in iSkyLIMS
        amd in Relecov Platform
        """
        sample_fields = self.get_fields_sample()
        self.update_databases(sample_fields)
