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


class FeedDatabase:
    def __init__(
        self,
        user=None,
        passwd=None,
        json_file=None,
        schema=None,
        type_of_info=None,
        database_server=None,
    ):
        if user is None:
            user = relecov_tools.utils.prompt_text(
                msg="Enter username for upload data to server"
            )
        self.user = user
        if passwd is None:
            passwd = relecov_tools.utils.prompt_text(msg="Enter credential password")
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
                stderr.print(f"[red] Relecov schema  {schema} does not exists")
                sys.exit(1)
        rel_schema_json = relecov_tools.utils.read_json_file(schema)
        try:
            Draft202012Validator.check_schema(rel_schema_json)
        except jsonschema.ValidationError:
            log.error("Schema does not fulfil Draft 202012 Validation ")
            stderr.print("[red] Schema does not fulfil Draft 202012 Validation")
            sys.exit(1)
        self.schema = rel_schema_json
        if type_of_info is None:
            type_of_info = relecov_tools.utils.prompt_selection(
                "Select:",
                ["sample", "bioinfodata", "variantdata"],
            )
        self.type_of_info = type_of_info

        if database_server is None:
            database_server = relecov_tools.utils.prompt_selection(
                "Select:",
                ["iskylims", "relecov"],
            )
        self.server_type = database_server
        # Get database settings
        try:
            self.database_settings = self.config_json.get_topic_data(
                "external_url", database_server
            )
        except KeyError:
            log.error("Unable to get parameters for dataserver")
            stderr.print(f"[red] Unable to fetch parameters data for {database_server}")
            sys.exit(1)
        self.database_server = self.database_settings["server"]
        self.database_url = self.database_settings["url"]

        self.database_rest_api = RestApi(self.database_server, self.database_url)

    def get_schema_ontology_values(self):
        """Read the schema and extract the values of ontology with the label"""
        ontology_dict = {}
        for prop, values in self.schema["properties"].items():
            if "ontology" in values and values["ontology"] != "":
                ontology_dict[values["ontology"]] = prop
        return ontology_dict

    def map_iskylims_sample_fields_values(self, sample_fields, s_project_fields):
        """Map the values to the properties send to dtabasee
        in json schema based on label
        """
        sample_list = []
        s_fields = list(sample_fields.keys())
        for row in self.json_data:
            s_dict = {}

            for key, value in row.items():
                if key in s_project_fields:
                    s_dict[key] = value
                elif key in s_fields:
                    s_dict[sample_fields[key]] = value
                else:
                    log.info("not key %s in iSkyLIMS", key)
            # include the fix value
            if self.server_type == "iskylims":
                fixed_value = self.config_json.get_configuration(
                    "iskylims_fixed_values"
                )
                for prop, val in fixed_value.items():
                    s_dict[prop] = val
            sample_list.append(s_dict)

        return sample_list

    def get_iskylims_fields_sample(self):
        """2 requests are sent to iSkyLIMS. One for getting the sample fields
        These fields are mapped using the ontology.
        The second request is for getting the sample project fields. These are
        mapped using the label value.
        """
        sample_fields = {}
        s_project_fields = []
        # get the ontology values for mapping values in sample fields
        ontology_dict = self.get_schema_ontology_values()
        sample_url = self.database_settings["url_sample_fields"]
        try:
            sample_fields_raw = self.database_rest_api.get_request(sample_url, "", "")
        except AttributeError:
            log.error("Unable to connect to server %s", self.database_server)
            stderr.print(f"[red] Unable to connect to server {self.database_server}")
            sys.exit(1)
        if "ERROR" in sample_fields_raw:
            log.error(
                "Unable to get parameters. Received error code %s",
                sample_fields_raw["ERROR"],
            )
            stderr.print(
                f"[red] Unable to fetch data. Received error {sample_fields_raw['ERROR']}"
            )
            sys.exit(1)

        for key, values in sample_fields_raw["DATA"].items():
            if "ontology" in values:
                try:
                    property = ontology_dict[values["ontology"]]
                    # sample_fields has a key the label in metadata and value
                    # the field name for sample
                    sample_fields[property] = values["field_name"]
                except KeyError as e:
                    stderr.print(f"[red]Error in map ontology {e}")
            else:
                # for the ones that do no have ontologuy label is the sample field
                # and the value is empty
                # sample_fields[key] = ""
                log.info("not ontology for item  %s", values["field_name"])

        # fetch label for sample Project
        s_project_url = self.database_settings["url_project_fields"]
        param = self.database_settings["param_sample_project"]
        p_name = self.database_settings["project_name"]
        s_project_fields_raw = self.database_rest_api.get_request(
            s_project_url, param, p_name
        )
        if "ERROR" in s_project_fields_raw:
            log.error(
                "Unable to get parameters. Received error code %s",
                s_project_fields_raw["ERROR"],
            )
            stderr.print(
                f"[red] Unable to fetch data. Received error {s_project_fields_raw['ERROR']}"
            )
            sys.exit(1)
        for field in s_project_fields_raw["DATA"]:
            s_project_fields.append(field["sampleProjectFieldName"])

        return [sample_fields, s_project_fields]

    def map_relecov_sample_data(self):
        """Select the values from self.json_data"""
        field_values = []
        r_fields = self.config_json.get_configuration("relecov_sample_metadata")

        for row in self.json_data:
            s_dict = {}
            for r_field in r_fields:
                if r_field in row:
                    s_dict[r_field] = row[r_field]
                else:
                    s_dict[r_field] = None
            field_values.append(s_dict)
        return field_values

    def update_database(self, field_values, post_url):
        """Send the request to update database"""
        suces_count = 0
        request_count = 0
        for chunk in field_values:
            req_sample = ""
            request_count += 1
            if "sample_name" in chunk:
                stderr.print(
                    f"[blue] sending request for sample {chunk['sample_name']}"
                )
                req_sample = chunk["sample_name"]
            elif "sequencing_sample_id" in chunk:
                stderr.print(
                    f"[blue] sending request for sample {chunk['sequencing_sample_id']}"
                )
                req_sample = chunk["sequencing_sample_id"]
            result = self.database_rest_api.post_request(
                json.dumps(chunk),
                {"user": self.user, "pass": self.passwd},
                self.database_settings[post_url],
            )

            if "ERROR" in result:
                if result["ERROR"] == "Server not available":
                    # retry to connect to server
                    for i in range(10):
                        # wait 5 sec before resending the request
                        time.sleep(5)
                        result = self.database_rest_api.post_request(
                            json.dumps(chunk),
                            {"user": self.user, "pass": self.passwd},
                            self.database_settings[post_url],
                        )
                        if "ERROR" not in result:
                            break
                    if i == 9 and "ERROR" in result:
                        log.error("Unable to sent the request to remote server")
                        stderr.print(
                            "[red] Unable to sent the request to remote server"
                        )
                        sys.exit(1)
                elif "already defined" in result["ERROR_TEST"].lower():
                    log.warning(
                        "Request to %s for %s was not accepted",
                        self.database_server,
                        req_sample,
                    )
                    stderr.print(
                        f"[yellow] Warning request for {req_sample} already defined"
                    )
                    continue
                else:
                    log.error("Request to %s was not accepted", self.database_server)
                    stderr.print(
                        f"[red] Error {result['ERROR']} when sending request to {self.database_server}"
                    )
                    sys.exit(1)
            log.info(
                "stored data in %s iskylims for sample %s",
                self.database_server,
                req_sample,
            )
            stderr.print(f"[green] Successful request for {req_sample}")
            suces_count += 1
        if request_count == suces_count:
            stderr.print(
                f"[gren] All information was sent sucessfuly to {self.server_type}"
            )
        else:
            stderr.print(
                "[yellow] Some of your requests were not successful stored in database"
            )
            stderr.print(f"[yellow] {suces_count} of the {request_count} were done ok")
        return

    def store_data(self):
        """Collect data from json file and split them to store data in iSkyLIMS
        and in Relecov Platform
        """
        map_fields = {}  #
        if self.type_of_info == "sample":
            if self.server_type == "iskylims":
                stderr.print(f"[blue] Getting sample fields from {self.server_type}")
                sample_fields, s_project_fields = self.get_iskylims_fields_sample()
                stderr.print("[blue] Selecting sample fields")
                map_fields = self.map_iskylims_sample_fields_values(
                    sample_fields, s_project_fields
                )

            else:

                # sample_fields, s_project_fields = self.get_iskylims_fields_sample()
                stderr.print("[blue] Selecting sample fields")
                map_fields = self.map_relecov_sample_data()
            post_url = "store_samples"

        elif self.type_of_info == "bioinfodata":
            post_url = "bioinfodata"
            map_fields = self.json_data

        elif self.type_of_info == "variantdata":
            post_url = "variantdata"
            map_fields = self.json_data
        else:
            stderr.print("[red] Invalid type to upload to database")
            sys.exit(1)

        self.update_database(map_fields, post_url)
        stderr.print(f"[green] Upload process to {self.server_type} completed")
