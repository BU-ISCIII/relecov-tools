#!/usr/bin/env python
import sys
import os
import re
import json
import rich.console
import time

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.rest_api import RestApi
from relecov_tools.base_module import BaseModule

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class UploadDatabase(BaseModule):
    def __init__(
        self,
        user: str | None = None,
        password: str | None = None,
        json: list[dict] | str | None = None,
        type: str | None = None,
        platform: str | None = None,
        server_url: str | None = None,
        full_update: bool = False,
        long_table: str | None = None,
    ):

        super().__init__(called_module="update-db")
        # create the instance for logging the summary information
        self.logsum = self.parent_log_summary()

        # Check CLI arguments
        if isinstance(json, list):
            self.json_data = json
        else:
            if json is None:
                json = relecov_tools.utils.prompt_path(
                    msg="Select the json file which has the data to map"
                )
            if not (isinstance(json, str) and os.path.isfile(json)):
                stderr.print(f"[red] JSON data file {json} does not exist")
                raise FileNotFoundError(f"JSON data file {json} does not exist")
            self.json_data = relecov_tools.utils.read_json_file(json)

        # Convert all values in json_data to strings
        # This is to ensure that all values are strings before uploading as they
        # are expected to be strings in the database
        for row in self.json_data:
            for key, value in row.items():
                if not isinstance(value, str):
                    row[key] = str(value)

        # Get the user and password for the database
        if user is None:
            user = relecov_tools.utils.prompt_text(
                msg="Enter username for upload data to server"
            )
        self.user = user

        if password is None:
            password = relecov_tools.utils.prompt_text(msg="Enter credential password")
        self.passwd = password

        # Validate long_table file if provided
        self.long_table_file = None
        if full_update or type == "variantdata":
            if not long_table or not os.path.isfile(long_table):
                raise ValueError(
                    f"Provided long_table file does not exist: {long_table}"
                )
            self.long_table_file = os.path.realpath(long_table)

        # Configure full update or specific type and platform
        self.full_update = bool(full_update)
        if self.full_update:
            self.server_url = None
        else:
            self.type_of_info = type or relecov_tools.utils.prompt_selection(
                "Select:",
                ["sample", "bioinfodata", "variantdata"],
            )

            self.platform = platform or relecov_tools.utils.prompt_selection(
                "Select:",
                ["iskylims", "relecov"],
            )

            self.server_url = server_url

        # Check and load configuration
        self.config_json = ConfigJson()

        batch_id = self.get_batch_id_from_data(self.json_data)
        self.set_batch_id(batch_id)

        schema_filename = self.config_json.get_topic_data("generic", "relecov_schema")
        if not isinstance(schema_filename, str) or not schema_filename:
            stderr.print("[red] Schema filename is not defined in the configuration.")
            raise ValueError("Schema filename is not defined in the configuration.")
        schema = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "schema",
            schema_filename,
        )
        self.schema = relecov_tools.utils.read_json_file(schema)

        try:
            self.platform_settings = self.config_json.get_topic_data(
                "update_db", "platform-params"
            )
        except KeyError as e:
            logtxt = f"Unable to fetch parameters for {platform} {e}"
            stderr.print(f"[red]{logtxt}")
            self.log.error(logtxt)
            raise KeyError(f"Unable to fetch parameters for {platform} {e}")

    def get_schema_ontology_values(self):
        """Read the schema and extract the values of ontology with the label"""
        ontology_dict = {}
        for prop, values in self.schema["properties"].items():
            if "ontology" in values and values["ontology"] != "":
                ontology_dict[values["ontology"]] = prop
        return ontology_dict

    def map_iskylims_sample_fields_values(self, sample_fields, s_project_fields):
        """Map the values to the properties send to databasee
        in json schema based on label
        """
        sample_list = []
        s_fields = list(sample_fields.keys())
        for row in self.json_data:
            s_dict = {}
            for sfield in s_fields:
                if sfield not in row.keys():
                    s_dict[sample_fields[sfield]] = "Not Provided"
                else:
                    value = row[sfield]
                    if isinstance(value, str):
                        found_ontology = re.search(r"(.+) \[\w+:.*", value)
                        if found_ontology:
                            # remove the ontology data from item value
                            value = found_ontology.group(1)
                    s_dict[sample_fields[sfield]] = value
            for pfield in s_project_fields:
                if pfield in row.keys():
                    value = row[pfield]
                    if isinstance(value, str):
                        found_ontology = re.search(r"(.+) \[\w+:.*", value)
                        if found_ontology:
                            # remove the ontology data from item value
                            value = found_ontology.group(1)
                        s_dict[pfield] = value
            # include the fixed value
            fixed_value = self.config_json.get_topic_data(
                "update_db", "iskylims_fixed_values"
            )
            for prop, val in fixed_value.items():
                s_dict[prop] = val
            all_iskylims_fields = s_project_fields + s_fields
            sid = row.get("sequencing_sample_id", row.get("sequence_file_R1"))
            for missing in list(set(list(row.keys())) - set(all_iskylims_fields)):
                self.log.debug(f"Field {missing} not found in Iskylims in sample {sid}")
            sample_list.append(s_dict)
        return sample_list

    def get_iskylims_fields_sample(self):
        """2 requests are sent to iSkyLIMS. One for getting the sample fields
        These fields are mapped using the ontology.
        The second request is for getting the sample project fields. These are
        mapped using the label value.
        """

        def map_lab_request_fields(ontology_dict, s_project_fields):
            """Map the lab request fields from iskylims using ontology values"""
            # fetch fields for lab_request model
            lab_request_response = self.platform_rest_api.get_request(
                self.platform_settings["iskylims"]["url_lab_request_mapping"], "", ""
            )
            if "ERROR" in lab_request_response:
                logtxt1 = f"Unable to fetch lab_request fields from {self.platform}."
                logtxt2 = f"Received error {lab_request_response.get('ERROR', lab_request_response.get("message", "Unknown error"))}"
                self.logsum.add_error(entry=str(logtxt1 + logtxt2))
            else:
                self.log.info("Fetched lab_request fields from iSkyLIMS")
                stderr.print("[blue]Fetched lab_request fields from iSkyLIMS")
                if not "data" in lab_request_response:
                    logtxt = "No data entry found in lab_request fields response. No mapping will be made."
                    self.logsum.add_warning(entry=logtxt)
                    stderr.print(f"[yellow]{logtxt}")
                    return s_project_fields
                else:
                    self.log.debug(
                        "LabRequest mapping: %s", str(lab_request_response["data"])
                    )
                for ontology in lab_request_response["data"]:
                    if ontology in ontology_dict:
                        if ontology_dict[ontology] not in s_project_fields:
                            s_project_fields.append(ontology_dict[ontology])
                        else:
                            self.log.warning(
                                f"Ontology {ontology} already in sample project fields"
                            )
            return s_project_fields

        sample_fields = {}
        s_project_fields = []
        # get the ontology values for mapping values in sample fields
        ontology_dict = self.get_schema_ontology_values()
        if (
            not self.platform_settings
            or "iskylims" not in self.platform_settings
            or self.platform_settings["iskylims"] is None
        ):
            logtxt = "Platform settings for 'iskylims' are not properly configured in configuration.json."
            self.logsum.add_error(entry=logtxt)
            stderr.print(f"[red]{logtxt}")
            raise ValueError(logtxt)
        sample_url = self.platform_settings["iskylims"]["url_sample_fields"]
        sample_fields_raw = self.platform_rest_api.get_request(sample_url, "", "")

        if "ERROR" in sample_fields_raw:
            logtxt1 = f"Unable to fetch data from {self.platform}."
            logtxt2 = f" Received error {sample_fields_raw['ERROR']}"
            self.logsum.add_error(entry=str(logtxt1 + logtxt2))
            stderr.print(f"[red]{logtxt1 + logtxt2}")
            sys.exit(1)

        for _, values in sample_fields_raw["data"].items():
            if "ontology" in values:
                try:
                    property = ontology_dict[values["ontology"]]
                    # sample_fields has a key, the label in metadata, and as value
                    # the field name for the sample
                    sample_fields[property] = values["field_name"]
                except KeyError as e:
                    self.logsum.add_warning(entry=f"Error mapping ontology {e}")
                    # stderr.print(f"[red]Error mapping ontology {e}")
            else:
                # for the ones that do not have ontology label in the sample field
                # and have an empty value: sample_fields[key] = ""
                logtxt = f"No ontology found for {values.get('field_name')}"
                self.logsum.add_warning(entry=(logtxt))
        # fetch label for sample Project
        s_project_url = self.platform_settings["iskylims"]["url_project_fields"]
        param = self.platform_settings["iskylims"]["param_sample_project"]
        p_name = self.platform_settings["iskylims"]["project_name"]
        s_project_fields_raw = self.platform_rest_api.get_request(
            s_project_url, param, p_name
        )
        if "ERROR" in s_project_fields_raw:
            logtxt1 = f"Unable to fetch data from {self.platform}."
            logtxt2 = f" Received error {s_project_fields_raw['ERROR']}"
            self.logsum.add_error(entry=str(logtxt1 + logtxt2))
            return
        else:
            self.log.info("Fetched sample project fields from iSkyLIMS")
            stderr.print("[blue] Fetched sample project fields from iSkyLIMS")
        for field in s_project_fields_raw["data"]:
            s_project_fields.append(field["sample_project_field_name"])
        s_project_fields = map_lab_request_fields(ontology_dict, s_project_fields)
        return [sample_fields, s_project_fields]

    def map_relecov_sample_data(self):
        """Select the values from self.json_data"""
        field_values = []
        r_fields = self.config_json.get_topic_data(
            "update_db", "relecov_sample_metadata"
        )

        for row in self.json_data:
            s_dict = {}
            for r_field in r_fields:
                if r_field in row:
                    s_dict[r_field] = row[r_field]
                else:
                    s_dict[r_field] = None
            field_values.append(s_dict)
        return field_values

    def clean_ambiguous_value(self, value):
        """Replace ambiguous values by default if value is not required."""
        if isinstance(value, str):
            if value.strip() in {"", "NA", "None"}:
                return "Not Provided"
        elif value is None:
            return "Not Provided"
        return value

    def update_database(self, field_values, post_url):
        """Send the request to update database"""

        if not self.platform_settings or self.platform not in self.platform_settings:
            raise ValueError("Platform is not set. Cannot update database.")

        post_url = self.platform_settings[self.platform][post_url]
        success_count = 0
        request_count = 0
        req_sample = None  # Ensure req_sample is always defined
        result_all = []
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

            self.logsum.feed_key(sample=req_sample)

            result = self.platform_rest_api.post_request(
                json.dumps(chunk),
                {"user": self.user, "pass": self.passwd},
                post_url,
            )

            if "ERROR" in result:
                if result["ERROR"] == "Server not available":
                    # retry to connect to server
                    for _ in range(10):
                        # wait 5 sec before resending the request
                        time.sleep(5)
                        result = self.platform_rest_api.post_request(
                            json.dumps(chunk),
                            {"user": self.user, "pass": self.passwd},
                            self.platform_settings[post_url],
                        )
                        if "ERROR" not in result:
                            break
                    else:
                        if "ERROR" in result:
                            logtxt = f"Unable to sent the request to {post_url}"
                            self.logsum.add_error(entry=logtxt, sample=req_sample)
                            stderr.print(f"[red]{logtxt}")
                            continue

                elif "is not defined" in result["ERROR"].lower():
                    error_txt = result["ERROR"]
                    logtxt = f"Sample {req_sample} failed in {post_url}: {error_txt}"
                    self.logsum.add_error(entry=logtxt, sample=req_sample)
                    stderr.print(f"[yellow]Warning: {logtxt}")
                    continue
                elif "already defined" in result["ERROR"].lower():
                    logtxt = f"Request to {post_url} already defined"
                    self.logsum.add_warning(entry=logtxt, sample=req_sample)
                    stderr.print(f"[yellow]{logtxt} for sample {req_sample}")
                    # If the sample is already defined, we can continue
                    # but we return the data of the already defined sample
                    result_all.append(result["data"])
                    continue
                else:
                    logtxt = f"Error {result['ERROR']} in request to {post_url}"
                    self.logsum.add_error(entry=logtxt, sample=req_sample)
                    stderr.print(f"[red]{logtxt}")
                    continue
            self.log.info(
                "stored data in %s for sample %s with unique id %s",
                self.platform,
                req_sample,
                result["data"].get(
                    "sample_unique_id", result["data"].get("sample_name")
                ),
            )
            stderr.print(f"[green] Successful request for {req_sample}")
            result_all.append(result["data"])
            success_count += 1

        if request_count == success_count:
            stderr.print(
                f"All {self.type_of_info} data sent sucessfully to {self.platform}"
            )
            stderr.print(f"[green]Upload process to {self.platform} completed")
            self.log.info(
                "%s of the %s requests were sent to %s",
                success_count,
                request_count,
                self.platform,
            )
        else:
            logtxt = "%s of the %s requests were sent to %s"
            self.logsum.add_warning(
                entry=logtxt % (success_count, request_count, self.platform),
                sample=req_sample,
            )
            stderr.print(
                f"[yellow]{logtxt % (success_count, request_count, self.platform)}"
            )

        return result_all

    def store_data(self, type_of_info, server_name):
        """Collect data from json file and split them to store data in iSkyLIMS
        and in Relecov Platform
        """
        map_fields = {}

        post_url = "store_samples"
        if type_of_info == "sample":
            if server_name == "iskylims":
                self.log.info("Getting sample fields from %s", server_name)
                stderr.print(f"[blue] Getting sample fields from {server_name}")
                sample_fields, s_project_fields = self.get_iskylims_fields_sample()
                self.log.info("Selecting sample fields")
                stderr.print("[blue] Selecting sample fields")
                map_fields = self.map_iskylims_sample_fields_values(
                    sample_fields, s_project_fields
                )
            else:
                stderr.print("[blue] Selecting sample fields")
                map_fields = self.map_relecov_sample_data()
            post_url = "store_samples"

        elif type_of_info == "bioinfodata":
            post_url = "bioinfodata"
            map_fields = [
                {k: self.clean_ambiguous_value(v) for k, v in row.items()}
                for row in self.json_data
            ]

        elif type_of_info == "variantdata":
            post_url = "variantdata"
            map_fields = self.json_data

        result = self.update_database(map_fields, post_url)
        return result

    def start_api(self, platform):
        """Open connection torwards database server API"""
        # Get database settings
        if not self.platform_settings or platform not in self.platform_settings:
            logtxt = f"Platform settings for '{platform}' are not properly configured in configuration.json."
            stderr.print(f"[red]{logtxt}")
            self.logsum.add_error(entry=logtxt)
            raise ValueError(logtxt)
        try:
            p_settings = self.platform_settings[platform]
        except KeyError as e:
            logtxt = f"Unable to fetch parameters for {platform} {e}"
            stderr.print(f"[red]{logtxt}")
            self.logsum.add_error(entry=logtxt)
            raise KeyError(f"Unable to fetch parameters for {platform} {e}")
        if self.server_url is None:
            server_url = p_settings["server_url"]
        else:
            server_url = self.server_url
        self.platform = platform
        self.api_url = p_settings["api_url"]
        self.platform_rest_api = RestApi(server_url, self.api_url)
        return

    def update_db(self):
        """Run the update database process with the provided input"""
        if self.full_update is True:
            self.server_name = "iskylims"
            self.type_of_info = "sample"
            self.start_api(self.server_name)
            self.store_data(self.type_of_info, self.server_name)

            self.server_name = "relecov"
            self.start_api(self.server_name)
            for datatype in ["sample", "bioinfodata", "variantdata"]:
                log_text = f"Sending {datatype} data to {self.server_name}"
                self.log.info(log_text)
                stderr.print(log_text)
                self.type_of_info = datatype
                # TODO: Handling for servers with different datatype needs
                if datatype == "variantdata":
                    self.log.info(
                        "Selected %s file for variant data", str(self.long_table_file)
                    )
                    self.json_data = relecov_tools.utils.read_json_file(
                        self.long_table_file
                    )
                self.store_data(datatype, self.server_name)
        else:
            self.start_api(self.platform)
            if self.type_of_info == "variantdata":
                self.log.info(
                    "Selected %s file for variant data", str(self.long_table_file)
                )
                self.json_data = relecov_tools.utils.read_json_file(
                    self.long_table_file
                )
            self.store_data(self.type_of_info, self.platform)

        self.parent_create_error_summary(called_module="update-db")
        return
