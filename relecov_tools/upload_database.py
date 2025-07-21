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
        user=None,
        password=None,
        json=None,
        type=None,
        platform=None,
        server_url=None,
        full_update=False,
        long_table=None,
    ):
        if json is None:
            json = relecov_tools.utils.prompt_path(
                msg="Select the json file which have the data to map"
            )
        json_dir = os.path.dirname(os.path.realpath(json))
        super().__init__(output_dir=json_dir, called_module="update-db")
        # Get the user and password for the database
        if user is None:
            user = relecov_tools.utils.prompt_text(
                msg="Enter username for upload data to server"
            )
        self.user = user
        if password is None:
            password = relecov_tools.utils.prompt_text(msg="Enter credential password")
        self.passwd = password
        # get the default coonfiguration used the instance
        self.config_json = ConfigJson()

        if not os.path.isfile(json):
            self.log.error("json data file %s does not exist ", json)
            stderr.print(f"[red] json data file {json} does not exist")
            sys.exit(1)
        self.json_data = relecov_tools.utils.read_json_file(json)
        batch_id = self.get_batch_id_from_data(self.json_data)
        self.set_batch_id(batch_id)
        for row in self.json_data:
            for key, value in row.items():
                if not isinstance(value, str):
                    row[key] = str(value)
        self.json_file = json
        schema = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "schema",
            self.config_json.get_topic_data("generic", "relecov_schema"),
        )
        self.schema = relecov_tools.utils.read_json_file(schema)
        if full_update is True:
            if not os.path.isfile(long_table):
                raise ValueError(
                    f"Provided long_table file does not exist {long_table}"
                )
            self.full_update = True
            self.server_url = None
            self.long_table_file = os.path.realpath(long_table)
        else:
            self.full_update = False
            if type is None:
                type = relecov_tools.utils.prompt_selection(
                    "Select:",
                    ["sample", "bioinfodata", "variantdata"],
                )
            self.type_of_info = type
            if self.type_of_info == "variantdata":
                if long_table is None:
                    json = relecov_tools.utils.prompt_path(
                        msg="Select the json file which have the data to map"
                    )
                    if not os.path.isfile(long_table):
                        raise ValueError(
                            f"Provided long_table file does not exist {long_table}"
                        )
                self.long_table_file = os.path.realpath(long_table)
            # collect data for plarform to upload data
            if platform is None:
                platform = relecov_tools.utils.prompt_selection(
                    "Select:",
                    ["iskylims", "relecov"],
                )
            self.platform = platform
            if server_url is None:
                self.server_url = server_url
        # Get configuration settings for upload database
        try:
            self.platform_settings = self.config_json.get_topic_data(
                "upload_database", "platform"
            )
        except KeyError as e:
            logtxt = f"Unable to fetch parameters for {platform} {e}"
            stderr.print(f"[red]{logtxt}")
            self.log.error(logtxt)
            sys.exit(1)
        # create the instance for logging the summary information
        lab_code = json_dir.split("/")[-2]
        self.logsum = self.parent_log_summary(
            output_dir=json_dir, lab_code=lab_code, path=json_dir
        )

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
                "upload_database", "iskylims_fixed_values"
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

        sample_fields = {}
        s_project_fields = []
        # get the ontology values for mapping values in sample fields
        ontology_dict = self.get_schema_ontology_values()
        sample_url = self.platform_settings["iskylims"]["url_sample_fields"]
        sample_fields_raw = self.platform_rest_api.get_request(sample_url, "", "")

        if "ERROR" in sample_fields_raw:
            logtxt1 = f"Unable to fetch data from {self.platform}."
            logtxt2 = f" Received error {sample_fields_raw['ERROR']}"
            self.logsum.add_error(entry=str(logtxt1 + logtxt2))
            stderr.print(f"[red]{logtxt1 + logtxt2}")
            sys.exit(1)

        for _, values in sample_fields_raw["DATA"].items():
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
        for field in s_project_fields_raw["DATA"]:
            s_project_fields.append(field["sample_project_field_name"])
        return [sample_fields, s_project_fields]

    def map_relecov_sample_data(self):
        """Select the values from self.json_data"""
        field_values = []
        r_fields = self.config_json.get_topic_data(
            "upload_database", "relecov_sample_metadata"
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
        post_url = self.platform_settings[self.platform][post_url]
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
            self.logsum.feed_key(sample=req_sample)
            result = self.platform_rest_api.post_request(
                json.dumps(chunk),
                {"user": self.user, "pass": self.passwd},
                post_url,
            )
            if "ERROR" in result:
                if result["ERROR"] == "Server not available":
                    # retry to connect to server
                    for i in range(10):
                        # wait 5 sec before resending the request
                        time.sleep(5)
                        result = self.platform_rest_api.post_request(
                            json.dumps(chunk),
                            {"user": self.user, "pass": self.passwd},
                            self.platform_settings[post_url],
                        )
                        if "ERROR" not in result:
                            break
                    if i == 9 and "ERROR" in result:
                        logtxt = f"Unable to sent the request to {post_url}"
                        self.logsum.add_error(entry=logtxt, sample=req_sample)
                        stderr.print(f"[red]{logtxt}")
                        continue

                elif "is not defined" in result["ERROR_TEST"].lower():
                    error_txt = result["ERROR_TEST"]
                    logtxt = f"Sample {req_sample} failed in {post_url}: {error_txt}"
                    self.logsum.add_error(entry=logtxt, sample=req_sample)
                    stderr.print(f"[yellow]Warning: {logtxt}")
                    continue
                elif "already defined" in result["ERROR_TEST"].lower():
                    logtxt = f"Request to {post_url} already defined"
                    self.logsum.add_warning(entry=logtxt, sample=req_sample)
                    stderr.print(f"[yellow]{logtxt} for sample {req_sample}")
                    continue
                else:
                    logtxt = f"Error {result['ERROR']} in request to {post_url}"
                    self.logsum.add_error(entry=logtxt, sample=req_sample)
                    stderr.print(f"[red]{logtxt}")
                    continue
            self.log.info(
                "stored data in %s iskylims for sample %s",
                self.platform,
                req_sample,
            )
            stderr.print(f"[green] Successful request for {req_sample}")
            suces_count += 1
        if request_count == suces_count:
            stderr.print(
                f"All {self.type_of_info} data sent sucessfuly to {self.platform}"
            )
        else:
            logtxt = "%s of the %s requests were sent to %s"
            self.logsum.add_warning(
                entry=logtxt % (suces_count, request_count, self.platform),
                sample=req_sample,
            )
            stderr.print(
                f"[yellow]{logtxt % (suces_count, request_count, self.platform)}"
            )
        return

    def store_data(self, type_of_info, server_name):
        """Collect data from json file and split them to store data in iSkyLIMS
        and in Relecov Platform
        """
        map_fields = {}

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

        self.update_database(map_fields, post_url)
        stderr.print(f"[green]Upload process to {self.platform} completed")

    def start_api(self, platform):
        """Open connection torwards database server API"""
        # Get database settings
        try:
            p_settings = self.platform_settings[platform]
        except KeyError as e:
            logtxt = f"Unable to fetch parameters for {platform} {e}"
            stderr.print(f"[red]{logtxt}")
            self.logsum.add_error(entry=logtxt)
            sys.exit(1)
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
