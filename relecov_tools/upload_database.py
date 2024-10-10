#!/usr/bin/env python
import sys
import os
import re
import glob
import json
import logging
import rich.console
import time

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.rest_api import RestApi
from relecov_tools.log_summary import LogSum

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class UpdateDatabase:
    def __init__(
        self,
        user=None,
        passwd=None,
        json_file=None,
        type_of_info=None,
        platform=None,
        server_url=None,
        full_update=False,
    ):
        # Get the user and password for the database
        if user is None:
            user = relecov_tools.utils.prompt_text(
                msg="Enter username for upload data to server"
            )
        self.user = user
        if passwd is None:
            passwd = relecov_tools.utils.prompt_text(msg="Enter credential password")
        self.passwd = passwd
        # get the default coonfiguration used the instance
        self.config_json = ConfigJson()
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
        schema = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "schema",
            self.config_json.get_topic_data("json_schemas", "relecov_schema"),
        )
        self.schema = relecov_tools.utils.read_json_file(schema)
        if full_update is True:
            self.full_update = True
            self.server_url = None
        else:
            self.full_update = False
            if type_of_info is None:
                type_of_info = relecov_tools.utils.prompt_selection(
                    "Select:",
                    ["sample", "bioinfodata", "variantdata"],
                )
            self.type_of_info = type_of_info
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
            log.error(logtxt)
            sys.exit(1)
        # create the instance for logging the summary information
        json_dir = os.path.dirname(os.path.realpath(self.json_file))
        lab_code = json_dir.split("/")[-2]
        self.logsum = LogSum(
            output_location=json_dir, unique_key=lab_code, path=json_dir
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
            for key, value in row.items():
                found_ontology = re.search(r"(.+) \[\w+:.*", value)
                if found_ontology:
                    # remove the ontology data from item value
                    value = found_ontology.group(1)
                if key in s_project_fields:
                    s_dict[key] = value
                if key in s_fields:
                    s_dict[sample_fields[key]] = value
                if key not in s_project_fields and key not in s_fields:
                    # just for debugging, write the fields that will not
                    # be included in iSkyLIMS request
                    log.debug("not key %s in iSkyLIMS", key)
            # include the fixed value
            fixed_value = self.config_json.get_topic_data(
                "upload_database", "iskylims_fixed_values"
            )
            for prop, val in fixed_value.items():
                s_dict[prop] = val
            # Adding tha specimen_source field to set sample_type
            try:
                s_dict["sample_type"] = row["specimen_source"]
            except KeyError as e:
                logtxt = f"Unable to fetch specimen_source from json file {e}"
                self.logsum.add_warning(entry=logtxt)
                s_dict["sample_type"] = "Other"
            sample_list.append(s_dict)
            # if sample_entry_date is not set then, add the current date
            if "sample_entry_date" not in row:
                logtxt = "sample_entry_date is not in the sample fields"
                self.logsum.add_warning(entry=logtxt)
                stderr.print(f"[yellow]{logtxt}")
                s_dict["sample_entry_date"] = time.strftime("%Y-%m-%d")

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
            log.info("Fetched sample project fields from iSkyLIMS")
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
            log.info(
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
                log.info("Getting sample fields from %s", server_name)
                stderr.print(f"[blue] Getting sample fields from {server_name}")
                sample_fields, s_project_fields = self.get_iskylims_fields_sample()
                log.info("Selecting sample fields")
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
            map_fields = self.json_data

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
                log.info(log_text)
                stderr.print(log_text)
                self.type_of_info = datatype
                # TODO: Handling for servers with different datatype needs
                if datatype == "variantdata":
                    json_dir = os.path.dirname(os.path.realpath(self.json_file))
                    long_tables = glob.glob(os.path.join(json_dir, "*long_table*.json"))
                    if not long_tables:
                        json_file = relecov_tools.utils.prompt_path(
                            msg="Select long_table json file for variant data"
                        )
                    else:
                        json_file = long_tables[0]
                    log.info("Selected %s file for variant data", str(json_file))
                    self.json_data = relecov_tools.utils.read_json_file(json_file)
                self.store_data(datatype, self.server_name)
        else:
            self.start_api(self.platform)
            self.store_data(self.type_of_info, self.platform)
        self.logsum.create_error_summary(called_module="update-db")
        return
