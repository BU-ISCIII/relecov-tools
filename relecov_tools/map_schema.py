#!/usr/bin/env python
from collections import OrderedDict
from datetime import datetime
import json
import jsonschema
from jsonschema import Draft202012Validator
import logging
import rich.console
import os
import sys

# import jsonschema
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class MappingSchema:
    def __init__(
        self,
        phage_plus_schema=None,
        json_data=None,
        destination_schema=None,
        schema_file=None,
        output=None,
    ):
        config_json = ConfigJson()
        if phage_plus_schema is None:
            phage_plus_schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_topic_data("json_schemas", "phage_plus_schema"),
            )
            with open(phage_plus_schema_file, "r") as fh:
                self.phage_plus_schema = json.load(fh)
        else:
            with open(phage_plus_schema, "r") as fh:
                json_schema = json.load(fh)
            try:
                Draft202012Validator.check_schema(json_schema)
            except jsonschema.ValidationError:
                stderr.print(
                    "[red] phage plus schema does not fulfil Draft 202012 Validation"
                )
                sys.exit(1)
            self.phage_plus_schema = json.load(fh)

        if json_data is None:
            self.json_data_file = relecov_tools.utils.prompt_path(
                msg="Select the json which have the data to map"
            )
        else:
            self.json_data_file = json_data
        if not os.path.isfile(self.json_data_file):
            log.error("json data file %s does not exist ", self.json_data_file)
            stderr.print(f"json data file {self.json_data_file} does not exist")
            sys.exit(1)

        if destination_schema is None:
            self.destination_schema = relecov_tools.utils.prompt_selection(
                msg="Select ENA, GISAID for already defined schemas or other for custom",
                choices=["ENA", "GISAID", "other"],
            )
        else:
            self.destination_schema = destination_schema
        if self.destination_schema == "other":
            if schema_file is None:
                self.schema_file = relecov_tools.utils.prompt_path(
                    msg="Select the json schema file to map your data"
                )
            else:
                self.schema_file = schema_file
            if not os.path.exists(self.schema_file):
                log.error(
                    "Schema file %s to map your data does not exist ",
                    self.metadata_file,
                )
                sys.exit(1)
            with open(self.schema_file, "r") as fh:
                json_schema = json.load(fh)
            try:
                Draft202012Validator.check_schema(json_schema)
            except jsonschema.ValidationError:
                stderr.print(
                    "[red] Json schema does not fulfil Draft 202012 Validation"
                )
                sys.exit(1)
        elif self.destination_schema == "ENA":
            self.schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_topic_data("json_schemas", "ena_schema"),
            )
        elif self.destination_schema == "GISAID":
            self.schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_item("json_schemas", "gisaid_schema"),
            )
        else:
            stderr.print("[red] Invalid option for mapping to schena")
            sys.exit(1)
        with open(self.schema_file, "r") as fh:
            self.mapped_to_schema = json.load(fh)

        self.ontology = {}
        for key, values in self.phage_plus_schema["properties"].items():
            self.ontology[values["ontology"]] = key

    def maping_schemas_based_on_geontology(self):
        """Return a dictionnary with the properties of the mapped_to_schema as key and
        properties of phagePlusSchema as value
        """
        mapped_dict = OrderedDict()
        for key, values in self.mapped_to_schema["properties"].items():
            try:
                mapped_dict[key] = self.ontology[values["ontology"]]
            except KeyError as e:
                # There is no exact match on ontology. Search for the parent
                # to be implemented later
                stderr.print(f"[red] Ontology value {e} not in phage plus schema")
        return mapped_dict

    def mapping_json_data(self, mapping_schema_dict):
        """Convert phage plus data to the requested schema"""
        mapped_data = []
        with open(self.json_data, "r") as fh:
            json_data = json.load(fh)
        for data in json_data:
            map_sample_dict = OrderedDict()
            for item, value in mapping_schema_dict.items:
                map_sample_dict[item] = self.data[value]
            mapped_data.append(map_sample_dict)
        return mapped_data

    def write_json_fo_file(self, mapped_json_data):
        """Write metadata to json file"""
        os.makedirs(self.output_folder, exist_ok=True)
        time = datetime.now().strftime("%Y_%m_%d_%H_%M")
        f_sub_name = os.path.basename(self.json_data).split(".")[0]
        file_name = f_sub_name + "_" + time + "_ena_mapped.json"
        json_file = os.path.join(self.output_folder, file_name)
        with open(json_file, "w") as fh:
            fh.write(json.dumps(mapped_json_data))
        return True

    def map_to_data_to_new_schema(self):
        """Mapping the json data from phage plus schema to the requested one"""
        mapping_schema_dict = self.maping_schemas_based_on_geontology()
        mapped_json_data = self.mapping_json_data(mapping_schema_dict)
        self.write_json_fo_file(mapped_json_data)
