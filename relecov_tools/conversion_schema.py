#!/usr/bin/env python
from collections import OrderedDict
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
    def __init__(self, phage_plus_schema=None, json_data=None, mapped_schema=None, schema_file=None, output=None):
        config_json = ConfigJson()
        if phage_plus_schema is None:
            phage_plus_schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_topic_data("json_schemas", "phage_plus_schema"))
            with open(phage_plus_schema_file, "r") as fh:
                self.phage_plus_schema = json.load(fh)
        else:
            with open(phage_plus_schema, "r") as fh:
                json_schema = json.load(fh)
            try:
                Draft202012Validator.check_schema(json_schema)
            except jsonschema.ValidationError:
                stderr.print("[red] phage plus schema does not fulfil Draft 202012 Validation")
                sys.exit(1)
            self.phage_plus_schema = json.load(fh)

        if json_data is None:
            self.json_data_file = relecov_tools.utils.prompt_path(
                msg="Select the json which have the data to map"
            )
        else:
            self.json_data_file = json_data

        if mapped_schema is None:
            self.mapped_schema = relecov_tools.utils.prompt_selection(
                msg="Select ENA, GISAID for already defined schemas or other for custom",
                choices=["ENA", "GISAID", "other"]
            )
        else:
            self.mapped_schema = mapped_schema
        if self.mapped_schema == "other":
            if schema_file is None:
                self.schema_file = relecov_tools.utils.prompt_path(
                    msg="Select the json schema file to map your data"
                )
            else:
                self.schema_file = schema_file
            if not os.path.exists(self.schema_file):
                log.error("Schema file %s to map your data does not exist ", self.metadata_file)
                sys.exit(1)
            with open(self.schema_file, "r") as fh:
                json_schema = json.load(fh)
            try:
                Draft202012Validator.check_schema(json_schema)
            except jsonschema.ValidationError:
                stderr.print("[red] Json schema does not fulfil Draft 202012 Validation")
                sys.exit(1)
        elif self.mapped_schema == "ENA":
            self.schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_item("json_schemas", "ena_schema"))
        elif self.mapped_schema == "GISAID":
            self.schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_item("json_schemas", "gisaid_schema"))
        else:
            stderr.print("[red] Invalid option for mapping to schena")
            sys.exit(1)
        with open(self.schema_file, "r") as fh:
            self.mapped_to_schema = json.load(fh)

        self.ontology = {}
        for key , values in self.phage_plus_schema['properties'].items():
            self.ontology[values['ontology']] = key

    def maping_schemas_based_on_geontology():
        """Return a dictionnary with the properties of the mapped_to_schema as key and
            properties of phagePlusSchema as value
        """
        mapped_dict = OrderedDict()
        for key, values in mapped_to_schema['properties'].items():
            try:
                mapped_dict[key] = self.ontology[values['ontology']]
            except KeyError :
                # There is no exact match on ontology. Search for the parent
                # to be implemented later
                pass
        return mapped_dict

    def convert_json(schema):
        pass

    def get_data(self, field):
        return self.data[field]

    def map_sample_to_schema(self, mapped_structure):
        mapped_sample_list = []
        map_sample_dict = OrderedDict()
        for item, value in mapped_structure.items:
            mapped_sample_list[item] = self.data[value]
        return map_sample_dict
