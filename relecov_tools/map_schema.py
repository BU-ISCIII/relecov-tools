#!/usr/bin/env python
from collections import OrderedDict
from datetime import datetime
import json
import jsonschema
from jsonschema import Draft202012Validator
from relecov_tools.config_json import ConfigJson
import logging
import rich.console
import os
import sys

# import jsonschema
import relecov_tools.utils


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
        relecov_schema=None,
        json_file=None,
        destination_schema=None,
        schema_file=None,
        output_folder=None,
    ):
        config_json = ConfigJson()
        if relecov_schema is None:
            relecov_schema = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_topic_data("json_schemas", "relecov_schema"),
            )
        else:
            if not os.path.isfile(relecov_schema):
                log.error("Relecov schema file %s does not exist", relecov_schema)
                stderr.print(
                    "[red] Relecov schema " + relecov_schema + " does not exist"
                )
                exit(1)
        rel_schema_json = relecov_tools.utils.read_json_file(relecov_schema)
        try:
            Draft202012Validator.check_schema(rel_schema_json)
        except jsonschema.ValidationError:
            log.error("Relecov schema does not fulfill Draft 202012 Validation ")
            stderr.print(
                "[red] Relecov schema does not fulfill Draft 202012 Validation"
            )
            sys.exit(1)
        self.relecov_schema = rel_schema_json

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
                    "[red] Json schema does not fulfill Draft 202012 Validation"
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
                config_json.get_topic_data("json_schemas", "gisaid_schema"),
            )
        else:
            stderr.print("[red] Invalid option for mapping to schena")
            sys.exit(1)
        with open(self.schema_file, "r") as fh:
            self.mapped_to_schema = json.load(fh)

        self.ontology = {}
        for key, values in self.relecov_schema["properties"].items():
            if values["ontology"] == "0":
                continue
            self.ontology[values["ontology"]] = key
        self.output_folder = output_folder

    def maping_schemas_based_on_geontology(self):
        """Return a dictionary with the properties of the mapped_to_schema as key and
        properties of Relecov Schema as value
        """
        mapped_dict = OrderedDict()
        errors = {}
        for key, values in self.mapped_to_schema["properties"].items():
            if values["ontology"] == "0":
                continue
            try:
                mapped_dict[key] = self.ontology[values["ontology"]]
            except KeyError as e:
                errors[key] = str(e)
        if len(errors) >= 1:
            output_errs = "\n".join(f"{k}:{v}" for k, v in errors.items())
            log.error(
                "Invalid ontology for: %s", str([x for x in errors.keys()]).strip("[]")
            )
            stderr.print(
                f"[red]Ontology values not found in relecov schema:\n{output_errs}"
            )
        return mapped_dict

    def mapping_json_data(self, mapping_schema_dict):
        """Convert phage plus data to the requested schema"""
        mapped_data = []

        for data in self.json_data:
            map_sample_dict = OrderedDict()
            for item, value in mapping_schema_dict.items():
                try:
                    data[value] = data[value].split(" [", 1)[0]

                    map_sample_dict[item] = data[value]
                except KeyError as e:
                    log.info("Property %s not set in the source data", e)
            mapped_data.append(map_sample_dict)
        return mapped_data

    def additional_formating(self, mapped_json_data):
        """Update data like MD5 to split in two fields, one for R1 file and
        second for R2, and include fields with fixed values.
        """
        config_json = ConfigJson()
        additional_data = config_json.get_topic_data(
            "ENA_fields", "additional_formating"
        )
        fixed_fields = config_json.get_topic_data("ENA_fields", "ena_fixed_fields")
        not_provided_fields = config_json.get_configuration("ENA_fields")[
            "map_not_provided_fields"
        ]
        if self.destination_schema == "ENA":
            for idx in range(len(self.json_data)):
                for key, value in fixed_fields.items():
                    mapped_json_data[idx][key] = value
                for key, _ in additional_data.items():
                    formated_data = {
                        x: " ".join(
                            [self.json_data[idx][f].split(" [", 1)[0] for f in y]
                        )
                        for x, y in additional_data.items()
                    }
                    mapped_json_data[idx][key] = formated_data[key]
                for _, value in enumerate(not_provided_fields):
                    mapped_json_data[idx][value] = "Not Provided"
        return mapped_json_data

    def write_json_fo_file(self, mapped_json_data):
        """Write metadata to json file"""
        os.makedirs(self.output_folder, exist_ok=True)
        time = datetime.now().strftime("%Y_%m_%d")
        file_name = (
            "mapped_metadata" + "_" + self.destination_schema + "_" + time + ".json"
        )
        json_file = os.path.join(self.output_folder, file_name)
        stderr.print("Writting mapped data to json file:", json_file)
        with open(json_file, "w", encoding="utf-8") as fh:
            fh.write(
                json.dumps(
                    mapped_json_data, indent=4, sort_keys=True, ensure_ascii=False
                )
            )
        return True

    def map_to_data_to_new_schema(self):
        """Mapping the json data from relecov schema to the requested one"""
        mapping_schema_dict = self.maping_schemas_based_on_geontology()
        mapped_json_data = self.mapping_json_data(mapping_schema_dict)
        updated_json_data = self.additional_formating(mapped_json_data)
        self.write_json_fo_file(updated_json_data)
        stderr.print(f"[green]Finished mapping to {self.destination_schema} schema")
        return
