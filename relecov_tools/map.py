#!/usr/bin/env python
from collections import OrderedDict
from datetime import datetime
import json
import jsonschema
from jsonschema import Draft202012Validator
from relecov_tools.config_json import ConfigJson
import rich.console
import os
import sys

# import jsonschema
import relecov_tools.utils
from relecov_tools.base_module import BaseModule

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class Map(BaseModule):
    def __init__(
        self,
        origin_schema=None,
        json_file=None,
        destination_schema=None,
        schema_file=None,
        output_dir=None,
    ):
        super().__init__(output_dir=output_dir, called_module=__name__)
        config_json = ConfigJson()
        self.config_json = config_json
        if origin_schema is None:
            origin_schema = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "schema",
                config_json.get_topic_data("json_schemas", "relecov_schema"),
            )
        else:
            if not os.path.isfile(origin_schema):
                self.log.error("Relecov schema file %s does not exist", origin_schema)
                stderr.print(
                    "[red] Relecov schema " + origin_schema + " does not exist"
                )
                exit(1)
        rel_schema_json = relecov_tools.utils.read_json_file(origin_schema)
        try:
            Draft202012Validator.check_schema(rel_schema_json)
        except jsonschema.ValidationError:
            self.log.error("Relecov schema does not fulfill Draft 202012 Validation ")
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
            self.log.error("json data file %s does not exist ", json_file)
            stderr.print(f"[red] json data file {json_file} does not exist")
            sys.exit(1)
        self.json_data = relecov_tools.utils.read_json_file(json_file)
        self.json_file = json_file
        batch_id = self.get_batch_id_from_data(self.json_data)
        self.set_batch_id(batch_id)

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
                self.log.error(
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
        self.output_dir = output_dir

        if os.path.exists(os.path.join(output_dir, "mapping_errors.log")):
            os.remove(os.path.join(output_dir, "mapping_errors.log"))

    def maping_schemas_based_on_geontology(self):
        """Return a dictionary with the properties of the mapped_to_schema as key and
        properties of Relecov Schema as value
        """
        mapped_dict = OrderedDict()
        errors = {}
        required_fields = self.mapped_to_schema["required"]

        for key, values in self.mapped_to_schema["properties"].items():
            if values["ontology"] == "0":
                continue
            try:
                mapped_dict[key] = self.ontology[values["ontology"]]
            except KeyError as e:
                if key in required_fields:
                    stderr.print(
                        f"[red]Required field {key} ontology missing in relecov schema"
                    )
                    sys.exit(1)
                else:
                    errors[key] = str(e)
        if len(errors) >= 1:
            output_errs = "\n".join(f"{field}:{info}" for field, info in errors.items())
            invalid_ontologies = str([field for field in errors.keys()]).strip("[]")
            self.log.error("Invalid ontology for: " + invalid_ontologies)
            stderr.print("[yellow]\nGot unmapped ontologies. Check mapping_errors.log")
            with open("mapping_errors.log", "w") as errs:
                errs.write("Ontology mapping errors:\n" + output_errs + "\n")
        return mapped_dict

    def mapping_json_data(self, mapping_schema_dict):
        """Convert phage plus data to the requested schema"""
        mapped_data = []

        for data in self.json_data:
            map_sample_dict = OrderedDict()
            for item, value in mapping_schema_dict.items():
                try:
                    if isinstance(data[value], str):
                        data[value] = data[value].split(" [", 1)[0]
                    map_sample_dict[item] = data[value]
                except KeyError as e:
                    self.log.info("Property %s not set in the source data", e)
            mapped_data.append(map_sample_dict)
        return mapped_data

    def additional_formating(self, mapped_json_data):
        """Update data that needs additional formating such as
        word splitting and include fields with fixed values.
        """
        additional_data = self.config_json.get_topic_data(
            "ENA_fields", "additional_formating"
        )
        fixed_fields = self.config_json.get_topic_data("ENA_fields", "ena_fixed_fields")

        if self.destination_schema == "ENA":
            for idx in range(len(self.json_data)):
                for key, value in fixed_fields.items():
                    mapped_json_data[idx][key] = value
                for key, _ in additional_data.items():
                    """
                    Some fields in ENA need special formatting such as sample_id+date.
                    Instead of directly merging them, -- is used as delimiter.
                    Also, the Not Provided fields are skipped in this process
                    """
                    formated_data = {
                        x: "--".join(
                            [
                                self.json_data[idx].get(f, "").split(" [", 1)[0]
                                for f in y
                                if "Not Provided"
                                not in self.json_data[idx].get(f, "").split(" [", 1)[0]
                            ]
                        )
                        for x, y in additional_data.items()
                    }
                    if "fastq_filepath" in key:
                        formated_data[key] = formated_data[key].replace("--", "/")
                    mapped_json_data[idx][key] = formated_data[key]
        elif self.destination_schema == "GISAID":
            for idx in range(len(self.json_data)):
                mapped_json_data[idx]["covv_type"] = "betacoronavirus"
        """
        This is a temporal solution for library_strategy. Once the values are also
        mapped by the ontology (not only the fields) this should not be necessary
        """
        for sample in mapped_json_data:
            if not sample.get("library_strategy"):
                continue
            sample["library_strategy"] = sample["library_strategy"].strip(" strategy")

        return mapped_json_data

    def check_required_fields(self, mapped_json_data, dest_schema):
        """Checks which required fields are Not Provided"""
        if dest_schema == "ENA":
            # The block below can probably go into an auxiliar function
            required_fields = self.mapped_to_schema["required"]
            for sample in mapped_json_data:
                missing_required = [x for x in required_fields if x not in sample]
                for field in missing_required:
                    sample[field] = "Not Provided"
            try:
                not_provided_fields = {
                    sample["isolate"]: [
                        field
                        for field in required_fields
                        if "Not Provided" in sample[field]
                    ]
                    for sample in mapped_json_data
                }
            except KeyError as e:
                print(f"Field {e} could not be found in json data. Aborting")
                print()
                sys.exit(1)
            notprov_report = "\n".join(
                f"Sample {key}: {str(val).strip('[]')}"
                for key, val in not_provided_fields.items()
            )
            self.log.error(
                f"Some required fields for {dest_schema} were Not Provided: "
                "Check mapping_errors.log for more details"
            )
            stderr.print(
                f"[yellow]\nSome required fields for {dest_schema} were Not Provided:",
                "[yellow]\nCheck mapping_errors.log for more details",
            )
            self.log.error("Required fields Not Provided:\n" + notprov_report)
            with open("mapping_errors.log", "a") as errs:
                errs.write("Required fields Not Provided:\n" + notprov_report)
        else:
            # Only ENA's schema is supported as yet
            return
        return mapped_json_data

    def write_json_fo_file(self, mapped_json_data):
        """Write metadata to json file"""
        os.makedirs(self.output_dir, exist_ok=True)
        time = datetime.now().strftime("%Y_%m_%d")
        file_name = (
            "mapped_metadata" + "_" + self.destination_schema + "_" + time + ".json"
        )
        json_file = os.path.join(self.output_dir, file_name)
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
        self.check_required_fields(mapped_json_data, self.destination_schema)
        self.write_json_fo_file(updated_json_data)
        stderr.print(f"[green]Finished mapping to {self.destination_schema} schema")
        return
