#!/usr/bin/env python
import logging
import rich.console
import pandas as pd
import os
import sys

import relecov_tools.utils
import relecov_tools.assets.schema_utils.jsonschema_draft
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SchemaBuilder:
    def __init__(
        self,
        excel_file_path=None,
        base_schema_path=None,
        show_diff=False,
        out_dir=None,
    ):
        # TODO: Fix old description
        """
        Initialize the SchemaBuilder with paths to the Excel file containing the database definitions
        and the schema_input.json file.

        Args:
            excel_file_path (str): Path to the Excel file containing the database definitions.
            schema_file_path (str): Path to the schema_input.json file.
        """
        self.excel_file_path = excel_file_path
        self.schema_file_path = base_schema_path
        self.show_diff = show_diff

        # Validate input variables
        if not self.excel_file_path or not os.path.isfile(self.excel_file_path):
            raise ValueError("A valid Excel file path must be provided.")
        if not self.excel_file_path.endswith(".xlsx"):
            raise ValueError("The Excel file must have a .xlsx extension.")
        if not out_dir:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder:"
            )
        else:
            if not os.path.exists(out_dir):
                stderr.print(
                    f"[red]The directory {out_dir} does not exist. Please, try again. Bye"
                )
                sys.exit(1)
            if not os.path.isdir(out_dir):
                stderr.print("[red]The provided path is not a directory.")
                sys.exit(1)
            else:
                self.out_dir = out_dir

    def read_database_definition(self):
        """Reads the database definition and converts it into json format."""
        # Read excel file
        df = pd.read_excel(self.excel_file_path)

        # Convert database to json format
        json_data = {}
        for _, row in df.iterrows():
            property_name = row.iloc[0]
            values = row.drop(df.columns[0]).to_dict()
            json_data[property_name] = values

        # Check json is not empty
        if len(json_data) == 0:
            stderr.print("[red]No data found in  xlsx database")
            sys.exit(1)
        return json_data

    def create_schema_draft_template(self, draft_version=None):
        "Loads JsonSchema template based on draft name: Available drafts: [2020-12]"
        draft_template = (
            relecov_tools.assets.schema_utils.jsonschema_draft.create_draft(
                draft_version
            )
        )
        return draft_template

    def get_current_schema(self):
        """
        Check if the current RELECOV schema is available in the configuration file.
        """
        try:
            conf = ConfigJson()
            schemas = conf.get_configuration("json_schemas")

            if "relecov_schema" in schemas:
                current_schema = schemas["relecov_schema"]
                stderr.print("[green]RELECOV schema found in the configuration.")
                return current_schema
            else:
                stderr.print("[orange]RELECOV schema not found in the configuration.")
                return None

        except FileNotFoundError as fnf_error:
            stderr.print(f"[red]Configuration file not found: {fnf_error}")
            return None

        except KeyError as key_error:
            stderr.print(f"[orange]Configuration key error: {key_error}")
            return None

        except Exception as e:
            stderr.print(f"An unexpected error occurred: {e}")
            return None

    def build_new_schema(self, json_data, schema_draft):
        """
        Create a json schema file based on input data and draf skeleton..
        """
        try:
            # List of properties to check in the features dictionary:
            #       key[database_key]: value[schema_key]
            properties_to_check = {
                "enum": "enum",
                "examples": "examples",
                "ontology_id": "ontology",
                "type": "type",
                "description": "description",
                "classification": "classification",
                "label_name": "label",
                "fill_mode": "fill_mode",
                "minLength": "minLength",
                "required (Y/N )": "required",
            }
            schema_required_unique = []

            # Filling properties
            for property_id, features in json_data.items():
                schema_property = {}
                schema_required = {}

                for feature_key, schema_key in properties_to_check.items():
                    if feature_key in features:
                        # For enum and examples, wrap the value in a list
                        if schema_key in ["enum", "examples"]:
                            value = str(features[feature_key])
                            # if no value, json key wont be necessary, then avoid adding it
                            if len(value) > 0 and not value == "nan":
                                # Enum is an array like object
                                if schema_key == "enum":
                                    schema_property[schema_key] = value.split(", ")
                                elif schema_key == "examples":
                                    schema_property[schema_key] = [value]
                        # Recover 'required' properties from database definition.
                        elif schema_key == "required":
                            value = str(features[feature_key])
                            if value != "nan":
                                schema_required[property_id] = value

                        else:
                            schema_property[schema_key] = features[feature_key]

                # Set default values if not provided
                if "fill_mode" not in schema_property:
                    schema_property["fill_mode"] = (
                        None  # FIXME: this does not appear in database definition
                    )
                if "minLength" not in schema_property:
                    schema_property["minLength"] = (
                        1  # FIXME: this does not appear in database definition
                    )

                # Finally, send schema_property object to new json schema.
                schema_draft["properties"][property_id] = schema_property

                # Finally, send schema_required object to new json schema.
                for key, values in schema_required.items():
                    if value == "Y":
                        schema_required_unique.append(key)
            schema_draft["required"] = schema_required_unique

            # Return new schema
            return schema_draft

        except Exception as e:
            stderr.print(f"[red]Error building schema: {str(e)}")
            raise
        # Once json schema is created, it requires validation

    def verify_schema(self, schema):
        """Verify the schema_draft follows the JSON Schema specification [XXXX] meta-schema."""
        relecov_tools.assets.schema_utils.jsonschema_draft.check_schema_draft(schema)
        # TODO: specification version should be added to input params and self.

    def update_schema(self):
        """
        Update the schema_input.json based on the definitions in the Excel file.
        """
        # TODO: Read the Excel file and extract the database definitions.

        # TODO: Compare the extracted definitions with the current schema_input.json.

        # TODO: Update the schema_input.json with the new definitions.

    def print_schema_diff(self):
        """
        Print the differences between the current version of schema_input.json
        and the updated version after calling update_schema().
        """
        # TODO: Load the current schema_input.json.

        # TODO: Load the updated schema_input.json after calling update_schema().

        # TODO: Compare the two versions and print/save the differences.

    def handle_build_schema(self):
        # Load xlsx database and convert into json format
        database_dic = self.read_database_definition()

        # Verify current schema used by relecov-tools:
        self.get_current_schema()
        # TODO: if schema not found do something
        # if not current_schema:

        # Create schema draft template (leave empty to be prompted to list of available schema versions)
        schema_draft_template = self.create_schema_draft_template("2020-12")

        # build new schema draft based on database definition.
        new_schema = self.build_new_schema(database_dic, schema_draft_template)

        # Verify new schema follows json schema specification rules.
        self.verify_schema(new_schema)

        # TODO: Compare current vs new schema

        # TODO: add method to add new schema via input file instead of building new (encompases validation checks).
