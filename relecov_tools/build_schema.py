#!/usr/bin/env python
import logging
import rich.console
import pandas as pd
import os
import sys
import json
import difflib

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


# TODO: user should be able to provide a custom schema as a file in order to replace the current one.
class SchemaBuilder:
    def __init__(
        self,
        excel_file_path=None,
        base_schema_path=None,
        draft_version=None,
        show_diff=False,
        out_dir=None,
    ):
        """
        Initialize the SchemaBuilder class. This class generates a JSON Schema file based on the provided draft version.
        It reads the database definition from an Excel file and allows customization of the schema generation process.
        """
        self.excel_file_path = excel_file_path
        self.show_diff = show_diff

        # Validate input variables
        if not self.excel_file_path or not os.path.isfile(self.excel_file_path):
            raise ValueError("A valid Excel file path must be provided.")
        if not self.excel_file_path.endswith(".xlsx"):
            raise ValueError("The Excel file must have a .xlsx extension.")

        # Validate output folder
        relecov_tools.utils.prompt_create_outdir(os.getcwd(), out_dir)
        self.output_folder = os.path.join(os.getcwd(), out_dir)

        # Validate json schema draft version
        self.draft_version = (
            relecov_tools.assets.schema_utils.jsonschema_draft.check_valid_version(
                draft_version
            )
        )

        # Validate base schema
        if not base_schema_path:
            try:
                config_json = ConfigJson()
                relecov_schema = config_json.get_topic_data(
                    "json_schemas", "relecov_schema"
                )
                try:
                    self.base_schema_path = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "schema",
                        relecov_schema,
                    )
                    os.path.isfile(self.base_schema_path)
                    stderr.print(
                        "[green]RELECOV schema successfully found in the configuration."
                    )
                except FileNotFoundError as fnf_error:
                    stderr.print(f"[red]Configuration file not found: {fnf_error}")
                    sys.exit(1)
            except KeyError as key_error:
                stderr.print(f"[orange]Configuration key error: {key_error}")
                sys.exit(1)

    def validate_database_definition(self, json_data):
        """Validate the mandatory features of each property in json_data."""
        # Check mandatory key features to build a json schema
        notvalid_properties = {}
        mandatory_features = [
            "enum",
            "examples",
            "ontology_id",
            "type",
            "description",
            "classification",
            "label_name",
            "fill_mode",
            "minLength",
            "required (Y/N)",
        ]
        # Iterate over each property in json_data
        for j_key, j_value in json_data.items():
            missing_features = []
            for feature in mandatory_features:
                if feature not in j_value:
                    missing_features.append(feature)

            if missing_features:
                notvalid_properties[j_key] = missing_features

        # Summarize validation
        if notvalid_properties:
            return notvalid_properties
        else:
            return None

    def read_database_definition(self):
        """Reads the database definition and converts it into json format."""
        # Read excel file
        df = pd.read_excel(self.excel_file_path)

        # Convert database to json format
        json_data = {}
        for row in df.itertuples(index=False):
            property_name = row[0]
            values = row[1:]
            json_data[property_name] = dict(zip(df.columns[1:], values))

        # Check json is not empty
        if len(json_data) == 0:
            stderr.print("[red]No data found in  xlsx database")
            sys.exit(1)

        # Perform validation of database content
        validation_out = self.validate_database_definition(json_data)

        if validation_out:
            stderr.print(
                f"[red]Validation of database content falied. Missing mandatory features in: {validation_out}"
            )
            sys.exit(1)
        else:
            stderr.print("[green]Validation of database content passed.")
            return json_data

    def create_schema_draft_template(self):
        "Loads JsonSchema template based on draft name: Available drafts: [2020-12]"
        draft_template = (
            relecov_tools.assets.schema_utils.jsonschema_draft.create_draft(
                self.draft_version, True
            )
        )
        return draft_template

    # TODO: add strategy to deal with json schema objects, defs and refs
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
                "required (Y/N)": "required",
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
                    schema_property["fill_mode"] = None
                if "minLength" not in schema_property:
                    schema_property["minLength"] = 1

                # Finally, send schema_property object to the new json schema.
                schema_draft["properties"][property_id] = schema_property

                # Finally, send schema_required object to the new json schema.
                for key, values in schema_required.items():
                    if values == "Y":
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
        relecov_tools.assets.schema_utils.jsonschema_draft.check_schema_draft(
            schema, self.draft_version
        )

    def print_schema_diff(self, base_schema, new_schema):
        """
        Print the differences between the base version of schema_input.json
        and the updated version.
        """
        # Set diff input
        base_schema_lines = json.dumps(base_schema, indent=4).splitlines()
        new_schema_lines = json.dumps(new_schema, indent=4).splitlines()

        # Get diff lines
        diff_lines = list(
            difflib.unified_diff(
                base_schema_lines,
                new_schema_lines,
                fromfile="base_schema.json",
                tofile="new_schema.json",
            )
        )

        if not diff_lines:
            stderr.print(
                "[orange]No differencess were found between already installed and new generated schema. Exiting. No changes made"
            )
            return None
        else:
            stderr.print(
                "Differences found between the existing schema and the newly generated schema."
            )
            # Set user's choices
            choices = ["Print to sandard output (stdout)", "Save to file", "Both"]
            diff_output_choice = relecov_tools.utils.prompt_selection(
                "How would you like to print the diff between schemes?:", choices
            )
            if diff_output_choice in ["Print to sandard output (stdout)", "Both"]:
                for line in diff_lines:
                    print(line)
                return True
            if diff_output_choice in ["Save to file", "Both"]:
                diff_filepath = os.path.join(
                    os.path.realpath(self.output_folder) + "/build_schema_diff.txt"
                )
                with open(diff_filepath, "w") as diff_file:
                    diff_file.write("\n".join(diff_lines))
                stderr.print(f"[green]Schema diff file saved to {diff_filepath}")
                return True

    def save_new_schema(self, json_data):
        """
        Saves the schema generated by SchemaBuilder class into output folder.
        """
        try:
            path_to_save = self.output_folder + "/relecov_schema.json"
            with open(path_to_save, "w") as schema_file:
                json.dump(json_data, schema_file, ensure_ascii=False, indent=4)
            stderr.print(f"[green]New JSON schema saved to: {path_to_save} ")
            return True
        except PermissionError as perm_error:
            stderr.print(f"[red]Permission error: {perm_error}")
        except IOError as io_error:
            stderr.print(f"[red]I/O error: {io_error}")
        except Exception as e:
            stderr.print(f"[red]An unexpected error occurred: {str(e)}")
        return False

    def handle_build_schema(self):
        # Load xlsx database and convert into json format
        database_dic = self.read_database_definition()

        # Verify current schema used by relecov-tools:
        base_schema_json = relecov_tools.utils.read_json_file(self.base_schema_path)
        if not base_schema_json:
            stderr.print("[red]Couldn't find relecov base schema. Exiting...)")
            sys.exit(1)

        # Create schema draft template (leave empty to be prompted to list of available schema versions)
        schema_draft_template = self.create_schema_draft_template()

        # build new schema draft based on database definition.
        new_schema_json = self.build_new_schema(database_dic, schema_draft_template)

        # Verify new schema follows json schema specification rules.
        self.verify_schema(new_schema_json)

        # Compare base vs new schema and saves new JSON schema
        schema_diff = self.print_schema_diff(base_schema_json, new_schema_json)
        if schema_diff:
            self.save_new_schema(new_schema_json)
        else:
            stderr.print(
                f"[green]No changes found against base schema ({self.base_schema_path}). Exiting..."
            )
            sys.exit(1)

        # TODO: add method to add new schema via input file instead of building new (encompases validation checks).
        # TODO: Add versioning of schema when it is updated.
        # TODO: Add method to register logs any time schema is updated
        # TODO: After updating schema, generate the excell template.
