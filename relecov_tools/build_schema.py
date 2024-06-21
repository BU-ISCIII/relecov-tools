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
                stderr.print(f"[red]The directory {out_dir} does not exist. Please, try again. Bye")
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


    def build_new_schema(self, schema_draft, json_data):
        """
        Create schema_input.json when no schema is already present.
        """
        # TODO: Read the Excel file and extract the database definitions.

        # TODO: Check if schema_input.json already exists.

        # TODO: If schema_input.json does not exist, create it and populate with the extracted definitions.

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
        self.read_database_definition()

        # Verify current schema used by relecov-tools:
        current_schema = self.get_current_schema()
        # TODO: if schema not found do something
        #if not current_schema:
        
        # Create schema draft template (leave empty to be prompted to list of available schema versions)
        schema_draft_template = self.create_schema_draft_template("2020-2")

        # build new schema draft based on database definition. 
        # TODO: Compare current vs new schema
