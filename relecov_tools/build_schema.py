#!/usr/bin/env python
import logging
import rich.console
import pandas as pd
import json
import os
import sys

import relecov_tools.utils

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
        print_diff=False,
        out_dir=None
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

        # Validate the Excel file path
        if not self.excel_file_path or not os.path.isfile(self.excel_file_path):
            raise ValueError("A valid Excel file path must be provided.")
        if not self.excel_file_path.endswith('.xlsx'):
            raise ValueError("The Excel file must have a .xlsx extension.")

    def read_database_definition(self):
        """Reads the database definition and converts it into json format."""
        # Read excel file
        df = pd.read_excel(self.excel_file_path)

        # Convert database to json format
        json_data = {}
        for _, row in df.iterrows():
            property_name = row[0]
            values=row.drop(df.columns[0]).to_dict()
            json_data[property_name] = values
        
        # Check json is not empty
        if len(json_data) == 0:
            stderr.print("[red]No data found in  xlsx database")
            sys.exit(1)


        return(json_data)
    def create_schema(self):
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
        self.read_database_definition()
