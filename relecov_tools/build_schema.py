#!/usr/bin/env python
import logging
import rich.console

import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SchemaBuilder:
    def __init__(self, excel_file_path=None, schema_file_path=None):
        """
        Initialize the SchemaBuilder with paths to the Excel file containing the database definitions
        and the schema_input.json file.

        Args:
            excel_file_path (str): Path to the Excel file containing the database definitions.
            schema_file_path (str): Path to the schema_input.json file.
        """
        self.excel_file_path = excel_file_path
        self.schema_file_path = schema_file_path

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
