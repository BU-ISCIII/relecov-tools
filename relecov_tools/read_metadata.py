#!/usr/bin/env python
from itertools import islice
import json
import logging
import rich.console
# from openpyxl import Workbook
import openpyxl
import os
import sys
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
import relecov_tools.schema_json

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class RelecovMetadata:
    def __init__(
        self,
        metadata_file=None,
        additional_metadata_file=None,
        output_folder=None
    ):
        if metadata_file is None:
            self.metadata_file = relecov_tools.utils.prompt_path(
                msg="Select the excel file which contains metadata"
            )
        else:
            self.metadata_file = metadata_file
        if not os.path.exists(self.metadata_file):
            log.error("Metadata file %s does not exist ", self.metadata_file)
            sys.exit(1)
        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = output_folder
        self.additional_metadata_file = additional_metadata_file



            # Perform workflow details


    def check_new_metadata(folder):
        """Check if there is a new metadata to be processed

        folder  Directory to be checked
        """
        pass


    def fetch_metadata_file(folder, file_name):
        """Fetch the metadata file
        folder  Directory to fetch metadata file
        file_name   metadata file name
        """
        wb_file = openpyxl.load_workbook(file_name, data_only=True)
        ws_metadata_lab = wb_file["METADATA_LAB"]
        heading = []
        for cell in ws_metadata_lab[1]:
            heading.append(cell.value)



    def validate_metadata_sample(row_sample):
        """Validate sample information"""


    def add_extra_data(metadata_file, extra_data, result_metadata):
        """Add the additional information that must be included in final metadata
        metadata Origin metadata file
        extra_data  additional data to be included
        result_metadata    final metadata after adding the additional data
        """
        pass


    def request_information(external_url, request):
        """Get information from external database server using Rest API

        external_url
        request
        """
        pass


    def store_information(external_url, request, data):
        """Update information"""
        pass

    def update_heading_to_json_schema(self, heading, meta_map_json):
        """Change the heading values from the metadata file for the ones defined
        in the json schema
        """
        map_heading = list(meta_map_json.keys())
        import pdb; pdb.set_trace()
        for i in range(len(heading)):
            if heading[i] in meta_map_json:
                heading[i] = meta_map_json[heading[i]]
        return heading

    def read_json_file(self, j_file):
        """Read json file."""
        with open(j_file, "r") as fh:
            data = json.load(fh)
        return data

    def read_metadata_file(self, meta_map_json):
        """Read the input metadata file. Return list of dict with data"""
        wb_file = openpyxl.load_workbook(self.metadata_file, data_only=True)
        ws_metadata_lab = wb_file["METADATA_LAB"]
        heading = []
        for cell in ws_metadata_lab[1]:
            heading.append(cell.value)
        for i in range(len(heading)):
            if heading[i] in list(mapping_file.keys()):
                index = list(mapping_file).index(heading[i])
                heading[index] = list(mapping_file.values())[index]
        for row in islice(ws_metadata_lab.values, 1, ws_metadata_lab.max_row):
            sample_data_row = {}
            for idx in range(len(heading)):
                if "date" in heading[idx]:
                    sample_data_row[heading[idx]] = row[idx].strftime("%d/%m/%Y")
                else:
                    sample_data_row[heading[idx]] = row[idx]

    def create_metadata_json(self):
        config_json = ConfigJson()
        schema_location = config_json.get_topic_data("json_schemas", "phage_plus_schema")
        schema_location_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "schema", schema_location)
        schema_json = self.read_json_file(schema_location_file)
        phage_plus_schema = relecov_tools.schema_json.PhagePlusSchema(schema_json)
        properties_in_schema = phage_plus_schema.get_schema_properties()
        metadata_mapping_json = config_json.get_configuration("mapping_metadata_json")
        meta_map_json_file = os.path.join(os.path.dirname(os.path.realpath(__file__)), "schema" , metadata_mapping_json)
        meta_map_json = self.read_json_file(meta_map_json_file)
        import pdb; pdb.set_trace()
        input_metadata = self.read_metadata_file(meta_map_json)
