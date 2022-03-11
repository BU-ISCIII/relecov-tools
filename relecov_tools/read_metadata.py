#!/usr/bin/env python
from itertools import islice
from geopy.geocoders import Nominatim
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
        self, metadata_file=None, additional_metadata_file=None, output_folder=None
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
        """Fetch the metadata file folder  Directory to fetch metadata file
        file_name   metadata file name
        """
        wb_file = openpyxl.load_workbook(file_name, data_only=True)
        ws_metadata_lab = wb_file["METADATA_LAB"]
        heading = []
        for cell in ws_metadata_lab[1]:
            heading.append(cell.value)

    def validate_metadata_sample(row_sample):
        """Validate sample information"""

    def add_extra_data(self, metadata, extra_data):
        """Add the additional information that must be included in final metadata
        metadata Origin metadata
        extra_data  additional data to be included
        result_metadata    final metadata after adding the additional data
        """
        geo_loc_data = {}
        extra_metadata = []
        for row in metadata:
            for new_field, value in extra_data.items():
                row[new_field] = value
            # get the geo location latitude and longitude
            country = row["geo_loc_country"]
            city = row["geo_loc_state"]
            if city not in geo_loc_data:
                geo_loc_data[city] = self.get_geo_location_data(city, country)
            row["geo_loc_latitude"], row["geo_loc_longitude"] = geo_loc_data[city]
            # update isolate qith the name of the sample
            row["isolate"] = row["sample_name"]
            extra_metadata.append(row)
        return extra_metadata

    def request_information(external_url, request):
        """Get information from external database server using Rest API

        external_url
        request
        """
        pass

    def store_information(external_url, request, data):
        """Update information"""
        pass

    def get_geo_location_data(self, state, country):
        """Get the geo_loc_latitude and geo_loc_longitude from state"""
        geolocator = Nominatim(user_agent="geoapiRelecov")
        loc = geolocator.geocode(state + "," + country)
        return [loc.latitude, loc.longitude]

    def update_heading_to_json(self, heading, meta_map_json):
        """Change the heading values from the metadata file for the ones defined
        in the json schema
        """
        mapped_heading = []
        for cell in heading:
            if cell.value in meta_map_json:
                mapped_heading.append(meta_map_json[cell.value])
            else:
                mapped_heading.append(cell.value)
        return mapped_heading

    def read_json_file(self, j_file):
        """Read json file."""
        with open(j_file, "r") as fh:
            data = json.load(fh)
        return data

    def read_metadata_file(self, meta_map_json):
        """Read the input metadata file, mapping the metadata heading with
        the values used in json. Convert the date colunms value to the
        dd/mm/yyyy format. Return list of dict with data, and errors
        """
        wb_file = openpyxl.load_workbook(self.metadata_file, data_only=True)
        ws_metadata_lab = wb_file["METADATA_LAB"]
        # removing the None columns in excel heading row
        heding_without_none = [i for i in ws_metadata_lab[1] if i.value]
        heading = self.update_heading_to_json(heding_without_none, meta_map_json)
        metadata_values = []
        errors = {}
        for row in islice(ws_metadata_lab.values, 1, ws_metadata_lab.max_row):
            sample_data_row = {}
            for idx in range(len(heading)):
                if "date" in heading[idx]:
                    try:
                        sample_data_row[heading[idx]] = row[idx].strftime("%d/%m/%Y")
                    except AttributeError:
                        if row[0] not in errors:
                            errors[row[0]] = {}
                        errors[row[0]][heading[idx]] = "Invalid date format"
                else:
                    sample_data_row[heading[idx]] = row[idx]
            metadata_values.append(sample_data_row)
        return metadata_values, errors

    def create_metadata_json(self):
        config_json = ConfigJson()
        schema_location = config_json.get_topic_data(
            "json_schemas", "phage_plus_schema"
        )
        schema_location_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", schema_location
        )
        schema_json = self.read_json_file(schema_location_file)
        phage_plus_schema = relecov_tools.schema_json.PhagePlusSchema(schema_json)
        properties_in_schema = phage_plus_schema.get_schema_properties()
        metadata_mapping_json = config_json.get_configuration("mapping_metadata_json")
        meta_map_json_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", metadata_mapping_json
        )
        meta_map_json = self.read_json_file(meta_map_json_file)

        valid_metadata_rows, errors = self.read_metadata_file(meta_map_json)
        completed_metadata = self.add_extra_data(
            valid_metadata_rows, meta_map_json["Additional_fields"]
        )
        # fake return data, just for litin
        return completed_metadata, properties_in_schema
