#!/usr/bin/env python
import os
import sys
import logging
import rich.console
import openpyxl
from itertools import islice
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class MetadataHomogeneizer:
    """MetadataHomogeneizer object"""

    def __init__(self, lab_metadata=None, institution=None, output_folder=None):
        self.config_json = ConfigJson()
        self.heading = self.config_json.get_configuration("metadata_lab_heading")
        if lab_metadata is None:
            self.lab_metadata = relecov_tools.utils.prompt_path(
                msg="Select the file which contains metadata"
            )
        else:
            self.lab_metadata = lab_metadata
        if not os.path.exists(self.lab_metadata):
            log.error("Metadata file %s does not exist ", self.lab_metadata)
            stderr.print("[red] Metadata file " + self.lab_metadata + " does not exist")
            sys.exit(1)
        if institution is None:
            self.institution = relecov_tools.utils.prompt_selection(
                msg="Select the available mapping institution",
                choices=["isciii", "hugtip", "hunsc-iter"],
            )
        else:
            self.institution = institution.upper()
        mapping_json_file = os.path.join(
            os.path.dirname(__file__),
            "schema",
            "institution_schemas",
            self.config_json.get_topic_data("mapping_file", self.institution),
        )
        self.mapping_json_data = relecov_tools.utils.read_json_file(mapping_json_file)
        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = output_folder

    def read_metadata_file(self):
        """Read the input metadata file"""
        wb_file = openpyxl.load_workbook(self.lab_metadata, data_only=True)
        ws_metadata_lab = wb_file["Sheet"]
        heading = [i.value.strip() for i in ws_metadata_lab[1] if i.value]
        ws_data = []
        for row in islice(ws_metadata_lab.values, 1, ws_metadata_lab.max_row):
            l_row = list(row)
            data_row = {}
            # Ignore the empty rows
            # guessing that row 1 and 2 with no data are empty rows
            if l_row[0] is None and l_row[1] is None:
                continue
            for idx in range(0, len(heading)):
                data_row[heading[idx]] = l_row[idx]
            ws_data.append(data_row)
        # import pdb; pdb.set_trace()
        return ws_data

    def mapping_metadata(self, ws_data):
        map_fields = self.mapping_json_data["mapped_fields"]
        map_data = []
        for row in ws_data:
            row_data = {}
            for dest_map, orig_map in map_fields.items():

                row_data[dest_map] = row[orig_map]
            map_data.append(row_data)

        return map_data

    def additional_fields(self, mapped_data):
        add_data = [self.heading]
        fixed_fields = self.mapping_json_data["fixed_fields"]
        for row in mapped_data:
            new_row_data = []
            for field in self.heading:
                if field in row:
                    data = row[field]
                elif field in fixed_fields:
                    data = fixed_fields[field]
                else:
                    data = ""
                new_row_data.append(data)
            add_data.append(new_row_data)
        return add_data

    def write_to_excel_file(self, data, f_name):
        book = openpyxl.Workbook()
        sheet = book.active
        for row in data:
            sheet.append(row)
        sheet.title = "METADATA_LAB"
        book.save(f_name)
        return

    def converting_metadata(self):
        ws_data = self.read_metadata_file()
        mapped_data = self.mapping_metadata(ws_data)
        converted_data = self.additional_fields(mapped_data)
        f_name = os.path.join(self.output_folder, "converted_metadata_lab.xlsx")
        self.write_to_excel_file(converted_data, f_name)
        return
