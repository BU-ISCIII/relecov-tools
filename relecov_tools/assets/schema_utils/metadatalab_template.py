#!/usr/bin/env python
import logging
import rich.console
import json
import xlsxwriter

import relecov_tools.utils


log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


def overview_tab(json_schema, excel_file, header, start_col="A", start_row=1, tab_name="Overview"):
    """"""
    errors_record = {}
    # Create workbook object
    workbook = xlsxwriter.Workbook(excel_file)
    sheet = workbook.add_worksheet(tab_name)
    bold = workbook.add_format({"bold": True})

    # Write the headers to the worksheet and setup dimentions
    for col_num, col_name in enumerate(header):
        sheet.write(0, col_num, col_name, bold)
    col_index = ord(start_col)
    row_index = start_row
    
    # Get schema property ID and its features
    # TODO: try except 
    required_list = json_schema.get('required')
    properties_dict = json_schema.get('properties')

    for property_id, features in properties_dict.items():
        label_name = features.get("label", "")
        description = features.get("description", "")
        group = features.get("classification", "")
        mandatory = "Y" if property_id in required_list else "N"
        example = features.get("examples", [""])[0]
        metadata_column = chr(col_index)
        # Fill the overview tab with processed data
        try:
            sheet.write(row_index, 0, group)
            sheet.write(row_index, 1, label_name)
            sheet.write(row_index, 2, description)
            sheet.write(row_index, 3, group)
            sheet.write(row_index, 4, mandatory)
            sheet.write(row_index, 5, example)
            sheet.write(row_index, 6, metadata_column)
        except TypeError as e:
            stderr.print(
                f"[red] Error when filling excell in property '{property_id}': {e}"
            )
            pass
        col_index += 1
        row_index += 1

    if len(errors_record)>0:
        stderr.print(f"[red]Errors encountered while generating the {tab_name} sheet. See errors {errors_record}")
        return False
    else:
        # Close the workbook
        workbook.close()
        return True

def metadatalab_tab():
    """"""
    return True
def datavalidation_tab():
    """"""
    return True
