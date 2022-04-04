#!/usr/bin/env python
import logging
import rich.console
import jsonschema
from jsonschema import validate
from jsonschema import Draft202012Validator
import json
import sys
import os
import openpyxl

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


def validate_json(json_data_file=None, json_schema_file=None, out_folder=None):
    """Validate json file against the schema"""
    if json_data_file is None:
        json_data_file = relecov_tools.utils.prompt_path(
            msg="Select the json file to validate"
        )
    if json_schema_file is None:
        config_json = ConfigJson()
        schema_name = config_json.get_topic_data("json_schemas", "relecov_schema")
        json_schema_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", schema_name
        )
    # schema_file = open('/home/lchapado/Projects/Proyecto_ERA/relecov-tools/schema/phage_plus_V0.json')
    with open(json_schema_file, "r") as fh:
        json_schema = json.load(fh)
    try:
        Draft202012Validator.check_schema(json_schema)
    except jsonschema.ValidationError:
        stderr.print("[red] Json schema does not fulfil Draft 202012 Validation")
        sys.exit(1)
    if not os.path.isfile(json_data_file):
        stderr.print("[red] Json file does not exists")
        sys.exit(1)
    json_data = relecov_tools.utils.read_json_file(json_data_file)
    with open(json_data_file, "r") as fh:
        json_data = json.load(fh)
    validated_json_data = []
    invalid_json = []
    errors = {}
    for item_row in json_data:
        try:
            validate(instance=item_row, schema=json_schema)
            validated_json_data.append(item_row)
        except jsonschema.ValidationError as e:
            log.error("Invalid sample data %s", e)
            invalid_json.append(item_row)
    return validated_json_data, invalid_json, errors


def create_invalid_metadata(metadata_file, invalid_json, out_folder):
    """Create a new sub excel file having only the samples that were invalid
    """
    invalid_metadata = ""
    sample_id_head = "Sequencing sample id"
    wb_inv = openpyxl.Workbook()
    sample_list = []

    json_data = relecov_tools.utils.read_json_file(invalid_json)
    for row in json_data:
        sample_list.append(row["collecting_lab_sample_id"])
    wb = openpyxl.load_workbook(metadata_file)
    import pdb; pdb.set_trace()
    for sheet in wb.sheetnames:
        ws_sheet = wb[sheet]
        ws_inv = wb_inv.create_sheet(sheet)
        head_list = [cell.value for cell in ws_sheet[1]]
        import pdb; pdb.set_trace()
        try:
            s_index = head_list.index(sample_id_head)
        except:

        for row in ws_sheet.iter_rows():
            if row[s_index].value in sample_list:
                ws_inv.append(row)
                import pdb; pdb.set_trace()
        target = wb_inv.copy_worksheet(ws_inv)
        target.title(sheet)
        import pdb; pdb.set_trace()


    wb_inv.save(filename=invalid_metadata)
