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
    """Create a new sub excel file having only the samples that were invalid.
    Samples name are checking the Sequencing sample id which are in
    column B (index 1).
    The rows that match the value collected from json file on tag
    collecting_lab_sample_id are removed from excel
    """
    sample_list = []
    json_data = relecov_tools.utils.read_json_file(invalid_json)
    for row in json_data:
        sample_list.append(row["collecting_lab_sample_id"])
    wb = openpyxl.load_workbook(metadata_file)
    exclude_sheet = ["Overview", "METADATA_LAB", "DATA VALIDATION"]
    for sheet in wb.sheetnames:
        if sheet in exclude_sheet:
            continue
        ws_sheet = wb[sheet]
        row_to_del = []
        # Findout where the sample index is
        idx_sample = 2 if ws_sheet.title == "1.Database Identifiers" else 1

        for row in ws_sheet.iter_rows(min_row=4, max_row=ws_sheet.max_row):
            if not row[2].value and not row[1].value:
                if len(row_to_del) > 0:
                    row_to_del.sort(reverse=True)
                    for idx in row_to_del:
                        ws_sheet.delete_rows(idx)
                break
            if row[0].value == "CAMPO":
                continue
            if str(row[idx_sample].value) not in sample_list:
                row_to_del.append(row[0].row)

    new_name = "invalid_" + os.path.basename(metadata_file)
    m_file = os.path.join(out_folder, new_name)
    wb.save(m_file)
    return
