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
    stderr.print("[blue] Reading the json file")
    json_data = relecov_tools.utils.read_json_file(json_data_file)
    with open(json_data_file, "r") as fh:
        json_data = json.load(fh)
    validated_json_data = []
    invalid_json = []
    errors = {}
    stderr.print("[blue] Start processing the json file")
    for item_row in json_data:
        try:
            validate(instance=item_row, schema=json_schema)
            validated_json_data.append(item_row)
        except jsonschema.ValidationError as e:
            log.error("Invalid sample data %s", e)
            invalid_json.append(item_row)
    # Enviar los errores por correo
    # logging.handlers.SMTPHandler(mailhost=("smtp.gmail.com", 465), fromaddr=correo_isciii, toaddrs=correo_usuario, subject="Validation errors", credentials=(usurario,contraseña), secure=None, timeout=1.0)
    if len(invalid_json) == 0:
        stderr.print("[green] Sucessful validation")
    else:
        stderr.print("[red] Some samples are not validated")
    return validated_json_data, invalid_json, errors


def create_invalid_metadata(metadata_file, invalid_json, out_folder):
    """Create a new sub excel file having only the samples that were invalid.
    Samples name are checking the Sequencing sample id which are in
    column B (index 1).
    The rows that match the value collected from json file on tag
    collecting_lab_sample_id are removed from excel
    """
    sample_list = []
    # import pdb; pdb.set_trace()
    # json_data = relecov_tools.utils.read_json_file(invalid_json)
    stderr.print("[red] Start preparation of invalid samples")
    for row in invalid_json:
        sample_list.append(str(row["collecting_lab_sample_id"]))
    wb = openpyxl.load_workbook(metadata_file)
    ws_sheet = wb["METADATA_LAB"]
    row_to_del = []

    for row in ws_sheet.iter_rows(min_row=5, max_row=ws_sheet.max_row):
        # if not data on row 1 and 2 assume that no more data are in file
        # then start deleting rows
        if not row[2].value and not row[1].value:
            if len(row_to_del) > 0:
                row_to_del.sort(reverse=True)
                for idx in row_to_del:
                    ws_sheet.delete_rows(idx)
            break

        if str(row[2].value) not in sample_list:
            row_to_del.append(row[0].row)

    os.makedirs(out_folder, exist_ok=True)
    new_name = "invalid_" + os.path.basename(metadata_file)
    m_file = os.path.join(out_folder, new_name)
    stderr.print("[red] Saving excel file with the invalid samples")
    wb.save(m_file)
    return
