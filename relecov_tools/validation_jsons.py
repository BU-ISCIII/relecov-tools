
#!/usr/bin/env python
import logging
import rich.console
import jsonschema
from jsonschema import validate
from jsonschema import Draft202012Validator
import json
import sys
import relecov_tools.utils
import relecov_tools.schema_json

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


def validate_json_vs_schena(json_schema_file=None, json_data_file=None):

    # schema_file = open('/home/lchapado/Projects/Proyecto_ERA/relecov-tools/schema/phage_plus_V0.json')
    with open(json_schema_file, "r") as fh:
        json_schema = json.load(fh)
    try:
        Draft202012Validator.check_schema(json_schema)
    except jsonschema.ValidationError:
        stderr.print("[red] Json schema does not fulfil Draft 202012 Validation")
        sys.exit(1)
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

