import logging
import sys
import rich.console

import relecov_tools.utils
import pkg_resources
from jsonschema import Draft202012Validator, exceptions

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

SCHEMA_VALIDATORS = {
    "2020-12": Draft202012Validator,
}


def check_valid_version(draft_version):
    """Validate the provided draft version against available JSON Schema versions."""
    available_schemas = [version for version in SCHEMA_VALIDATORS.keys()]
    try:
        if not draft_version:
            draft_version = relecov_tools.utils.prompt_selection(
                "Choose a Json Schema valid version:", available_schemas
            )
        elif draft_version not in available_schemas:
            draft_version = relecov_tools.utils.prompt_selection(
                f"Draft version '{draft_version}' not found. Choose a valid Json Schema version:",
                available_schemas,
            )
    except Exception as e:
        stderr.print(f"[red]An error occurred while selecting the draft version: {e}")
        sys.exit(1)

    if not draft_version:
        stderr.print("[red]No valid draft version selected. Exiting.")
        sys.exit(1)

    stderr.print(f"[green]Using draft version: {draft_version}")
    return draft_version


def create_draft(draft_version, required_items=None):
    """Creates a JSON Schema Draft template with required fields."""
    # Check if required draft version is available

    url_str = Draft202012Validator.META_SCHEMA["$id"]
    id_str = "https://github.com/BU-ISCIII/relecov-tools/blob/develop/relecov_tools/schema/relecov_schema.json"
    description_str = "Json schema that specifies the structure, content, and validation rules for RELECOV metadata"
    pakage_version_str = pkg_resources.get_distribution("relecov_tools").version

    # Construct the draft template
    draft_template = {
        "$schema": url_str,
        "$id": id_str,
        "title": "RELECOV schema",
        "description": description_str,
        "version": pakage_version_str,
        "type": "object",
        "properties": {},
    }

    # Include required fields if specified
    if required_items:
        draft_template["required"] = []

    return draft_template


def check_schema_draft(schema_draft, draft_version):
    """Validates the schema_draft against the JSON Schema Draft 2020-12 meta-schema."""
    if draft_version not in SCHEMA_VALIDATORS:
        stderr.print(f"[red]Unsupported draft version: {draft_version}")
        sys.exit(1)

    validator_class = SCHEMA_VALIDATORS[draft_version]

    try:
        validator_class.check_schema(schema_draft)
        stderr.print("[green]New schema is valid based on JSON Specification rules.")
    except exceptions.SchemaError as e:
        stderr.print(f"[red]Schema validation error: {e.message}")
    except Exception as e:
        stderr.print(f"An error occurred during schema validation: {str(e)}")
