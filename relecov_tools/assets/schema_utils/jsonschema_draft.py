import logging
import sys

import rich.console

import relecov_tools.utils
import pkg_resources

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

def create_draft(draft_version, required=None):
    """Creates a JSON Schema Draft template with required fields."""
    available_schemas = ["2020-12"]

    if draft_version not in available_schemas:
        stderr.print(f"[red] {draft_version} not found. Available schemas are: {', '.join(available_schemas)}")
        sys.exit(1)
    
    url_str = f"https://json-schema.org/draft/{draft_version}/schema"
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
        "properties": {
        }
    }

    # Include required fields if specified
    if required:
        draft_template["required"] = required

    return draft_template


# TODO: draft202012Validator should be implemented here
