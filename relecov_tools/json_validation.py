#!/usr/bin/env python
import logging
import rich.console
import jsonschema
from jsonschema import Draft202012Validator
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


class SchemaValidation:
    def __init__(
        self, json_data_file=None, json_schema_file=None, metadata=None, out_folder=None
    ):
        """Validate json file against the schema"""

        if json_schema_file is None:
            config_json = ConfigJson()
            schema_name = config_json.get_topic_data("json_schemas", "relecov_schema")
            json_schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schema", schema_name
            )

        self.json_schema = relecov_tools.utils.read_json_file(json_schema_file)

        if json_data_file is None:
            json_data_file = relecov_tools.utils.prompt_path(
                msg="Select the json file to be validated"
            )

        if out_folder is None:
            self.out_folder = relecov_tools.utils.prompt_path(
                msg="Select the folder where excel file with invalid data will be saved"
            )
        else:
            self.out_folder = out_folder

        # Read and check json to validate file
        if not os.path.isfile(json_data_file):
            stderr.print("[red] Json file does not exists")
            sys.exit(1)

        stderr.print("[blue] Reading the json file")
        self.json_data = relecov_tools.utils.read_json_file(json_data_file)

        self.metadata = metadata

    def validate_schema(self):
        """Validate json schema against draft"""
        try:
            Draft202012Validator.check_schema(self.json_schema)
        except jsonschema.ValidationError:
            stderr.print("[red] Json schema does not fulfill Draft 202012 Validation")
            sys.exit(1)

    def validate_instances(self):
        """Validate data instances against a validated json schema"""

        # create validator
        validator = Draft202012Validator(self.json_schema)

        validated_json_data = []
        invalid_json = []
        errors = {}
        error_keys = {}
        stderr.print("[blue] Start processing the json file")
        for item_row in self.json_data:
            # validate(instance=item_row, schema=json_schema)
            if validator.is_valid(item_row):
                validated_json_data.append(item_row)
            else:
                # Count error types
                for error in validator.iter_errors(item_row):
                    try:
                        error_keys[error.message] = error.absolute_path[0]
                    except Exception:
                        error_keys[error.message] = error.message
                    if error.message in errors:
                        errors[error.message] += 1
                    else:
                        errors[error.message] = 1
                # append row with errors
                invalid_json.append(item_row)

        # Summarize errors
        stderr.print("[blue] --------------------")
        stderr.print("[blue] VALIDATION SUMMARY")
        stderr.print("[blue] --------------------")
        for error_type in errors.keys():
            num_of_errors = str(errors[error_type])
            field_with_error = error_keys[error_type]
            log.error(
                "%s samples failed validation for %s:\n%s",
                num_of_errors,
                field_with_error,
                error_type,
            )
            stderr.print(
                "[red]"
                + num_of_errors
                + " samples failed validation for "
                + f"{field_with_error}:\n"
                + error_type
            )
            stderr.print("[red] --------------------")

        return invalid_json

    def create_invalid_metadata(self, invalid_json, metadata, out_folder):
        """Create a new sub excel file having only the samples that were invalid.
        Samples name are checking the Sequencing sample id which are in
        column B (index 1).
        The rows that match the value collected from json file on tag
        collecting_lab_sample_id are removed from excel
        """
        if len(invalid_json) == 0:
            stderr.print(
                "[green] Sucessful validation, no invalid file will be written!!"
            )
        else:
            log.error("Some of the samples in json metadata were not validated")
            stderr.print("[red] Some of the Samples are not validate")
            if metadata is None:
                metadata = relecov_tools.utils.prompt_path(
                    msg="Select the metadata file to select those not-validated samples."
                )
            if not os.path.isfile(metadata):
                log.error("Metadata file %s does not exist", metadata)
                stderr.print(
                    "[red] Unable to create excel file for invalid samples. Metadata file ",
                    metadata,
                    " does not exist",
                )
                sys.exit(1)

            sample_list = []

            stderr.print("Start preparation of invalid samples")

            for row in invalid_json:
                sample_list.append(str(row["sequencing_sample_id"]))

            wb = openpyxl.load_workbook(metadata)
            ws_sheet = wb["METADATA_LAB"]
            row_to_del = []

            for row in ws_sheet.iter_rows(min_row=5, max_row=ws_sheet.max_row):
                # if not data on row 1 and 2 assume that no more data are in file
                # then start deleting rows
                if not row[2].value and not row[1].value:
                    break
                if str(row[2].value) not in sample_list:
                    row_to_del.append(row[0].row)

            stderr.print("Collected rows to create the excel file")
            if len(row_to_del) > 0:
                row_to_del.sort(reverse=True)
                for idx in row_to_del:
                    try:
                        ws_sheet.delete_rows(idx)
                    except TypeError as e:
                        log.error(
                            "Unable to delete row %s from metadata file because of",
                            idx,
                            e,
                        )
                        stderr.print(f"[red] Unable to delete row {idx} becuase of {e}")
                        sys.exit(1)

            os.makedirs(out_folder, exist_ok=True)
            new_name = "invalid_" + os.path.basename(metadata)
            m_file = os.path.join(out_folder, new_name)
            stderr.print("Saving excel file with the invalid samples")
            wb.save(m_file)
        return

    def validate(self):
        """Write invalid samples from metadata to excel"""

        self.validate_schema()
        invalid_json = self.validate_instances()
        self.create_invalid_metadata(invalid_json, self.metadata, self.out_folder)
