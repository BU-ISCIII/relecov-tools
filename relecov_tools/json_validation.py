#!/usr/bin/env python
import logging
import rich.console
from jsonschema import Draft202012Validator
import sys
import os
import openpyxl

import relecov_tools.utils
import relecov_tools.assets.schema_utils.jsonschema_draft
from relecov_tools.config_json import ConfigJson
from relecov_tools.log_summary import LogSum

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
        self.json_data_file = json_data_file
        out_path = os.path.dirname(os.path.realpath(self.json_data_file))
        self.lab_code = out_path.split("/")[-2]
        self.logsum = LogSum(
            output_location=self.out_folder, unique_key=self.lab_code, path=out_path
        )

        stderr.print("[blue] Reading the json file")
        self.json_data = relecov_tools.utils.read_json_file(json_data_file)
        if isinstance(self.json_data, dict):
            stderr.print(f"[red]Invalid json file content in {json_data_file}.")
            stderr.print("Should be a list of dicts. Create it with read-lab-metadata")
            sys.exit(1)
        self.metadata = metadata
        try:
            self.sample_id_field = self.get_sample_id_field()
        except ValueError as e:
            self.sample_id_field = None
            self.SAMPLE_FIELD_ERROR = str(e)

    def validate_schema(self):
        """Validate json schema against draft"""
        relecov_tools.assets.schema_utils.jsonschema_draft.check_schema_draft(
            self.json_schema, "2020-12"
        )

    def get_sample_id_field(self):
        """Find the name of the field used to track the samples in the given schema"""
        # TODO: Include this field in configuration.json
        sample_id_ontology = "GENEPIO:0000079"
        ontology_match = [
            x
            for x, y in self.json_schema["properties"].items()
            if y.get("ontology") == sample_id_ontology
        ]
        if ontology_match:
            sample_id_field = ontology_match[0]
        else:
            error_text = f"No valid sample ID field ({sample_id_ontology}) in schema"
            raise ValueError(error_text)
        return sample_id_field

    def validate_instances(self):
        """Validate data instances against a validated json schema"""

        # create validator
        validator = Draft202012Validator(self.json_schema)
        schema_props = self.json_schema["properties"]

        validated_json_data = []
        invalid_json = []
        errors = {}
        error_keys = {}
        if self.sample_id_field is None:
            log_text = f"Logs keys set to None. Reason: {self.SAMPLE_FIELD_ERROR}"
            self.logsum.add_warning(sample=self.sample_id_field, entry=log_text)
        stderr.print("[blue] Start processing the json file")
        for item_row in self.json_data:
            # validate(instance=item_row, schema=json_schema)
            sample_id_value = item_row.get(self.sample_id_field)
            if validator.is_valid(item_row):
                validated_json_data.append(item_row)
                self.logsum.feed_key(sample=sample_id_value)
            else:
                # Count error types
                for error in validator.iter_errors(item_row):
                    if error.validator == "required":
                        error_field = [
                            f for f in error.validator_value if f in error.message
                        ][0]
                    else:
                        error_field = error.absolute_path[0]
                    try:
                        err_field_label = schema_props[error_field]["label"]
                    except KeyError:
                        log.error("Could not extract label for %s" % error_field)
                        err_field_label = error_field
                    error.message.replace(error_field, err_field_label)
                    error_text = f"Error in column {err_field_label}: {error.message}"
                    error_keys[error.message] = error_field
                    if error.message in errors:
                        errors[error.message] += 1
                    else:
                        errors[error.message] = 1
                    self.logsum.add_error(sample=sample_id_value, entry=error_text)
                # append row with errors
                invalid_json.append(item_row)

        # Summarize errors
        stderr.print("[blue] --------------------")
        stderr.print("[blue] VALIDATION SUMMARY")
        stderr.print("[blue] --------------------")
        for error_type in errors.keys():
            num_of_errors = str(errors[error_type])
            field_with_error = str(error_keys[error_type])
            error_text = "{} samples failed validation for {}:\n{}"
            error_text = error_text.format(num_of_errors, field_with_error, error_type)
            self.logsum.add_warning(entry=error_text)
            stderr.print(f"[red]{error_text}")
            stderr.print("[red] --------------------")

        return validated_json_data, invalid_json

    def create_invalid_metadata(self, invalid_json, metadata, out_folder):
        """Create a new sub excel file having only the samples that were invalid.
        Samples name are checking the Sequencing sample id which are in
        column B (index 1).
        The rows that match the value collected from json file on tag
        collecting_lab_sample_id are removed from excel
        """
        if self.sample_id_field is None:
            log_text = f"Invalid excel file won't be created: {self.SAMPLE_FIELD_ERROR}"
            self.logsum.add_error(entry=log_text)
            return
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
            sample_list.append(str(row[self.sample_id_field]))
        wb = openpyxl.load_workbook(metadata)
        # TODO: Include this as a key in configuration.json
        ws_sheet = wb["METADATA_LAB"]
        tag = "Sample ID given for sequencing"
        seq_id_col = [idx for idx, cell in enumerate(ws_sheet[1]) if tag in cell.value]
        if seq_id_col:
            id_col = seq_id_col[0]
        row_to_del = []
        row_iterator = ws_sheet.iter_rows(min_row=1, max_row=ws_sheet.max_row)
        consec_empty_rows = 0
        for row in row_iterator:
            # if no data in 10 consecutive rows, break loop
            if not any(row[x].value for x in range(10)):
                row_to_del.append(row[0].row)
                consec_empty_rows += 1
            if consec_empty_rows > 10:
                break
            consec_empty_rows = 0
            if str(row[id_col].value) not in sample_list:
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

    def create_validated_json(self, valid_json_data, out_folder):
        """Create a copy of the input json file, keeping only the validated samples

        Args:
            valid_json_data (list(dict)): List of samples metadata as dictionaries
            out_folder (str): path to folder where file will be created
        """
        file_name = "_".join(["validated", os.path.basename(self.json_data_file)])
        file_path = os.path.join(out_folder, file_name)
        log.info("Saving Json file with the validated samples as %s", file_name)
        relecov_tools.utils.write_json_fo_file(valid_json_data, file_path)
        return

    def validate(self):
        """Validate samples from metadata, create an excel with invalid samples,
        and a json file with the validated ones
        """
        self.validate_schema()
        valid_json_data, invalid_json = self.validate_instances()
        if not invalid_json:
            stderr.print("[green]Sucessful validation, no invalid file created!!")
        else:
            log_text = "Summary: %s valid and %s invalid samples"
            self.logsum.add_warning(
                entry=log_text % (len(valid_json_data), len(invalid_json))
            )
            self.create_invalid_metadata(invalid_json, self.metadata, self.out_folder)
        if valid_json_data:
            self.create_validated_json(valid_json_data, self.out_folder)
        else:
            log_text = "All the samples were invalid. No valid file created"
            self.logsum.add_error(entry=log_text)
            stderr.print(f"[red]{log_text}")
        self.logsum.create_error_summary(called_module="validate")
        return valid_json_data, invalid_json
