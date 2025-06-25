#!/usr/bin/env python
import rich.console
from jsonschema import Draft202012Validator, FormatChecker
import sys
import os
import openpyxl
from datetime import datetime
from collections import defaultdict

import relecov_tools.utils
import relecov_tools.assets.schema_utils.jsonschema_draft
import relecov_tools.assets.schema_utils.custom_validators
from relecov_tools.config_json import ConfigJson
from relecov_tools.base_module import BaseModule


stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SchemaValidation(BaseModule):
    def __init__(
        self,
        json_file=None,
        json_schema_file=None,
        metadata=None,
        output_dir=None,
        excel_sheet=None,
        registry=None,
    ):
        """Validate json file against the schema"""
        super().__init__(output_dir=output_dir, called_module=__name__)
        self.config = ConfigJson()
        self.log.info("Initiating validation process")
        if json_schema_file is None:
            schema_name = self.config.get_topic_data("json_schemas", "relecov_schema")
            json_schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schema", schema_name
            )

        self.json_schema = relecov_tools.utils.read_json_file(json_schema_file)

        if json_file is None:
            json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file to be validated"
            )

        if output_dir is None:
            self.out_folder = relecov_tools.utils.prompt_path(
                msg="Select the folder where excel file with invalid data will be saved"
            )
        else:
            self.out_folder = output_dir

        # Read and check json to validate file
        if not os.path.isfile(json_file):
            stderr.print("[red] Json file does not exist")
            self.log.error("Json file does not exist")
            sys.exit(1)
        self.json_data_file = json_file
        out_path = os.path.dirname(os.path.realpath(self.json_data_file))
        self.lab_code = out_path.split("/")[-2]
        self.logsum = self.parent_log_summary(
            output_dir=self.out_folder, lab_code=self.lab_code, path=out_path
        )

        stderr.print("[blue] Reading the json file")
        self.log.info("Reading the json file")
        self.json_data = relecov_tools.utils.read_json_file(json_file)
        if not isinstance(self.json_data, list):
            stderr.print(f"[red]Invalid json file content in {json_file}.")
            stderr.print("Should be a list of dicts. Create it with read-lab-metadata")
            self.log.error(f"[red]Invalid json file content in {json_file}.")
            self.log.error(
                "Should be a list of dicts. Create it with read-lab-metadata"
            )
            raise TypeError(f"Invalid json file content in {json_file}")
        try:
            batch_id = self.get_batch_id_from_data(self.json_data)
        except ValueError:
            raise ValueError(f"Provided json file {json_file} is empty")
        except AttributeError as e:
            raise ValueError(f"Invalid json file content in {json_file}: {e}")
        self.set_batch_id(batch_id)
        self.metadata = metadata

        # TODO: Include this field in configuration.json
        sample_id_ontology = "GENEPIO:0000079"
        try:
            self.sample_id_field = SchemaValidation.get_field_from_schema(
                sample_id_ontology, self.json_schema
            )
        except ValueError as e:
            self.sample_id_field = None
            self.log.error(f"Could not extract sample_id_field: {e}. Set to None")
        self.excel_sheet = excel_sheet
        self.registry_path = registry

    def validate_schema(self):
        """Validate json schema against draft and check if all properties have label"""
        relecov_tools.assets.schema_utils.jsonschema_draft.check_schema_draft(
            self.json_schema, "2020-12"
        )
        for prop in self.json_schema["properties"]:
            if "label" not in prop:
                self.log.warning(f"Property {prop} is missing 'label'")
        return

    @staticmethod
    def get_field_from_schema(ontology, schema_json):
        """Find the name of the field used to track the samples in the given schema
        using its ontology value

        Args:
            field (str): Name of the field
            schema_json (dict): Loaded json schema as a dictionary

        Returns:
            sample_id_field (str): Name of the FIRST field that matches the given ontology
        """
        ontology_match = [
            x
            for x, y in schema_json["properties"].items()
            if y.get("ontology") == ontology
        ]
        if ontology_match:
            sample_id_field = ontology_match[0]
        else:
            error_text = f"No valid sample ID field ({ontology}) in schema"
            raise ValueError(error_text)
        return sample_id_field

    @staticmethod
    def validate_instances(
        json_data, json_schema, sample_id_field=None, validator=None
    ):
        """Validate data instances against a validated JSON schema

        Args:
            json_data (list(dict)): List of samples with processed metadata
            json_schema (dict): Loaded JSON schema as a dictionary
            sample_id_field (str, optional): Metadata field used as ID to
            identify the samples associated with each error.
            validator (jsonschema.Validator(), optional): Validator with any custom
            characteristics included. Default is Draft202012Validator()

        Returns:
            validated_json_data (list(dict)): List of successfully validated samples
            errors (dict): Custom dict used to summarize validation errors.
            '''
                errors = {
                    "fields": {
                        'host_age is a required property': 'host_age'
                        'Collection_date is not a valid date': 'Collection_date'
                    },
                    "samples": {
                        'host_age is a required property': ["sample_1", "sample_2"]
                        'Collection_date is not a valid date': ["sample_1"]
                    }
                }
            '''
        """

        def get_property_label(schema_props, error_field):
            """Extract the label for the given property given a list of schema properties"""
            try:
                err_field_label = schema_props[error_field]["label"]
            except KeyError:
                return error_field
            return err_field_label

        # Create default validator if not given.
        if not validator:
            validator = Draft202012Validator(
                json_schema, format_checker=FormatChecker()
            )
        schema_props = json_schema["properties"]

        validated_json_data = []
        errors = defaultdict(dict)

        stderr.print("[blue] Start processing the JSON file")

        # Start validation
        for idx, item_row in enumerate(json_data):
            sample_id_value = item_row.get(sample_id_field, f"UnknownSample#{idx}")

            # Collect all errors (don't raise immediately)
            validation_errors = list(validator.iter_errors(item_row))

            # Run the custom validator to check if errors should be ignored
            validation_errors = relecov_tools.assets.schema_utils.custom_validators.validate_with_exceptions(
                json_schema, item_row, validation_errors
            )

            if not validation_errors:
                validated_json_data.append(item_row)

            else:
                # Process remaining errors
                for error in validation_errors:
                    if error.cause:
                        # Probably generated by a custom validator or format_checker
                        error.message = str(error.cause)
                    try:
                        if error.validator == "required":
                            error_field = list(error.message.split("'"))[1]
                        elif error.validator == "anyOf":
                            # AnyOf errors include multiple clauses so they need some processing
                            multi_errdict = {}
                            for suberror in error.context:
                                error_type = suberror.validator
                                suberr_label = get_property_label(
                                    schema_props, suberror.validator_value[0]
                                )
                                label_message = suberror.message.replace(
                                    suberror.validator_value[0], suberr_label
                                )
                                if suberror.validator in multi_errdict:
                                    multi_errdict[error_type].append(
                                        (suberr_label, label_message)
                                    )
                                else:
                                    multi_errdict[error_type] = [
                                        (suberr_label, label_message)
                                    ]
                            error_field = ""
                            multi_message = {}
                            # Combine the different error messages from this AnyOf in a single message
                            for errtype, fieldtups in multi_errdict.items():
                                failed_fields = " or ".join([t[0] for t in fieldtups])
                                clean_message = (
                                    fieldtups[0][1]
                                    .replace(fieldtups[0][0], "")
                                    .strip("'")
                                )
                                if error_field:
                                    error_field = error_field + " and"
                                error_field = error_field + failed_fields
                                multi_message[errtype] = (
                                    f"{failed_fields}: {clean_message}"
                                )
                            # Override error.message with the combination of the sub-messages
                            error.message = "Any of the following: " + " --- ".join(
                                multi_message.values()
                            )
                        elif error.absolute_path:
                            error_field = str(error.absolute_path[0])
                        else:
                            error_field = error.validator + " error: " + error.message
                    except Exception as ex:
                        errtxt = f"Error extracting error_field from: {error.validator_value}, {ex}"
                        error_field = str(error)
                        errors["fields"][errtxt] = error.validator_value
                        errors["samples"].setdefault(errtxt, []).append(sample_id_value)
                        continue

                    # Try to get the human-readable label from the schema
                    err_field_label = get_property_label(schema_props, error_field)
                    # Format the error message
                    error.message = error.message.replace(error_field, err_field_label)
                    error_text = f"Error in column {err_field_label}: {error.message}"

                    # Log errors for summary
                    errors["fields"][error_text] = error_field
                    errors["samples"].setdefault(error_text, []).append(sample_id_value)

                # Add the invalid row to the list
        return validated_json_data, errors

    def summarize_errors(self, errors):
        """Summarize errors from validation process and add them to log_summary

        Args:
            errors (dict): Error dict from validate_instances()
        """

        def truncate_error_message(error_text, max_length):
            truncated_msg = (
                error_text[:max_length] + "..."
                if len(error_text) > max_length
                else error_text
            )
            return truncated_msg

        stderr.print("[blue] --------------------")
        stderr.print("[blue] VALIDATION SUMMARY")
        stderr.print("[blue] --------------------")
        self.log.info("Validation summary:")
        max_length = 250
        for error_type, failed_samples in errors["samples"].items():
            count = len(failed_samples)
            field_with_error = errors["fields"][error_type]
            error_text = f"{count} samples failed validation for {field_with_error}: {error_type}"
            truncated_msg = truncate_error_message(error_text, max_length)
            self.logsum.add_warning(entry=truncated_msg)
            for failsamp in failed_samples:
                err_msg = truncate_error_message(error_type, max_length)
                self.logsum.add_error(sample=failsamp, entry=err_msg)
            stderr.print(f"[red]{truncated_msg}")
            stderr.print("[red] --------------------")
        return

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
        self.log.error("Some of the samples in json metadata were not validated")
        stderr.print("[red] Some of the Samples are not validate")
        if metadata is None:
            metadata = relecov_tools.utils.prompt_path(
                msg="Select the metadata file to select those not-validated samples."
            )
        if not os.path.isfile(metadata):
            self.log.error("Metadata file %s does not exist", metadata)
            stderr.print(
                "[red] Unable to create excel file for invalid samples. Metadata file ",
                metadata,
                " does not exist",
            )
            raise FileNotFoundError(
                f"Unable to create excel file for invalid samples. Metadata file {metadata} does not exist"
            )
        sample_list = []
        stderr.print("Start preparation of invalid samples")
        self.log.info("Start preparation of invalid samples")
        for row in invalid_json:
            sample_list.append(str(row[self.sample_id_field]))
        wb = openpyxl.load_workbook(metadata)
        try:
            ws_sheet = wb[self.excel_sheet]
        except KeyError:
            logtxt = f"No sheet named {self.excel_sheet} could be found in {metadata}"
            self.log.error(logtxt)
            raise
        tag = "Sample ID given for sequencing"
        # Check if mandatory colum ($tag) is defined in metadata.
        try:
            header_row = [idx + 1 for idx, x in enumerate(ws_sheet.values) if tag in x][
                0
            ]
        except IndexError:
            self.log.error(
                f"Column with tag '{tag}' not found in any row of the Excel sheet."
            )
            stderr.print(f"[red]Column with tag '{tag}' not found. Cannot continue.")
            raise
        row_to_del = []
        row_iterator = ws_sheet.iter_rows(min_row=header_row + 1, max_row=ws_sheet.max_row)
        consec_empty_rows = 0
        id_col = [
            idx for idx, val in enumerate(ws_sheet[header_row]) if val.value == tag
        ][0]
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
                    self.log.error(
                        "Unable to delete row %s from metadata file because of",
                        idx,
                        e,
                    )
                    stderr.print(f"[red] Unable to delete row {idx} becuase of {e}")
                    raise
        os.makedirs(out_folder, exist_ok=True)
        new_name = "invalid_" + os.path.basename(metadata)
        m_file = os.path.join(out_folder, new_name)
        self.log.info("Saving excel file with the invalid samples")
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
        self.log.info("Saving Json file with the validated samples in %s", file_path)
        stderr.print(f"Saving Json file with the validated samples in {file_path}")
        relecov_tools.utils.write_json_to_file(valid_json_data, file_path)
        return

    def validate_registry_file(self):
        """Validate specified registry file path. Try to get it from config if invalid."""
        # Parse file containing sample IDs registry
        if self.registry_path is not None and relecov_tools.utils.file_exists(
            os.path.abspath(os.path.expanduser(self.registry_path))
        ):
            self.registry_path = os.path.abspath(os.path.expanduser(self.registry_path))
        else:
            config_json = ConfigJson(extra_config=True)
            try:
                default_path = config_json.get_topic_data(
                    "validate_config", "default_sample_id_registry"
                )
            except KeyError:
                default_path = None

            if default_path and relecov_tools.utils.file_exists(default_path):
                self.registry_path = default_path
            else:
                stderr.print(
                    "[yellow]No valid ID registry found. Please select the file manually."
                )
                prompted_path = relecov_tools.utils.prompt_path(
                    "Select the JSON file with registered unique sample IDs"
                )
                if relecov_tools.utils.file_exists(prompted_path):
                    self.registry_path = prompted_path
                else:
                    stderr.print(
                        "[red]No valid ID registry file could be found or selected."
                    )
                    raise FileNotFoundError("No valid ID registry file could be found")

        # Read id registry file
        try:
            self.id_registry = relecov_tools.utils.read_json_file(self.registry_path)
        except Exception as e:
            stderr.print(f"[red]Failed to read ID registry JSON: {e}")
            self.log.error(f"Failed to read ID registry JSON: {e}")
            raise
        return

    def validate_invexcel_args(self):
        """Validate arguments needed to create invalid_samples.xlsx file"""
        if not self.excel_sheet:
            conf_subdata = self.config.get_topic_data(
                "sftp_handle", "metadata_processing"
            )
            try:
                self.excel_sheet = conf_subdata["excel_sheet"]
            except KeyError:
                self.log.error("Default metadata sheet name should be in config file")
                raise
        return

    def update_unique_id_registry(self, valid_json_data):
        """Prepare ID registry. Validated samples will be asigned with an unique ID"""
        new_ids_to_save = {}
        for sample in valid_json_data:
            # If sample has validated, then assign an unique id
            if "unique_sample_id" not in sample:
                new_id = self.generate_incremental_unique_id(
                    {**self.id_registry, **new_ids_to_save}
                )
                sample["unique_sample_id"] = new_id
            # If sample has validated, then assign an unique id
            if "unique_sample_id" not in sample:
                new_id = self.generate_incremental_unique_id(
                    {**self.id_registry, **new_ids_to_save}
                )
                sample["unique_sample_id"] = new_id
                new_ids_to_save[new_id] = {
                    "sequencing_sample_id": sample["sequencing_sample_id"],
                    "lab_code": self.lab_code,
                    "generated_at": datetime.now().isoformat(timespec="seconds"),
                }
        self.save_new_ids(new_ids_to_save)
        return valid_json_data

    def generate_incremental_unique_id(self, current_registry, prefix="RLCV"):
        """Generates an incremental unique ID using as baseline the latest record
        in a registry.

        Args:
            current_registry (dict): dict containing sample's unique IDs
            prefix (str, optional): String that preceeds the unique ID. Defaults to "RLCV".

        Returns:
            string: An unique ID
        """
        existing_numbers = []
        for uid in current_registry:
            if uid.startswith(prefix):
                try:
                    number = int(uid.replace(f"{prefix}-", ""))
                    existing_numbers.append(number)
                except ValueError:
                    continue
        next_number = max(existing_numbers, default=0) + 1
        return f"{prefix}-{next_number:09d}"

    def save_new_ids(self, new_ids_dict):
        """Updates sample id registry by adding sample unique ids of new validated samples.

        Args:
            new_ids_dict (dict): Dict of new unique sample IDs.
        """
        updated_registry = {**self.id_registry, **new_ids_dict}
        relecov_tools.utils.write_json_to_file(updated_registry, self.registry_path)
        self.log.info(f"Added {len(new_ids_dict)} new sample IDs to registry")

    def validate(self):
        """Validate samples from metadata, create an excel with invalid samples,
        and a json file with the validated ones
        """
        self.log.info("Validate the given schema")
        self.validate_schema()
        self.log.info("Preparing validator based on config")
        starting_date = self.config.get_topic_data("validate_config", "starting_date")
        date_checker = (
            relecov_tools.assets.schema_utils.custom_validators.make_date_checker(
                datetime.strptime(starting_date, "%Y-%m-%d").date(),
                datetime.now().date(),
            )
        )
        validator = Draft202012Validator(self.json_schema, format_checker=date_checker)
        self.log.info("Starting validation process of JSON file against schema")
        valid_json_data, errors = SchemaValidation.validate_instances(
            self.json_data,
            self.json_schema,
            sample_id_field=self.sample_id_field,
            validator=validator,
        )
        for sample in valid_json_data:
            sample_id_value = sample.get(self.sample_id_field)
            self.logsum.feed_key(sample=sample_id_value)
        if errors:
            self.summarize_errors(errors)

        # Add all valid samples to the unique_id registry file
        self.validate_registry_file()
        valid_json_data = self.update_unique_id_registry(valid_json_data)

        invalid_json = [x for x in self.json_data if x not in valid_json_data]
        if invalid_json:
            log_text = "Summary: %s valid and %s invalid samples"
            self.logsum.add_warning(
                entry=log_text % (len(valid_json_data), len(invalid_json))
            )
            self.validate_invexcel_args()
            self.create_invalid_metadata(invalid_json, self.metadata, self.out_folder)
        else:
            stderr.print("[green]Sucessful validation, no invalid file created!!")
            self.log.info("Sucessful validation, no invalid file created.")
        if valid_json_data:
            self.log.info("Creating json_file with validated samples...")
            self.create_validated_json(valid_json_data, self.out_folder)
        else:
            log_text = "All the samples were invalid. No valid file created"
            self.logsum.add_error(entry=log_text)
            stderr.print(f"[red]{log_text}")
        self.parent_create_error_summary(called_module="validate")
        return valid_json_data, invalid_json
