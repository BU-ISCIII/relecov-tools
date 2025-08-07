#!/usr/bin/env python
import importlib
import os
import re
import shutil
from datetime import datetime
from typing import Any

import numpy as np  # pyright: ignore[reportMissingImports]
import pandas as pd
import rich.console
from rich.prompt import Prompt

import relecov_tools.utils
import relecov_tools.validate
from relecov_tools.base_module import BaseModule
from relecov_tools.config_json import ConfigJson

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class BioinfoReportLog:
    def __init__(self, log_report=None):
        if not log_report:
            self.report = {"error": {}, "valid": {}, "warning": {}}
        else:
            self.report = log_report

    def update_log_report(self, method_name, status, message):
        """Update the progress log report with the given method name, status, and message.

        Args:
            method_name (str): The name of the method being logged.
            status (str): The status of the log message, can be one of 'valid', 'error', or 'warning'.
            message (str): The message to be logged.

        Returns:
            dict: The updated progress log report.

        Raises:
            ValueError: If an invalid status is provided.
        """
        if status == "valid":
            self.report["valid"].setdefault(method_name, []).append(message)
            return self.report
        elif status == "error":
            self.report["error"].setdefault(method_name, []).append(message)
            return self.report
        elif status == "warning":
            self.report["warning"].setdefault(method_name, []).append(message)
            return self.report
        else:
            raise ValueError("Invalid status provided.")

    def print_log_report(self, name, sections):
        """Prints the log report by calling util's function.

        Args:
            name (str): The name of the log report.
            sections (list of str): The sections of the log report to be printed.

        Returns:
            None
        """
        relecov_tools.utils.print_log_report(self.report, name, sections)


# TODO: Add method to validate bioinfo_config.json file requirements.
class BioinfoMetadata(BaseModule):
    def __init__(
        self,
        json_file: str | None = None,
        json_schema_file: str | None = None,
        input_folder: str | None = None,
        output_dir: str | None = None,
        software_name: str | None = None,
        update: bool = False,
        soft_validation: bool = False,
    ):

        super().__init__(output_dir=output_dir, called_module=__name__)
        self.log.info("Initiating read-bioinfo-metadata process")
        # Init params
        self.update = update
        self.soft_validation = soft_validation

        # Init logs
        self.log_report = BioinfoReportLog()
        self.logsum = self.parent_log_summary(output_dir=output_dir)

        # Init attributes
        self._init_input_folder(input_folder)
        self._init_software_name(software_name)
        self._init_json_schema(json_schema_file)
        self._init_output_dir(output_dir)
        self._init_readlabmeta_json(json_file)
        self._init_j_data_and_batch()

        # Init config
        self._init_bioinfo_config()

    def _init_json_schema(self, json_schema_file: str | None = None) -> None:
        """Initializes the JSON schema for bioinformatics metadata.

        Args:
            json_schema_file (str) optional
                Path to the JSON schema file. If not provided, it will use the default schema defined in the config.

        Returns:
            None
        """
        if json_schema_file is None:
            config_json = ConfigJson()
            schema_name = config_json.get_topic_data("generic", "relecov_schema")
            json_schema_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "schema", str(schema_name)
            )
        self.json_schema = relecov_tools.utils.read_json_file(json_schema_file)

    def _init_output_dir(self, output_dir: str | None = None) -> None:
        """Initializes the output directory for storing results.

        Args:
            output_dir (str | None)
                Path to the output directory. If not provided, it will prompt the user to select one.

        Returns:
            None
        """
        if output_dir is None:
            self.output_dir = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_dir = os.path.realpath(output_dir)

    def _init_readlabmeta_json(self, json_file: str | None = None) -> None:
        """Initializes the readlab metadata JSON file.

        Args:
            json_file (str | None): Path to the readlab metadata JSON file.
            If not provided, it will prompt the user to select one.

        Raises:
            ValueError
                If the provided JSON file does not exist or is not a valid file.
        Returns:
            None
        """
        if json_file is None:
            json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file that was created by the pipeline-manager or read-lab-metadata module."
            )
        if json_file is None or not os.path.isfile(json_file):
            self.update_all_logs(
                self.__init__.__name__,
                "error",
                f"file {json_file} does not exist",
            )
            self.log_report.print_log_report(self.__init__.__name__, ["error"])
            raise ValueError(f"Json file {json_file} does not exist, cannot continue.")
        self.readlabmeta_json_file = json_file

    def _init_j_data_and_batch(self) -> None:
        """Initializes the j_data and batch ID from the readlab metadata JSON file.
        Args:
            None
        Returns:
            None
        """
        stderr.print("[blue]Reading lab metadata json")
        self.j_data = self.collect_info_from_lab_json()
        batch_id = self.get_batch_id_from_data(self.j_data)
        self.set_batch_id(batch_id)

    def _init_input_folder(self, input_folder: str | None = None) -> None:
        """Initializes the input folder for bioinformatics analysis files.

        Args:
        input_folder (str | None)
            Path to the input folder. If not provided, it will prompt the user to select one.
        Returns:
            None
        """
        if input_folder is None:
            self.input_folder = relecov_tools.utils.prompt_path(
                msg="Select the input folder"
            )
        else:
            self.input_folder = input_folder

    def _init_software_name(self, software_name: str | None = None) -> None:
        """Initializes the software name used in the bioinformatic analysis.

        Args:
        software_name (str | None)
            Name of the software, pipeline or tool used in the bioinformatic analysis.
            If not provided, it will prompt the user to select one.

        Raises:
        ValueError
            If no software name is provided, it raises a ValueError and logs an error message.

        Returns:
            None
        """
        if software_name is None:
            software_name = relecov_tools.utils.prompt_path(
                msg="Select the software, pipeline or tool use in the bioinformatic analysis: "
            )
        if software_name:
            self.software_name = software_name
        else:
            self.update_all_logs(
                self.__init__.__name__,
                "error",
                "No software name provided, cannot continue.",
            )
            self.log_report.print_log_report(self.__init__.__name__, ["error"])
            raise ValueError("No software name provided, cannot continue.")

    def _init_bioinfo_config(self) -> None:
        """Initializes the bioinformatics configuration based on the provided software name.

        Raises:
            ValueError: If the software name is not found or no configuration is available for it.
        """
        self.bioinfo_json_file = os.path.join(
            os.path.dirname(__file__), "conf", "bioinfo_config.json"
        )

        bioinfo_config = ConfigJson(self.bioinfo_json_file)
        available_software = relecov_tools.utils.get_available_software(
            self.bioinfo_json_file
        )
        method_name = self.__init__.__name__

        # Raises ValueError if the software name is not in the available software list
        if self.software_name not in available_software:
            options = ", ".join(available_software)
            error_msg = (
                f"No configuration available for '{self.software_name}'. "
                f"Currently, the only available software options are: {options}"
            )
            self.update_all_logs(method_name, "error", error_msg)
            self.log_report.print_log_report(self.__init__.__name__, ["error"])
            raise ValueError(error_msg)

        config = bioinfo_config.get_configuration(self.software_name)
        # Raises ValueError if the configuration for the software is None
        if config is None:
            error_msg = (
                f"No configuration found for '{self.software_name}' "
                f"in {self.bioinfo_json_file}."
            )
            self.update_all_logs(method_name, "error", error_msg)
            self.log_report.print_log_report(self.__init__.__name__, ["error"])
            raise ValueError(error_msg)

        # set software configuration indicating that it is a dictionary
        # and access its keys and values directly making sure that's it not going to ever be None.
        self.software_config: dict[str, Any] = config

    def update_all_logs(self, method_name: str, status: str, message: str) -> dict:
        """
        Updates the log report with the given method name, status, and message.

        Args:
        method_name (str)
            The name of the method being logged.
        status (str)
            The status of the log message, can be one of 'valid', 'error', or 'warning'.
        message (str)
            The message to be logged.

        Returns:
            report (dict): The updated log report.
        """
        report = self.log_report.update_log_report(method_name, status, message)
        if status == "error":
            self.logsum.add_error(key=method_name, entry=message)
        elif status == "warning":
            self.logsum.add_warning(key=method_name, entry=message)
        return report

    def scan_directory(
        self,
    ) -> dict[str, list[str]]:
        """Scanns bioinfo analysis directory and identifies files according to the file name patterns defined in the software configuration json.

        Returns:
            files_found (dict): A dictionary containing file paths found based on the definitions provided
            in the bioinformatic JSON file within the software scope (self.software_config).
        """
        # set method name
        method_name = f"{self.scan_directory.__name__}"

        # Get the total number of files and scanned files in all folders in self.input_folder
        total_files, scanned_files_per_folder = self._walk_input_folder()
        files_found = self._find_matching_files(method_name, scanned_files_per_folder)
        # search for files matching the patterns defined in the software configuration
        if files_found:
            # If files are found, update the log report and return the files_found dictionary
            scan_success_msg = f"Scanning process succeed. Scanned {total_files} files."
            self.update_all_logs(
                self.scan_directory.__name__,
                "valid",
                scan_success_msg,
            )
            self.log_report.print_log_report(method_name, ["valid", "warning"])
            return files_found
        else:
            # If no files are found matching the patterns, log an error and raise a ValueError
            err_msg = f"No files found in '{self.input_folder}' according to '{os.path.basename(self.bioinfo_json_file)}' file name patterns."
            self.update_all_logs(
                method_name,
                "error",
                err_msg,
            )
            self.log_report.print_log_report(method_name, ["error"])
            raise ValueError(err_msg)

    def _walk_input_folder(self) -> tuple[int, list[tuple[str, list[str]]]]:
        """Walk through the input folder and collect all files.
        Returns:
        """
        total_files = 0
        scanned_files_per_folder = []
        for root, dirs, files in os.walk(self.input_folder, topdown=True):
            # Skipping nextflow's work junk directory
            # dirs[:] is used to modify the list of directories in place
            dirs[:] = [d for d in dirs if d != "work"]
            total_files += len(files)
            scanned_files_per_folder.append((root, files))
        return total_files, scanned_files_per_folder

    def _find_matching_files(
        self, method_name: str, scanned_files_per_folder: list[tuple[str, list[str]]]
    ) -> dict[str, list[str]]:
        """For each topic in the software configuration, search for matching files.

        Args:
        scanned_files_per_folder (list[tuple[str, list[str]]]): A list of tuples containing the root path and a list of files in that path.
        method_name (str): The name of the method being logged.

        Returns:
        files_found (dict): A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).
        """
        files_found = {}
        # Iterate over each topic in the software configuration
        for topic_key, topic_scope in self.software_config.items():
            # if topic has not fn (file pattern) defined, skip it
            if not self._topic_has_file_pattern(topic_key, topic_scope, method_name):
                continue

            # get file pattern from topic scope now that we know it has a valid pattern
            file_pattern = topic_scope["fn"]

            # Search for files matching the pattern in the scanned files
            for root_path, file_list in scanned_files_per_folder:
                if matching_files := self._get_matching_files_for_pattern(
                    file_list, root_path, file_pattern
                ):
                    # If matching files are found, add them to the files_found dictionary
                    # set default to an empty list if the topic_key is not already present
                    files_found.setdefault(topic_key, []).extend(matching_files)

        return files_found

    def _topic_has_file_pattern(
        self, topic_key: str, topic_scope: dict, method_name: str
    ) -> bool:
        """Check if topic_scope has a valid pattern; log warning if not.

        Args:
            topic_key (str): The key of the topic in the software configuration.
            topic_scope (dict): The scope of the topic in the software configuration.
            method_name (str): The name of the method being logged.

        Returns:
            bool: True if the topic has a valid file pattern, False otherwise.
        """
        # set pattern
        pattern = topic_scope.get("fn")

        # Check if pattern is valid
        is_invalid_type = not isinstance(pattern, str)
        # Check if pattern is an empty string
        is_empty_string = isinstance(pattern, str) and not pattern.strip()
        # If pattern is invalid or empty, log a warning and return False
        if is_invalid_type or is_empty_string:
            topic_name = f"{self.software_name}.{topic_key}"
            error_msg = f"Invalid or missing 'fn' (file pattern) in '{topic_name}'."
            self.update_all_logs(method_name, "warning", error_msg)
            return False
        return True

    def _get_matching_files_for_pattern(
        self, file_list: list[str], root_path: str, file_pattern: str
    ) -> list[str]:
        """Return a list of files in file_list under root_path that match file_pattern.
        Args:
            file_list (list[str]): A list of file names to search for.
            root_path (str): The root path where the files are located.
            file_pattern (str): The regex pattern to match the files.
        Returns:
            matching_files (list[str]): A list of file paths that match the file_pattern.
        """
        matching_files = []
        # Iterate over each file in the file_list
        for file_name in file_list:
            file_path = os.path.join(root_path, file_name)
            # Check if the file matches the pattern
            # update matching_files with the file matching the pattern
            if re.search(file_pattern, file_path):
                matching_files.append(file_path)
        return matching_files

    def mapping_over_table(
        self,
        j_data: list[dict],
        map_data: dict,
        mapping_fields: dict,
        table_name: str,
    ) -> list[dict]:
        """
        Maps bioinformatics metadata from map_data to j_data based on the mapping_fields.

        Args:
            j_data (list[dict]): Metadata from the lab (one item per sample).
            map_data (dict): Processed bioinformatic metadata, indexed by sample name.
            mapping_fields (dict): Mapping from JSON fields to software fields as defined in the config.
            table_name (str): Name or path of the mapping table.

        Returns:
            list[dict]: The updated j_data list with mapped bioinformatic fields.
        """
        # Set method name and initialize error lists
        method_name = f"{self.mapping_over_table.__name__}:{self.software_name}.{self.current_config_key}"
        errors: list[str] = []
        field_errors: dict = {}
        field_valid: dict = {}

        # Get sample ids from j_data and map_data
        sample_ids = {
            sample_name for row in j_data if (sample_name := self._get_sample_name(row))
        }
        matched_samples = sample_ids & set(map_data.keys())
        # check if samples from j_data match the samples in map_data
        no_samples = not matched_samples

        # iterate over j_data
        for row in j_data:
            # get sample name from row
            sample_name = self._get_sample_name(row)

            if not sample_name:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"Sequencing_sample_id missing in {row.get('collecting_sample_id')}... Skipping...",
                )
                continue
            # If sample_name is in map_data, map the fields
            if sample_name in map_data:
                self._map_fields_to_row(
                    row,
                    map_data,
                    mapping_fields,
                    sample_name,
                    field_valid,
                    field_errors,
                )
            # If the software configuration allows multiple samples and no samples were matched, handle it accordingly
            # Used for tools that do not have a table with samples but rather individual files
            elif (
                not self.software_config[self.current_config_key].get(
                    "multiple_samples"
                )
                and no_samples
            ):
                self._map_non_multiple_sample(
                    row, map_data, mapping_fields, field_valid, field_errors
                )
            else:
                errors.append(sample_name)
                for field in mapping_fields:
                    row[field] = "Not Provided [SNOMED:434941000124101]"

        # work around when map_data comes from several per-sample tables/files instead of single table
        # get the dirname where the tables/files are or basename of the table_name
        resolved_table = (
            os.path.dirname(table_name[0]) if len(table_name) > 2 else table_name[0]
        )

        # Manage errors and field validations
        if errors:
            self.update_all_logs(
                method_name,
                "warning",
                f"{len(errors)} samples missing in '{resolved_table}': {', '.join(errors)}.",
            )
        else:
            self.update_all_logs(
                method_name,
                "valid",
                f"All samples were successfully found in {resolved_table}.",
            )

        if field_errors:
            self.update_all_logs(
                method_name,
                "warning",
                f"Missing fields in {resolved_table}:\n\t{field_errors}",
            )
        else:
            self.update_all_logs(
                method_name,
                "valid",
                f"Successfully mapped fields in {', '.join(field_valid.keys())}.",
            )

        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

    def _get_sample_name(self, row: dict) -> str | None:
        """
        Generates a sample name from sequencing_sample_id and unique_sample_id if present.

        Args:
            row (dict): The metadata row from j_data.

        Returns:
            str | None: A unique sample identifier, or None if sequencing_sample_id is missing.
        """
        # get sample ids from j_data
        # If unique_sample_id is not present, it will use only sequencing_sample_id.
        # else it will use both to create a unique sample id.
        seq_id = row.get("sequencing_sample_id")
        if not seq_id:
            return None
        uniq_id = row.get("unique_sample_id")
        return f"{seq_id}_{uniq_id}" if uniq_id else seq_id

    def _map_fields_to_row(
        self,
        row: dict,
        map_data: dict,
        mapping_fields: dict,
        sample_name: str,
        field_valid: dict,
        field_errors: dict,
    ) -> None:
        """
        Maps fields for a given sample from map_data using the mapping_fields.

        Args:
            row (dict): The metadata row to update.
            map_data (dict): Dictionary with bioinfo values by sample name.
            mapping_fields (dict): Mapping of fields.
            sample_name (str): The key in map_data.
            field_valid (dict): Accumulator for successfully mapped fields.
            field_errors (dict): Accumulator for mapping errors.
        """
        # Iterate over mapping fields and map values to the row
        for field, value in mapping_fields.items():
            try:
                # Get the raw value from map_data for the sample
                raw_val = map_data[sample_name][value]
                # Replace NA values if needed
                raw_val = self.replace_na_value_if_needed(field, raw_val)
                # get the expected type from the JSON schema
                expected_type = (
                    self.json_schema["properties"].get(field, {}).get("type", "string")
                )
                # convert the raw value to the expected type
                row[field] = relecov_tools.utils.cast_value_to_schema_type(
                    raw_val, expected_type
                )
                # assign the field to field value validated
                field_valid[sample_name] = {field: value}

            except KeyError as e:
                field_errors[sample_name] = {field: str(e)}
                row[field] = "Not Provided [SNOMED:434941000124101]"

    def _map_non_multiple_sample(
        self,
        row: dict,
        map_data: dict,
        mapping_fields: dict,
        field_valid: dict,
        field_errors: dict,
    ) -> None:
        """
        Handles mapping for tools where samples are not in a table but individually defined.

        Args:
            row (dict): The metadata row to update.
            map_data (dict): Bioinfo metadata from config-defined keys.
            mapping_fields (dict): Dictionary with nested mappings.
            field_valid (dict): Collector for valid mappings.
            field_errors (dict): Collector for failed mappings.
        """
        # Iterate over mapping fields and map values to the row
        for field, value_dict in mapping_fields.items():
            # iterate over the value_dict which contains json_field and software_key
            for json_field, software_key in value_dict.items():
                try:
                    raw_val = map_data[software_key][field]
                    expected_type = (
                        self.json_schema["properties"]
                        .get(json_field, {})
                        .get("type", "string")
                    )
                    row[json_field] = relecov_tools.utils.cast_value_to_schema_type(
                        raw_val, expected_type
                    )
                    field_valid[software_key] = {json_field: field}
                except KeyError as e:
                    field_errors[software_key] = {json_field: str(e)}
                    row[json_field] = "Not Provided [SNOMED:434941000124101]"

    def validate_sample_names(self) -> None:
        """Validate that the sequencing_sample_id from the JSON input is present in the samples_id.txt.

        Raises:
            ValueError: If no sample from the JSON input matches the samples in the samples_id.txt.
        """
        # Check if samples_id.txt exists
        samples_id_path = os.path.join(self.input_folder, "samples_id.txt")
        with open(samples_id_path, "r") as file:
            samples_id_list = [line.strip() for line in file.readlines()]

        # Get sample names from JSON input.
        # if unique_sample_id is not present, it will use only sequencing_sample_id.
        json_samples = [
            sample_name
            for sample in self.j_data
            if (sample_name := self._get_sample_name(sample)) is not None
        ]
        matching_samples = set(json_samples).intersection(samples_id_list)

        if not matching_samples:
            raise ValueError(
                "No sample from the JSON input matches the samples in the provided analysis folder."
            )
        print(
            f"Found {len(matching_samples)}/{len(json_samples)} matching samples in the samplesheet."
        )
        self.log.info(
            f"Found {len(matching_samples)}/{len(json_samples)} matching samples in the samplesheet."
        )

    def validate_software_mandatory_files(self, files_dict: dict) -> None:
        """Validates the presence of all mandatory files as defined in the software configuration JSON.

        Args:
            files_dict (dict{str:str}): A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).
        """
        method_name = f"{self.validate_software_mandatory_files.__name__}"
        missing_required = []
        # Iterate over the software configuration keys to check for required files
        for key in self.software_config:
            if key == "fixed_values":
                continue
            if self.software_config[key].get("required") is True:
                try:
                    files_dict[key]
                except KeyError:
                    missing_required.append(key)
                    continue
            else:
                continue
        # If any required files are missing, log an error and raise a ValueError
        if missing_required:
            error_msg = f"Missing mandatory files in {self.software_name}:{', '.join(missing_required)}"
            self.update_all_logs(
                method_name,
                "error",
                error_msg,
            )
            self.log_report.print_log_report(method_name, ["error"])
            raise ValueError(error_msg)
        else:
            self.update_all_logs(
                method_name, "valid", "Successful validation of mandatory files."
            )
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return

    def add_bioinfo_results_metadata(
        self,
        files_dict: dict,
        file_tag: str,
        j_data: list[dict],
        output_dir: str | None = None,
    ) -> tuple[list[dict], list[dict]]:
        """
        Adds metadata from bioinformatics results to j_data.
        Calls file handler and maps handled data into j_data.

        Args:
            files_dict (dict): Mapping of config keys to file paths.
            file_tag (str): Tag used for output filenames.
            j_data (list[dict]): List of lab metadata entries.
            output_dir (str, optional): Path to write outputs.

        Returns:
            tuple: (updated j_data, extra_json_data)
        """

        # Initialize method name and extra_json_data
        method_name = self.add_bioinfo_results_metadata.__name__
        extra_json_data: list[dict] = []
        j_data_mapped = j_data

        # Iterate over each key in the software configuration
        for key in self.software_config:
            # skip workflow_summary and fixed_values keys that will be handled later
            if key in ("workflow_summary", "fixed_values"):
                continue

            self.current_config_key = key
            map_method_name = f"{method_name}:{self.software_name}.{key}"

            if key not in files_dict:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"No file path found for '{self.software_name}.{key}'",
                )
                continue

            stderr.print(f"[blue]Start processing {self.software_name}.{key}")
            self.log.info(f"Start processing {self.software_name}.{key}")

            # Process the config key to get data to map and extra data if any
            data_to_map, extra_data = self._process_config_key(
                key, files_dict[key], file_tag, output_dir
            )

            if extra_data:
                extra_json_data.append(extra_data)

            # get mapping fields from the software configuration
            mapping_fields = self.software_config[key].get("content")
            if not mapping_fields:
                self.update_all_logs(
                    map_method_name,
                    "warning",
                    f"No metadata found to perform mapping from '{self.software_name}.{key}' despite 'content' fields being defined.",
                )
                self.log_report.print_log_report(map_method_name, ["warning"])
                continue

            # If data_to_map is not empty, perform mapping
            if data_to_map:
                j_data_mapped = self.mapping_over_table(
                    j_data=j_data,
                    map_data=data_to_map,
                    mapping_fields=mapping_fields,
                    table_name=files_dict[key],
                )
            else:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"No metadata found to perform standard mapping when processing '{self.software_name}.{key}'",
                )

        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data_mapped, extra_json_data

    def _process_config_key(
        self,
        key: str,
        file_path: list[str],
        file_tag: str,
        output_dir: str | None,
    ) -> tuple[dict | None, dict | None]:
        """
        Handles the data processing logic for a single config key.

        Args:
            key (str): The config key to process.
            file_path (list): Path to the corresponding file.
            file_tag (str): Tag for generated outputs.
            output_dir (str | None): Output directory.
            method_name (str): Name for logging context.

        Returns:
            tuple: (data_to_map, extra_json_data)
        """
        config = self.software_config[key]
        file_name = config.get("fn")
        func_name = config.get("function")
        # If func_name is None, it means we will handle the file as a table with a default function
        if func_name is None:
            data = self.handling_tables(file_path, file_name)
        # If func_name is defined, we will process the file using the function defined in the config
        # and present in assets.pipeline_utils
        else:
            data = self.process_metadata(
                file_path, file_tag=file_tag, func_name=func_name, out_path=output_dir
            )

        if config.get("split_by_batch") and config.get("extra_dict"):
            return None, data
        return data, None

    def handling_tables(self, file_list: list, conf_tab_name: str) -> dict:
        """Reads a tabular file in different formats and returns a dictionary containing
        the corresponding data for each sample.

        Args:
            file_list (list): A list of file path/s to be processed.
            conf_tab_name (str): Name of the table in the defined config file.

        Returns:
            data (dict): A dictionary containing metadata as defined in handling_files.
        """
        method_name = f"{self.add_bioinfo_results_metadata.__name__}:{self.handling_tables.__name__}"
        # get file extension and sample index column position
        file_ext = os.path.splitext(conf_tab_name)[1]
        sample_idx_col_pos = self.get_sample_idx_col_pos(self.current_config_key)
        # allowed file extensions and their separators
        ext_dict = {".csv": ",", ".tsv": "\t", ".tab": "\t"}

        mapping_fields = self.software_config[self.current_config_key].get("content")

        if not mapping_fields:
            return {}

        if conf_tab_name.endswith(".gz"):
            inner_ext = os.path.splitext(conf_tab_name.strip(".gz"))[1]
            if inner_ext in ext_dict:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"Expected tabular file '{conf_tab_name}' is compressed and cannot be processed.",
                )
            return {}

        if file_ext in ext_dict:
            try:
                return relecov_tools.utils.read_csv_file_return_dict(
                    file_name=file_list[0],
                    sep=ext_dict[file_ext],
                    key_position=sample_idx_col_pos,
                )
            except FileNotFoundError as e:
                self.update_all_logs(
                    method_name,
                    "error",
                    f"Tabular file not found: '{file_list[0]}': {e}",
                )
                raise FileNotFoundError(
                    f"Tabular file not found: '{file_list[0]}'"
                ) from e
        else:
            self.update_all_logs(
                method_name,
                "error",
                f"Unrecognized defined file name extension '{file_ext}' in '{conf_tab_name}'.",
            )
            raise ValueError(self.log_report.print_log_report(method_name, ["error"]))

    def process_metadata(
        self,
        file_list: list,
        file_tag: str,
        func_name: str,
        out_path: str | None = None,
    ) -> dict:
        """This method dynamically loads and executes the functions specified in config file.
        It is used to apply standard or custom metadata processing depending on the current
        software context (`self.software_name`).

        Args:
            file_list (list): A list of file path/s to be processed.
            func_name (str): The name of the function to execute
            file_tag (str): Tag that will be used for output filenames includes batch date (same as download date) and hex.
            out_path (str): Path to save output files generated during handling_files() process.

        Returns:
            data: A dictionary containing bioinfo metadata handled for each sample.
        """
        method_name = f"{self.add_bioinfo_results_metadata.__name__}:{self.process_metadata.__name__}"
        try:
            # Dynamically import the function from the specified module
            if func_name.startswith("utils/"):
                utils_name = "relecov_tools.assets.pipeline_utils.utils"
                func_name = func_name.split("/", 1)[1]
            else:
                utils_name = f"relecov_tools.assets.pipeline_utils.{self.software_name}"

            # Dynamically import the function from the specified module
            module = importlib.import_module(utils_name)
            # Get method from func_name and execute it.
            func_obj = getattr(module, func_name)
            data = func_obj(
                file_list,
                file_tag=file_tag,
                pipeline_name=self.software_name,
                output_folder=out_path,
            )

        except Exception as e:
            self.update_all_logs(
                method_name,
                "error",
                f"Error occurred while parsing '{func_name}': {e}.",
            )
            self.log_report.print_log_report(method_name, ["error"])
            raise ValueError(f"Error occurred while parsing '{func_name}': {e}.") from e
        return data

    def add_fixed_values(self, j_data: list[dict], out_filename: str) -> list[dict]:
        """Add fixed values to j_data as defined in the bioinformatics configuration (definition: "fixed values")

        Args:
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (one item per sample).
            out_filename (str): File name of the bioinfo_lab_metadata json

        Returns:
            j_data: updated j_data with fixed values added in it.
        """
        method_name = f"{self.add_fixed_values.__name__}"
        try:
            f_values = self.software_config["fixed_values"]
            for row in j_data:
                row["bioinfo_metadata_file"] = out_filename
                for field, value in f_values.items():
                    row[field] = value
            self.update_all_logs(method_name, "valid", "Fields added successfully.")
        except KeyError as e:
            self.update_all_logs(
                method_name, "warning", f"Error found while adding fixed values: {e}"
            )

        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

    def replace_na_value_if_needed(self, field: str, raw_val: str | None) -> str | None:
        """
        Replace 'NA', None or NaN with 'Not Provided [SNOMED:434941000124101]'
        if the field is not required in the schema.

        Args:
            field (str): The field name to check.
            raw_val (str | None): The value to check and potentially replace.
        Returns:
            str: The original value if it is not 'NA', None or NaN, otherwise Not Provided.
        """
        required_fields = self.json_schema.get("required", [])
        is_na = (
            raw_val is None
            or (isinstance(raw_val, str) and raw_val.strip().upper() in ["NA", "NONE"])
            or (isinstance(raw_val, float) and np.isnan(raw_val))
        )
        if is_na and field not in required_fields:
            return "Not Provided [SNOMED:434941000124101]"
        return raw_val

    def map_and_extract_bioinfo_paths(
        self, files_found_dict: dict, j_data: list[dict]
    ) -> list[dict]:
        """
        Adds file paths to j_data for each sample, using the provided files_found_dict
        and the current software configuration.

        Args:
            files_found_dict (dict): Mapping of config keys to file paths found.
            j_data (list[dict]): Sample metadata.

        Returns:
            list[dict]: Updated j_data with file paths.
        """
        method_name = self.map_and_extract_bioinfo_paths.__name__
        sample_name_error = 0
        multiple_sample_files = self.get_multiple_sample_files()

        for row in j_data:
            # ger sample_name that will be a combination of sequencing_sample_id and unique_sample_id
            # if unique_sample_id is not present, it will use only sequencing_sample_id.
            sample_name = self._get_sample_name(row)
            base_cod_path = row.get("sequence_file_path_R1")

            if not sample_name:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"Sequencing_sample_id missing in {row.get('collecting_sample_id')}... Skipping...",
                )
                continue

            if not base_cod_path:
                self.update_all_logs(
                    method_name,
                    "error",
                    f"No 'sequence_file_path_R1' found for sample {sample_name}. Unable to generate paths.",
                )
                continue
            # Iterate over each key (mapping_stats, quality_control, etc.) in files_found_dict
            # Process files associated with each key
            for key, files in files_found_dict.items():
                # select matching file paths based on sample_name and key
                file_paths = self._select_matching_paths(
                    files, sample_name, key, multiple_sample_files
                )
                # set matched file paths to appropriate field in json row
                path_key = self._assign_file_paths_to_row(row, key, file_paths, base_cod_path)

                # Extract files to analysis_results folder if configured
                if self.software_config[key].get("extract"):
                    self.extract_file(
                        file=file_paths,
                        dest_folder=base_cod_path,
                        sample_name=sample_name,
                        path_key=path_key,
                    )

        self.log_report.print_log_report(method_name, ["warning"])
        if sample_name_error == 0:
            self.update_all_logs(method_name, "valid", "File paths added successfully.")
        self.log_report.print_log_report(method_name, ["valid", "warning"])

        return j_data

    def _select_matching_paths(
        self, files: list[str], sample_name: str, key: str, multi_sample_keys: list[str]
    ) -> list[str]:
        """
        Selects matching file paths for a given sample name and config key.
        Args:
            files (list[str]): List of file paths to search.
            sample_name (str): The sample name to match against file paths.
            key (str): The configuration key to check against multi-sample keys.
            multi_sample_keys (list[str]): List of keys that allow multiple samples.
        Returns:
            list[str]: Matching paths or placeholder.
        """
        if not files:
            return ["Not Provided [SNOMED:434941000124101]"]
        # if key is in multi_sample_keys, return all files
        if key in multi_sample_keys:
            return files
        # else return files that match the sample_name
        return [f for f in files if sample_name in f]

    def _assign_file_paths_to_row(
        self,
        row: dict,
        key: str,
        file_paths: list[str],
        base_cod_path: str,
    ) -> None:
        """
        Assigns file path(s) to the row, handling renaming and extraction if needed.
        """
        # get config for the current key
        config = self.software_config[key]
        path_key = config.get("filepath_name", key)

        # get the name of the field configured in config file "filepath_name"
        if config.get("filepath_name"):
            analysis_results_paths = []
            # for each file_path in file_paths
            for f in file_paths:
                # If not provided append and continue
                if f == "Not Provided [SNOMED:434941000124101]":
                    analysis_results_paths.append(f)
                    continue
                # check if configured as extract or function
                extract_or_func = config.get("extract") or config.get("function")
                # if not configured as extract or function, append the file path and continue
                if not extract_or_func:
                    analysis_results_paths.append(f)
                    continue
                # if configured as split_by_batch, rename the file to match the COD
                if config.get("split_by_batch"):
                    base, ext = os.path.splitext(os.path.basename(f))
                    batch_id = row["batch_id"]
                    new_fname = f"{base}_{batch_id}_{self.hex}{ext}"
                else:
                    new_fname = os.path.basename(f)

                analysis_results_path = os.path.join(
                    base_cod_path, "analysis_results", new_fname
                )
                analysis_results_paths.append(analysis_results_path)
            # in case of multiple files, join them with a comma
            row[path_key] = ", ".join(analysis_results_paths)
        else:
            # in case there is no file_path_name defined, use key as path_key
            row[path_key] = ", ".join(file_paths)

        return path_key

    def collect_info_from_lab_json(self) -> list[dict]:
        """Reads lab metadata from a JSON file and creates a list of dictionaries.
        Reads lab metadata from the specified JSON file and converts it into a list of dictionaries.
        This list is used to add the rest of the fields.

        Returns:
            json_lab_data: A list of dictionaries containing lab metadata (aka j_data).
        """
        method_name = f"{self.collect_info_from_lab_json.__name__}"
        try:
            json_lab_data = relecov_tools.utils.read_json_file(
                self.readlabmeta_json_file
            )
        except ValueError as e:
            error_msg = (
                f"Invalid lab-metadata json file: self.{self.readlabmeta_json_file}"
            )
            self.update_all_logs(
                method_name,
                "error",
                error_msg,
            )
            self.log_report.print_log_report(method_name, ["error"])
            raise ValueError(error_msg) from e
        return json_lab_data

    def get_sample_idx_col_pos(self, config_key: str) -> int:
        """Extract which column contain sample names for that specific file from config

        Args:
            config_key (str): Key of the configuration item in the software configuration JSON.

        Returns:
            sample_idx_col_pos: column number which contains sample names
        """
        try:
            sample_idx_col_pos = self.software_config[config_key]["sample_col_idx"] - 1
        except (KeyError, TypeError):
            sample_idx_col_pos = 0
            self.update_all_logs(
                "get_sample_idx_col_pos",
                "warning",
                f"No valid sample-index-column defined in '{self.software_name}.{config_key}'. Using default instead.",
            )
        return sample_idx_col_pos

    def extract_file(
        self,
        file: list[str],
        dest_folder: str,
        sample_name: str | None = None,
        path_key: str | None = None,
    ) -> bool:
        """Copy input file to the given destination, include sample name and key in log
        Args:
            file (list[str]): Paths of the files that are going to be copied
            dest_folder (str): Folder with files from batch of samples
            sample_name (str, optional): Name of the sample in metadata. Defaults to None.
            path_key (str, optional): Metadata field for the file. Defaults to None.
        Returns:
            bool: True if the process was successful, else False
        """
        dest_folder = os.path.join(dest_folder, "analysis_results")
        os.makedirs(dest_folder, exist_ok=True)
        for filepath in file:
            out_filepath = os.path.join(dest_folder, os.path.basename(filepath))
            if os.path.isfile(out_filepath):
                self.log.debug(f"{out_filepath} already exists, not extracted")
                continue
            if filepath == "Not Provided [SNOMED:434941000124101]":
                self.update_all_logs(
                    self.extract_file.__name__,
                    "warning",
                    f"File for {path_key} not provided in sample {sample_name}",
                )
                continue
            try:
                shutil.copy(filepath, out_filepath)
            except (IOError, PermissionError) as e:
                self.update_all_logs(
                    self.extract_file.__name__,
                    "warning",
                    f"Could not extract {filepath}: {e}",
                )
                continue
        return True

    def split_data_by_batch(self, j_data: list[dict]) -> dict:
        """Split metadata from json for each batch of samples found according to folder location of the samples.
        Args:
            j_data (list(dict)): List of dictionaries, one per sample, including metadata for that sample
        Returns:
            data_by_batch (dict(list(dict))): Dictionary containing parts of j_data corresponding to each
            different folder with samples (batch) included in the original json metadata used as input
        """
        unique_batchs = {x.get("sequence_file_path_R1") for x in j_data}
        data_by_batch = {batch_dir: {} for batch_dir in unique_batchs}
        for batch_dir in data_by_batch:
            data_by_batch[batch_dir]["j_data"] = [
                samp
                for samp in j_data
                if samp.get("sequence_file_path_R1") == batch_dir
            ]
        return data_by_batch

    def split_tables_by_batch(
        self,
        files_found_dict: dict,
        file_tag: str,
        batch_data: list[dict],
        output_dir: str,
    ) -> None:
        """Filter table content to output a new table containing only the samples present in given metadata
        Args:
            files_found_dict (dict): A dictionary containing file paths identified for each configuration item.
            file_tag (str): File tag to be added to the new table file name.
            batch_data (list(dict)): Metadata corresponding to a single folder with samples (folder)
            output_dir (str): Output location for the generated tabular file
        """

        def extract_batch_rows_to_file(file, new_filename):
            """Create a new table file only with rows matching samples in batch_data"""
            ext_dict = {".csv": ",", ".tsv": "\t", ".tab": "\t"}
            file_extension = os.path.splitext(file)[1]
            file_df = pd.read_csv(
                file, sep=ext_dict.get(file_extension), header=header_pos
            )
            sample_col = file_df.columns[sample_col_pos]
            file_df[sample_col] = file_df[sample_col].astype(str)
            file_df = file_df[file_df[sample_col].isin(batch_samples)]

            os.makedirs(os.path.join(output_dir, "analysis_results"), exist_ok=True)
            output_path = os.path.join(output_dir, "analysis_results", new_filename)
            sep = ext_dict.get(file_extension, ",")
            file_df.to_csv(output_path, index=False, sep=sep)
            return

        method_name = self.split_tables_by_batch.__name__
        # sample names are created by concatenating sequencing_sample_id and unique_sample_id
        # if unique_sample_id is not present, it will use only sequencing_sample_id.
        batch_samples = [
            (
                f"{row.get('sequencing_sample_id')}_{row.get('unique_sample_id')}"
                if row.get("unique_sample_id")
                else row.get("sequencing_sample_id")
            )
            for row in batch_data
        ]
        for key, files in files_found_dict.items():
            if not self.software_config[key].get("split_by_batch"):
                continue
            header_pos = self.software_config[key].get("header_row_idx", 1) - 1
            sample_col_pos = self.get_sample_idx_col_pos(key)
            for file in files:
                try:
                    if self.software_config[key].get("filepath_name"):
                        filepath_key = self.software_config[key].get("filepath_name")
                        new_filename = os.path.basename(
                            list({row[filepath_key] for row in batch_data})[0]
                        )
                    else:
                        base, ext = os.path.splitext(os.path.basename(file))
                        new_filename = f"{base}_{file_tag}{ext}"
                    extract_batch_rows_to_file(file, new_filename)
                except Exception as e:
                    log_type = (
                        "error"
                        if self.software_config[key].get("required")
                        else "warning"
                    )
                    self.update_all_logs(
                        method_name,
                        log_type,
                        f"Could not create batch table for {file}: {e}",
                    )
        self.log_report.print_log_report(method_name, ["valid", "warning", "error"])
        return

    def merge_metadata(self, batch_filepath: str, batch_data: list[dict]) -> list[dict]:
        """
        Merge metadata json if sample does not exist in the metadata file,
        or prompt the user to update if --update flag is provided and sample differs.

        Args:
            batch_filepath (str): Path to save the json file with the metadata.
            batch_data (list): A list of dictionaries containing metadata of the samples.

        Returns:
            merged_metadata (list): The updated list of metadata entries.
        """
        merged_metadata = relecov_tools.utils.read_json_file(batch_filepath)
        prev_metadata_dict = {
            item["sequencing_sample_id"]: item for item in merged_metadata
        }

        overwrite_all = False

        for item in batch_data:
            sample_id = item["sequencing_sample_id"]
            if sample_id in prev_metadata_dict:
                if prev_metadata_dict[sample_id] != item:
                    if self.update:
                        if not overwrite_all:
                            stderr.print(
                                f"[red]Sample '{sample_id}' has different metadata than the existing entry in {batch_filepath}."
                            )
                            response = Prompt.ask(
                                "[yellow]Do you want to overwrite this sample?",
                                choices=["y", "n", "all"],
                                default="n",
                            )
                            if response == "all":
                                overwrite_all = True
                                stderr.print(
                                    "[green]All subsequent samples will be overwritten automatically."
                                )
                                prev_metadata_dict[sample_id] = item
                            elif response == "y":
                                prev_metadata_dict[sample_id] = item
                                stderr.print(f"[green]Sample '{sample_id}' updated.")
                            else:
                                stderr.print(
                                    f"[blue]Skipping update for sample '{sample_id}'."
                                )
                        else:
                            prev_metadata_dict[sample_id] = item
                            stderr.print(f"[green]Sample '{sample_id}' updated (auto).")
                    else:
                        error_msg = f"Sample '{sample_id}' has different data in {batch_filepath} and new metadata. Can't merge."
                        stderr.print(f"[red]{error_msg}")
                        self.log.error(error_msg)
                        raise ValueError(error_msg)
            else:
                prev_metadata_dict[sample_id] = item

        merged_metadata = list(prev_metadata_dict.values())
        relecov_tools.utils.write_json_to_file(merged_metadata, batch_filepath)
        return merged_metadata

    def get_multiple_sample_files(self) -> list[str]:
        """
        Get a list of software configuration keys that have multiple samples.

        Returns:
            multiple_sample_files (list[str]): A list of keys from the software configuration
        """
        multiple_sample_files = []
        multiple_sample_files.extend(
            key
            for key in self.software_config.keys()
            if self.software_config[key].get("multiple_samples")
        )
        return multiple_sample_files

    def filter_properties(self, data: list[dict]) -> list[dict]:
        """
        Remove properties from bioinfo_metadata that are not in the schema properties.

        Args:
        data : list[dict]
            Dictionary with the bioinfo metadata

        Returns:
        data: list[dict]
            Bioinfo metadata json filtered
        """
        valid_keys = set(self.json_schema.get("properties", {}).keys())

        for sample in data:
            for k in list(sample.keys()):
                if k not in valid_keys:
                    sample.pop(k)
        return data

    def split_extra_json_data(
        self, extra_data: dict, batch_data: list[dict]
    ) -> tuple[list[dict], str]:
        """
        Split extra json data based on the sample names in the batch_data.

        Args:
        extra_data
            dict: List of dictionaries containing extra metadata that needs to be filtered based on the samples in
        batch_data
            list[dict]: List of dictionaries containing metadata for the current batch of samples.

        Returns:
        filtered_batch_data: list[dict]
            List of dictionaries containing extra metadata filtered based on the samples in batch_data.
        filename: str
            The file name of the first item in filtered_batch_data.
        """
        # Get the sample names from batch_data
        # If unique_sample_id is present, it will be used to create the sample name,
        # otherwise only sequencing_sample_id will be used.
        sample_names_in_batch = {
            (
                f"{sample_data['sequencing_sample_id']}_{sample_data['unique_sample_id']}"
                if sample_data["unique_sample_id"]
                else sample_data["sequencing_sample_id"]
            )
            for sample_data in batch_data
        }
        # filter extra_data based on sample names in batch_data
        filtered_batch_data = [
            sample_data
            for sample_data in extra_data
            if sample_data["sample_name"] in sample_names_in_batch
        ]

        # get the file name from the first item in filtered_batch_data
        filename = filtered_batch_data[0]["file_name"]

        return filtered_batch_data, filename

    def create_bioinfo_file(self) -> bool:
        """Create the bioinfodata json with collecting information from lab
        metadata json, mapping_stats, and more information from the files
        inside input directory.

        Returns:
            bool: True if the bioinfo file creation process was successful.
        """

        tag = "bioinfo_lab_metadata_"
        file_tag = self.batch_id + "_" + self.hex
        out_filename = tag + file_tag + ".json"
        batch_filename = self.tag_filename(out_filename)

        year = str(datetime.now().year)
        out_path = os.path.join(self.output_dir, year)
        os.makedirs(out_path, exist_ok=True)
        batch_filepath = os.path.join(out_path, out_filename)

        self.validate_sample_names()

        stderr.print("[blue]Scanning input directory...")
        self.log.info("Scanning input directory")
        files_found_dict = self.scan_directory()

        stderr.print("[blue]Validating required files...")
        self.log.info("Validating required files")
        self.validate_software_mandatory_files(files_found_dict)

        stderr.print("[blue]Adding bioinfo metadata to read lab metadata...")
        self.log.info("Adding bioinfo metadata to read lab metadata")
        self.j_data, extra_json_data = self.add_bioinfo_results_metadata(
            files_found_dict, file_tag, self.j_data, out_path
        )

        stderr.print("[blue]Adding fixed values")
        self.log.info("Adding fixed values")
        self.j_data = self.add_fixed_values(self.j_data, batch_filename)

        stderr.print("[blue]Adding files path to read lab metadata")
        self.log.info("Adding files path to read lab metadata")
        self.j_data = self.map_and_extract_bioinfo_paths(files_found_dict, self.j_data)

        stderr.print("[blue]Evaluating quality control...")
        self.log.info("Evaluating quality control")
        module = importlib.import_module(
            f"relecov_tools.assets.pipeline_utils.{self.software_name}"
        )
        try:
            if hasattr(module, "quality_control_evaluation"):
                qc_func = getattr(module, "quality_control_evaluation")
                self.j_data = qc_func(self.j_data)
        except (AttributeError, NameError, TypeError, ValueError) as e:
            self.update_all_logs(
                self.create_bioinfo_file.__name__,
                "warning",
                f"Could not evaluate quality_control_evaluation for batch {self.j_data}: {e}",
            )
            stderr.print(
                f"[orange]Could not evaluate quality_control_evaluation for batch {self.j_data}: {e}"
            )

        stderr.print("[blue]Validating bioinfo metadata...")
        self.log.info("Validating bioinfo metadata")
        if not self._validate_jdata_and_log_errors(out_path):
            return False

        stderr.print("[blue]Writing and splitting batches...")
        self.log.info("Writing and splitting batches")
        self._write_and_split_batches(
            files_found_dict=files_found_dict,
            batch_filepath=batch_filepath,
            extra_json_data=extra_json_data,
            out_path=out_path,
        )

        return True

    def _validate_jdata_and_log_errors(self, out_path: str) -> bool:
        self.j_data = self.filter_properties(self.j_data)

        valid_rows, invalid_rows = relecov_tools.validate.Validate.validate_instances(
            self.j_data, self.json_schema, "sequencing_sample_id"
        )

        for sample in valid_rows:
            self.logsum.feed_key(key=out_path, sample=sample.get("sequencing_sample_id"))

        if invalid_rows:
            unique_failed_samples = list({
                sample for samples in invalid_rows["samples"].values() for sample in samples
            })
            for error_message, failed_samples in invalid_rows["samples"].items():
                field_with_error = invalid_rows["fields"][error_message]
                sample_list = "', '".join(failed_samples)
                error_text = (
                    f"{error_message} in field '{field_with_error}' for {len(failed_samples)} sample/s: '{sample_list}'"
                )
                log_fn = self.logsum.add_error if len(unique_failed_samples) == len(self.j_data) else self.logsum.add_warning
                log_fn(key=out_path, entry=error_text)
                for fail_samp in failed_samples:
                    self.logsum.add_error(key=out_path, sample=fail_samp, entry=error_text)

            if not self.soft_validation:
                self.parent_create_error_summary(
                    called_module="read-bioinfo-metadata", logs=self.logsum.logs
                )
                self.log.warning("Metadata was not completely validate, fix the errors or run with --soft_validation")
                return False
        else:
            self.j_data = valid_rows
            self.log.info("Bioinfo json successfully validated.")

        return True

    def _write_and_split_batches(
        self,
        files_found_dict: dict,
        batch_filepath: str,
        extra_json_data: list[dict],
        out_path: str,
    ) -> None:
        self.j_data = self._write_or_merge_json(batch_filepath, self.j_data)
        self.log.info(f"Created output json file: {batch_filepath}")

        data_by_batch = self.split_data_by_batch(self.j_data)
        for batch_dir, batch_dict in data_by_batch.items():
            batch_data = batch_dict["j_data"]
            if not batch_data:
                self.log.warning(f"Data from batch {batch_dir} was completely empty. Skipped.")
                self.update_all_logs(
                    self.create_bioinfo_file.__name__,
                    "warning",
                    f"Data from batch {batch_dir} was completely empty. Skipped.",
                )
                continue

            lab_code = batch_data[0].get("submitting_institution_id", batch_dir.split("/")[-2])
            batch_date = batch_data[0].get("batch_id", batch_dir.split("/")[-1])
            file_tag = batch_date + "_" + self.hex

            self.log.info(f"Processing data from {batch_dir}")
            self.split_tables_by_batch(files_found_dict, file_tag, batch_data, batch_dir)

            batch_filename = self.tag_filename("bioinfo_lab_metadata_" + lab_code + ".json")
            batch_filepath = os.path.join(batch_dir, batch_filename)

            batch_data = self._write_or_merge_json(batch_filepath, batch_data)
            self.log.info(f"Created output json file: {batch_filepath}")

            for sample in batch_data:
                self.logsum.feed_key(key=batch_dir, sample=sample.get("sequencing_sample_id"))

            for extra_json in extra_json_data:
                filtered_batch_data, filename = self.split_extra_json_data(extra_json, batch_data)
                extra_filename = f"{filename}_{lab_code}_{file_tag}.json"
                extra_filepath = os.path.join(batch_dir, extra_filename)
                relecov_tools.utils.write_json_to_file(filtered_batch_data, extra_filepath)

        self.parent_create_error_summary(
            called_module="read-bioinfo-metadata", logs=self.logsum.logs
        )

    def _write_or_merge_json(self, filepath: str, data: list[dict]) -> list[dict]:
        """
        Writes data to a JSON file, or merges with existing data if the file already exists.

        Args:
            filepath (str): Path to the output JSON file.
            data (list[dict]): List of dictionaries to write or merge.

        Returns:
            list[dict]: The resulting data after potential merge.
        """
        if os.path.exists(filepath):
            stderr.print(f"[blue]Bioinfo metadata {filepath} file already exists. Merging new data if possible.")
            self.log.info(f"Bioinfo metadata {filepath} file already exists. Merging new data if possible.")
            return self.merge_metadata(filepath, data)

        relecov_tools.utils.write_json_to_file(data, filepath)
        return data
