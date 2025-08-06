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
        self, j_data: list[dict], map_data: dict, mapping_fields: dict, table_name: str
    ) -> list[dict]:
        """Maps bioinformatics metadata from map_data to j_data based on the mapping_fields.
        Args:
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (one item per sample).
            map_data (dict(dict{str:str})): A dictionary containing bioinfo metadata handled by the method handling_files().
            mapping_fields (dict{str:str}): A dictionary of mapping fields defined in the 'content' definition under each software scope (see conf/bioinfo.config).
            table_name (str): Path to the mapping file/table.
        Returns:
            j_data: updated j_data with bioinformatic metadata mapped in it.
        """
        method_name = f"{self.mapping_over_table.__name__}:{self.software_name}.{self.current_config_key}"
        errors = []
        field_errors = {}
        field_valid = {}

        # get sample ids from j_data
        # If unique_sample_id is not present, it will use only sequencing_sample_id.
        # else it will use both to create a unique sample id.
        sample_ids = {
            row.get("sequencing_sample_id")
            for row in j_data
            if row.get("sequencing_sample_id")
        }

        # check if map_data contains sample ids
        matched_samples = sample_ids & set(map_data.keys())
        no_samples = not matched_samples

        for row in j_data:
            # TODO: We should consider an independent module that verifies that sample's name matches this pattern.
            #       If we add warnings within this module, every time mapping_over_table is invoked it will print redundant warnings
            if not row.get("sequencing_sample_id"):
                self.update_all_logs(
                    method_name,
                    "warning",
                    f'Sequencing_sample_id missing in {row.get("collecting_sample_id")}... Skipping...',
                )
                continue

            # Get sample name from row
            # If unique_sample_id is present, it will use both sequencing_sample_id and unique_sample_id to create a unique sample name.
            # else only sequencing_sample_id will be used.
            if "unique_sample_id" in row:
                sample_name = f"{row['sequencing_sample_id']}_{row['unique_sample_id']}"
            else:
                sample_name = row["sequencing_sample_id"]

            # Check if sample_name is in map_data
            if sample_name in map_data:
                # for each field in mapping_fields, try to map the data
                for field, value in mapping_fields.items():
                    try:
                        raw_val = map_data[sample_name][value]
                        raw_val = self.replace_na_value_if_needed(field, raw_val)
                        expected_type = (
                            self.json_schema["properties"]
                            .get(field, {})
                            .get("type", "string")
                        )
                        row[field] = relecov_tools.utils.cast_value_to_schema_type(
                            raw_val, expected_type
                        )
                        field_valid[sample_name] = {field: value}
                    except KeyError as e:
                        field_errors[sample_name] = {field: e}
                        row[field] = "Not Provided [SNOMED:434941000124101]"
                        continue
            elif (
                not self.software_config[self.current_config_key].get(
                    "multiple_samples"
                )
                and no_samples
            ):  # When sample ID is not in mapping_fields because is not a multiple_sample table
                for field, value_dict in mapping_fields.items():
                    for json_field, software_key in value_dict.items():
                        try:
                            raw_val = map_data[software_key][field]
                            expected_type = (
                                self.json_schema["properties"]
                                .get(json_field, {})
                                .get("type", "string")
                            )

                            row[json_field] = (
                                relecov_tools.utils.cast_value_to_schema_type(
                                    raw_val, expected_type
                                )
                            )
                            field_valid[software_key] = {json_field: field}
                        except KeyError as e:
                            field_errors[software_key] = {json_field: str(e)}
                            row[json_field] = "Not Provided [SNOMED:434941000124101]"
            else:
                errors.append(sample_name)
                for field in mapping_fields:
                    row[field] = "Not Provided [SNOMED:434941000124101]"

        # work around when map_data comes from several per-sample tables/files instead of single table
        if len(table_name) > 2:
            table_name = os.path.dirname(table_name[0])
        else:
            table_name = table_name[0]
        # Parse missing sample errors
        if errors:
            len_errs = len(errors)
            self.update_all_logs(
                method_name,
                "warning",
                f"{len_errs} samples missing in '{table_name}': {', '.join(errors)}.",
            )
        else:
            self.update_all_logs(
                method_name,
                "valid",
                f"All samples were successfully found in {table_name}.",
            )

        # Parse missing fields errors
        # TODO: this stdout can be improved
        if field_errors:
            self.update_all_logs(
                method_name,
                "warning",
                f"Missing fields in {table_name}:\n\t{field_errors}",
            )
        else:
            self.update_all_logs(
                method_name,
                "valid",
                f"Successfully mapped fields in {', '.join(field_valid.keys())}.",
            )
        # Print report
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

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
            (
                f"{sample['sequencing_sample_id']}_{sample['unique_sample_id']}"
                if sample.get("unique_sample_id")
                else sample["sequencing_sample_id"]
            )
            for sample in self.j_data
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
        """Adds metadata from bioinformatics results to j_data.
        It first calls file_handlers and then maps the handled
        data into j_data.

        Args:
            files_dict (dict{str:str}): A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (list item per sample).
            output_dir (str): Path to save output files generated during handling_files() process.
            file_tag (str): Tag that will be used for output filenames includes batch date (same as download date) and hex.

        Returns:
            j_data_mapped: A list of dictionaries with bioinformatics metadata mapped into j_data.
            extra_json_data: List of dictionaries with data that hasn't to be mapped in order to be processed afterwards.
        """
        # set method name and initialize extra_json_data
        method_name = f"{self.add_bioinfo_results_metadata.__name__}"
        extra_json_data = []
        j_data_mapped = j_data  # Ensure j_data_mapped is always defined

        # For each key in the software configuration, process the files
        for key in self.software_config:
            # Reset map_data flag so it only activates when table expects mapping
            map_data_flag = False
            # Update bioinfo configuration key/scope
            self.current_config_key = key
            map_method_name = f"{method_name}:{self.software_name}.{key}"
            # This skip files that will be parsed with other methods
            if key in ("workflow_summary", "fixed_values"):
                continue
            try:
                files_dict[key]
                stderr.print(f"[blue]Start processing {self.software_name}.{key}")
                self.log.info(f"Start processing {self.software_name}.{key}")
            except KeyError:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"No file path found for '{self.software_name}.{key}'",
                )
                continue

            current_config = self.software_config[self.current_config_key]
            file_name = current_config.get("fn", None)
            func_name = current_config.get("function", None)

            if func_name is None:
                data = self.handling_tables(files_dict[key], file_name)
            else:
                data = self.process_metadata(
                    files_dict[key],
                    file_tag=file_tag,
                    func_name=func_name,
                    out_path=output_dir,
                )

            if current_config.get("split_by_batch") and current_config.get(
                "extra_dict"
            ):
                extra_json_data.append(data)
                data_to_map = None
                map_data_flag = False
            else:
                data_to_map = data
                map_data_flag = True

            # Mapping data to j_data
            mapping_fields = self.software_config[key].get("content")
            if not mapping_fields:
                self.update_all_logs(
                    map_method_name,
                    "warning",
                    f"No metadata found to perform mapping from '{self.software_name}.{key}' despite 'content' fields being defined.",
                )
                self.log_report.print_log_report(map_method_name, ["warning"])
                continue

            if data_to_map and map_data_flag:
                j_data_mapped = self.mapping_over_table(
                    j_data=j_data,
                    map_data=data_to_map,
                    mapping_fields=self.software_config[key]["content"],
                    table_name=files_dict[key],
                )
            else:
                self.update_all_logs(
                    method_name,
                    "warning",
                    f"No metadata found to perform standard mapping when processing '{self.software_name}.{key}'",
                )
                continue

        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data_mapped, extra_json_data

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

    def add_bioinfo_files_path(
        self, files_found_dict: dict, j_data: list[dict]
    ) -> list[dict]:
        """Adds file paths essential for handling and mapping bioinformatics metadata to the j_data.
        For each sample in j_data, the function assigns the corresponding file path based on the identified files in files_found_dict.

        If multiple files are identified per configuration item (e.g., viralrecon.mapping_consensus → *.consensus.fa), each sample in j_data receives its respective file path.
        If no file path is located, the function appends "Not Provided [SNOMED:434941000124101]" to indicate missing data.

        Args:
            files_found_dict (dict): A dictionary containing file paths identified for each configuration item.
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (one item per sample).

        Returns:
            j_data: Updated j_data with file paths mapped for bioinformatic metadata.
        """
        method_name = f"{self.add_bioinfo_files_path.__name__}"
        sample_name_error = 0
        multiple_sample_files = self.get_multiple_sample_files()
        for row in j_data:
            if not row.get("sequencing_sample_id"):
                self.update_all_logs(
                    method_name,
                    "warning",
                    f'Sequencing_sample_id missing in {row.get("collecting_sample_id")}... Skipping...',
                )
                continue
            sample_name = row["sequencing_sample_id"]
            base_cod_path = row.get("sequence_file_path_R1")
            if base_cod_path is None:
                self.update_all_logs(
                    method_name,
                    "error",
                    f"No 'sequence_file_path_R1' found for sample {sample_name}. Unable to generate paths.",
                )
                continue
            for key, values in files_found_dict.items():
                file_path = []
                if values:  # Check if value is not empty
                    for file in values:
                        if key in multiple_sample_files or sample_name in file:
                            file_path.append(file)
                else:
                    file_path.append("Not Provided [SNOMED:434941000124101]")
                if self.software_config[key].get("filepath_name"):
                    path_key = self.software_config[key].get("filepath_name")
                    analysis_results_paths = []
                    for paths in file_path:
                        if file_path != "Not Provided [SNOMED:434941000124101]" and (
                            self.software_config[key].get("extract")
                            or self.software_config[key].get("function")
                        ):
                            if self.software_config[key].get("split_by_batch"):
                                base, ext = os.path.splitext(os.path.basename(paths))
                                batch_id = row["batch_id"]
                                new_fname = f"{base}_{batch_id}_{self.hex}{ext}"
                                analysis_results_path = os.path.join(
                                    base_cod_path,
                                    "analysis_results",
                                    new_fname,
                                )
                            else:
                                analysis_results_path = os.path.join(
                                    base_cod_path,
                                    "analysis_results",
                                    os.path.basename(paths),
                                )
                            analysis_results_paths.append(analysis_results_path)
                        else:
                            analysis_results_paths = file_path
                    row[path_key] = ", ".join(analysis_results_paths)
                else:
                    path_key = key

                if self.software_config[key].get("extract"):
                    self.extract_file(
                        file=file_path,
                        dest_folder=row.get("sequence_file_path_R1", ""),
                        sample_name=sample_name,
                        path_key=path_key,
                    )

        self.log_report.print_log_report(method_name, ["warning"])

        if sample_name_error == 0:
            self.update_all_logs(method_name, "valid", "File paths added successfully.")
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

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

        # Check samplesheet for matching samples
        self.validate_sample_names()

        # Find and validate bioinfo files
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
        # Adding files path
        stderr.print("[blue]Adding files path to read lab metadata")
        self.log.info("Adding files path to read lab metadata")
        self.j_data = self.add_bioinfo_files_path(files_found_dict, self.j_data)

        # Dynamically import the function from the specified module
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

        # Filter properties from batch_data that are not included in the schema
        self.j_data = self.filter_properties(self.j_data)
        valid_rows, invalid_rows = relecov_tools.validate.Validate.validate_instances(
            self.j_data, self.json_schema, "sequencing_sample_id"
        )
        valid_samples = [sample.get("sequencing_sample_id") for sample in valid_rows]
        for sample in valid_samples:
            self.logsum.feed_key(key=out_path, sample=sample)
        if invalid_rows:
            unique_failed_samples = list(
                {
                    sample
                    for samples in invalid_rows["samples"].values()
                    for sample in samples
                }
            )
            for error_message, failed_samples in invalid_rows["samples"].items():
                num_samples = len(failed_samples)
                field_with_error = invalid_rows["fields"][error_message]
                sample_list = "', '".join(failed_samples)
                error_text = f"{error_message} in field '{field_with_error}' for {num_samples} sample/s: '{sample_list}'"
                if len(unique_failed_samples) == len(self.j_data):
                    self.logsum.add_error(key=out_path, entry=error_text)
                else:
                    self.logsum.add_warning(key=out_path, entry=error_text)
                self.log.info(error_text)
                stderr.print(f"[red]{error_text}")

                for fail_samp in failed_samples:
                    self.logsum.add_error(
                        key=out_path, sample=fail_samp, entry=error_text
                    )

            if not self.soft_validation:
                self.parent_create_error_summary(
                    called_module="read-bioinfo-metadata", logs=self.logsum.logs
                )
                error_msg = "Metadata was not completely validate, fix the errors or run with --soft_validation"
                self.log.warning(error_msg)
                stderr.print(f"[red]{error_msg}")
                return False

        else:
            stderr.print("[green]Bioinfo json successfully validated.")
            self.log.info("Bioinfo json successfully validated.")

        self.j_data = valid_rows

        for sample in self.j_data:
            self.logsum.feed_key(
                key=out_path, sample=sample.get("sequencing_sample_id")
            )

        # Split files found based on each batch of samples
        data_by_batch = self.split_data_by_batch(self.j_data)

        if os.path.exists(batch_filepath):
            stderr.print(
                f"[blue]Bioinfo metadata {batch_filepath} file already exists. Merging new data if possible."
            )
            self.log.info(
                f"Bioinfo metadata {batch_filepath} file already exists. Merging new data if possible."
            )
            self.j_data = self.merge_metadata(batch_filepath, self.j_data)
        else:
            relecov_tools.utils.write_json_to_file(self.j_data, batch_filepath)

        self.log.info(f"Created output json file: {batch_filepath}")
        stderr.print(f"[green]Created batch json file: {batch_filepath}")

        self.log.info("Splitting data by batch")
        stderr.print("[blue]Splitting data by batch")

        # Add bioinfo metadata to j_data
        for batch_dir, batch_dict in data_by_batch.items():
            batch_data = batch_dict["j_data"]
            if not batch_data:
                self.log.warning(
                    f"Data from batch {batch_dir} was completely empty. Skipped."
                )
                self.update_all_logs(
                    self.create_bioinfo_file.__name__,
                    "warning",
                    f"Data from batch {batch_dir} was completely empty. Skipped.",
                )
                continue
            first_sample = batch_data[0]
            lab_code = first_sample.get(
                "submitting_institution_id", batch_dir.split("/")[-2]
            )
            batch_date = first_sample.get("batch_id", batch_dir.split("/")[-1])
            file_tag = batch_date + "_" + self.hex
            stderr.print(f"[blue]Processing data from {batch_dir}")
            self.log.info(f"Processing data from {batch_dir}")

            self.split_tables_by_batch(
                files_found_dict, file_tag, batch_data, batch_dir
            )

            tag = "bioinfo_lab_metadata_"
            batch_filename = tag + lab_code + ".json"
            batch_filename = self.tag_filename(batch_filename)
            batch_filepath = os.path.join(batch_dir, batch_filename)

            if os.path.exists(batch_filepath):
                stderr.print(
                    f"[blue]Bioinfo metadata {batch_filepath} file already exists. Merging new data if possible."
                )
                self.log.info(
                    f"Bioinfo metadata {batch_filepath} file already exists. Merging new data if possible."
                )
                batch_data = self.merge_metadata(batch_filepath, batch_data)
            else:
                relecov_tools.utils.write_json_to_file(batch_data, batch_filepath)
            for sample in batch_data:
                self.logsum.feed_key(
                    key=batch_dir, sample=sample.get("sequencing_sample_id")
                )
            self.log.info(f"Created output json file: {batch_filepath}")
            stderr.print(f"[green]Created batch json file: {batch_filepath}")

            for extra_json in extra_json_data:
                filtered_batch_data, filename = self.split_extra_json_data(
                    extra_json, batch_data
                )

                extra_filename = filename + "_" + lab_code + "_" + file_tag + ".json"
                extra_filepath = os.path.join(batch_dir, extra_filename)

                relecov_tools.utils.write_json_to_file(
                    filtered_batch_data, extra_filepath
                )

        self.parent_create_error_summary(
            called_module="read-bioinfo-metadata", logs=self.logsum.logs
        )
        return True
