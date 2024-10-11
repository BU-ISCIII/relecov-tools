#!/usr/bin/env python
import os
import sys
import logging
import rich.console
import re
import shutil
from bs4 import BeautifulSoup

import pandas as pd
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.log_summary import LogSum

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class BioinfoReportLog:
    def __init__(self, log_report=None, output_folder="/tmp/"):
        if not log_report:
            self.report = {"error": {}, "valid": {}, "warning": {}}
        else:
            self.report = log_report
        self.logsum = LogSum(output_location=output_folder)

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
            self.logsum.add_error(key=method_name, entry=message)
            return self.report
        elif status == "warning":
            self.report["warning"].setdefault(method_name, []).append(message)
            self.logsum.add_warning(key=method_name, entry=message)
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
class BioinfoMetadata:
    def __init__(
        self,
        readlabmeta_json_file=None,
        input_folder=None,
        output_folder=None,
        software=None,
    ):
        # Init process log
        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = os.path.realpath(output_folder)
        self.log_report = BioinfoReportLog(output_folder=output_folder)

        # Parse read-lab-meta-data
        if readlabmeta_json_file is None:
            readlabmeta_json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file that was created by the read-lab-metadata"
            )
        if not os.path.isfile(readlabmeta_json_file):
            self.log_report.update_log_report(
                self.__init__.__name__,
                "error",
                f"file {readlabmeta_json_file} does not exist",
            )
            sys.exit(
                self.log_report.print_log_report(self.__init__.__name__, ["error"])
            )
        self.readlabmeta_json_file = readlabmeta_json_file
        meta_basename = os.path.basename(self.readlabmeta_json_file).split(".")[0]
        self.out_filename = "bioinfo_" + meta_basename + ".json"
        # Initialize j_data object
        stderr.print("[blue]Reading lab metadata json")
        self.j_data = self.collect_info_from_lab_json()

        # Parse input/output folder
        if input_folder is None:
            self.input_folder = relecov_tools.utils.prompt_path(
                msg="Select the input folder"
            )
        else:
            self.input_folder = input_folder

        # Parse bioinfo configuration
        self.bioinfo_json_file = os.path.join(
            os.path.dirname(__file__), "conf", "bioinfo_config.json"
        )
        if software is None:
            software = relecov_tools.utils.prompt_path(
                msg="Select the software, pipeline or tool use in the bioinformatic analysis: "
            )
        self.software_name = software
        available_software = self.get_available_software(self.bioinfo_json_file)
        bioinfo_config = ConfigJson(self.bioinfo_json_file)
        if self.software_name in available_software:
            self.software_config = bioinfo_config.get_configuration(self.software_name)
        else:
            self.log_report.update_log_report(
                self.__init__.__name__,
                "error",
                f"No configuration available for '{self.software_name}'. Currently, the only available software options are:: {', '.join(available_software)}",
            )
            sys.exit(
                self.log_report.print_log_report(self.__init__.__name__, ["error"])
            )

    def get_available_software(self, json):
        """Get list of available software in configuration

        Args:
            json (str): Path to bioinfo configuration json file.

        Returns:
            available_software: List containing available software defined in json.
        """
        config = relecov_tools.utils.read_json_file(json)
        available_software = list(config.keys())
        return available_software

    def scann_directory(self):
        """Scanns bioinfo analysis directory and identifies files according to the file name patterns defined in the software configuration json.

        Returns:
            files_found: A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).
        """
        method_name = f"{self.scann_directory.__name__}"
        total_files = 0
        files_found = {}
        all_scanned_things = []
        for root, dirs, files in os.walk(self.input_folder, topdown=True):
            # Skipping nextflow's work junk directory
            dirs[:] = [d for d in dirs if "work" not in root.split("/")]
            total_files = total_files + len(files)
            all_scanned_things.append((root, files))
        for topic_key, topic_scope in self.software_config.items():
            if "fn" not in topic_scope:  # try/except fn
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f"No 'fn' (file pattern) found in '{self.software_name}.{topic_key}'.",
                )
                continue
            for tup in all_scanned_things:
                matching_files = [
                    os.path.join(tup[0], file_name)
                    for file_name in tup[1]
                    if re.search(topic_scope["fn"], file_name)
                ]
                if len(matching_files) >= 1:
                    files_found[topic_key] = matching_files
                    break
        if len(files_found) < 1:
            self.log_report.update_log_report(
                method_name,
                "error",
                f"No files found in '{self.input_folder}' according to '{os.path.basename(self.bioinfo_json_file)}' file name patterns.",
            )
            sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        else:
            self.log_report.update_log_report(
                self.scann_directory.__name__,
                "valid",
                f"Scanning process succeed. Scanned {total_files} files.",
            )
            self.log_report.print_log_report(method_name, ["valid", "warning"])
            return files_found

    def validate_software_mandatory_files(self, files_dict):
        """Validates the presence of all mandatory files as defined in the software configuration JSON.

        Args:
            files_dict (dict{str:str}): A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).
        """
        method_name = f"{self.validate_software_mandatory_files.__name__}"
        missing_required = []
        for key in self.software_config:
            if self.software_config[key].get("required") is True:
                try:
                    files_dict[key]
                except KeyError:
                    missing_required.append(key)
                    continue
            else:
                continue
        if len(missing_required) >= 1:
            self.log_report.update_log_report(
                method_name,
                "error",
                f"Missing mandatory files in {self.software_name}.{key}:{', '.join(missing_required)}",
            )
            sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        else:
            self.log_report.update_log_report(
                method_name, "valid", "Successfull validation of mandatory files."
            )
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return

    def add_bioinfo_results_metadata(self, files_dict, j_data, output_folder=None):
        """Adds metadata from bioinformatics results to j_data.
        It first calls file_handlers and then maps the handled
        data into j_data.

        Args:
            files_dict (dict{str:str}): A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (list item per sample).
            output_folder (str): Path to save output files generated during handling_files() process.

        Returns:
            j_data_mapped: A list of dictionaries with bioinformatics metadata mapped into j_data.
        """
        method_name = f"{self.add_bioinfo_results_metadata.__name__}"
        for key in self.software_config.keys():
            # Update bioinfo cofiguration key/scope
            self.current_config_key = key
            # This skip files that will be parsed with other methods
            if key == "workflow_summary" or key == "fixed_values":
                continue
            try:
                files_dict[key]
                stderr.print(f"[blue]Start processing {self.software_name}.{key}")
            except KeyError:
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f"No file path found for '{self.software_name}.{key}'",
                )
                continue
            # Handling files
            data_to_map = self.handling_files(files_dict[key], output_folder)
            # Mapping data to j_data
            mapping_fields = self.software_config[key].get("content")
            if not mapping_fields:
                continue
            if data_to_map:
                j_data_mapped = self.mapping_over_table(
                    j_data=j_data,
                    map_data=data_to_map,
                    mapping_fields=self.software_config[key]["content"],
                    table_name=files_dict[key],
                )
            else:
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f"No metadata found to perform standard mapping when processing '{self.software_name}.{key}'",
                )
                continue
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data_mapped

    def handling_tables(self, file_list, conf_tab_name):
        """Reads a tabular file in different formats and returns a dictionary containing
        the corresponding data for each sample.

        Args:
            file_list (list): A list of file path/s to be processed.
            conf_tab_name (str): Name of the table in the defined config file.

        Returns:
            data (dict): A dictionary containing metadata as defined in handling_files.
        """
        method_name = f"{self.add_bioinfo_results_metadata.__name__}:{self.handling_tables.__name__}"
        file_ext = os.path.splitext(conf_tab_name)[1]
        # Parsing key position
        sample_idx_colpos = self.get_sample_idx_colpos(self.current_config_key)
        extdict = {".csv": ",", ".tsv": "\t", ".tab": "\t"}
        if file_ext in extdict.keys():
            data = relecov_tools.utils.read_csv_file_return_dict(
                file_name=file_list[0],
                sep=extdict.get(file_ext),
                key_position=sample_idx_colpos,
            )
            return data
        elif conf_tab_name.endswith(".gz"):
            self.log_report.update_log_report(
                method_name,
                "warning",
                f".gz files are not supported yet for data extraction: {conf_tab_name}",
            )
            data = {}
        else:
            self.log_report.update_log_report(
                method_name,
                "error",
                f"Unrecognized defined file name extension '{file_ext}' in '{conf_tab_name}'.",
            )
            sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        return data

    def handling_files(self, file_list, output_folder):
        """Handles different file formats to extract data regardless of their structure.
        The goal is to extract the data contained in files specified in ${file_list},
        using either 'standard' handlers defined in this class or pipeline-specific file handlers.
        (inspired from ./metadata_homogenizer.py)

            A file handler method must generate a data structure as follow:
            {
                'SAMPLE1': {
                    'field1': 'value1'
                    'field2': 'value2'
                    'field3': 'value3'
                },
                SAMPLE2': {
                    'field1': 'value1'
                    'field2': 'value2'
                    'field3': 'value3'
                },
                ...
            }
            Note: ensure that 'field1','field2','field3' corresponds with the values
            especified in the 'content' section of each software configuration scope
            (see: conf/bioinfo_config.json).

        Args:
            file_list (list): A list of file path/s to be processed.
            output_folder (str): Path to save output files from imported method if necessary

        Returns:
            data: A dictionary containing bioinfo metadata handled for each sample.
        """
        method_name = f"{self.add_bioinfo_results_metadata.__name__}:{self.handling_files.__name__}"
        file_name = self.software_config[self.current_config_key].get("fn")
        # Parsing files
        func_name = self.software_config[self.current_config_key]["function"]
        if func_name is None:
            data = self.handling_tables(file_list=file_list, conf_tab_name=file_name)
        else:
            try:
                # Dynamically import the function from the specified module
                utils_name = f"relecov_tools.assets.pipeline_utils.{self.software_name}"
                import_statement = f"import {utils_name}"
                exec(import_statement)
                # Get method name and execute it.
                data = eval(utils_name + "." + func_name + "(file_list, output_folder)")
            except Exception as e:
                self.log_report.update_log_report(
                    self.add_bioinfo_results_metadata.__name__,
                    "error",
                    f"Error occurred while parsing '{func_name}': {e}.",
                )
                sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        return data

    def mapping_over_table(self, j_data, map_data, mapping_fields, table_name):
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
        for row in j_data:
            # TODO: We should consider an independent module that verifies that sample's name matches this pattern.
            #       If we add warnings within this module, every time mapping_over_table is invoked it will print redundant warings
            if not row.get("sequencing_sample_id"):
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f'Sequencing_sample_id missing in {row.get("collecting_sample_id")}... Skipping...',
                )
                continue
            sample_name = row["sequencing_sample_id"]
            if sample_name in map_data.keys():
                for field, value in mapping_fields.items():
                    try:
                        # FIXME: we have to allow more than one data type to make json validation module work.
                        row[field] = str(map_data[sample_name][value])
                        field_valid[sample_name] = {field: value}
                    except KeyError as e:
                        field_errors[sample_name] = {field: e}
                        row[field] = "Not Provided [GENEPIO:0001668]"
                        continue
            else:
                errors.append(sample_name)
                for field in mapping_fields.keys():
                    row[field] = "Not Provided [GENEPIO:0001668]"
        # work around when map_data comes from several per-sample tables/files instead of single table
        if len(table_name) > 2:
            table_name = os.path.dirname(table_name[0])
        else:
            table_name = table_name[0]
        # Parse missing sample errors
        if errors:
            lenerrs = len(errors)
            self.log_report.update_log_report(
                method_name,
                "warning",
                f"{lenerrs} samples missing in '{table_name}': {', '.join(errors)}.",
            )
        else:
            self.log_report.update_log_report(
                method_name,
                "valid",
                f"All samples were successfully found in {table_name}.",
            )
        # Parse missing fields errors
        # TODO: this stdout can be improved
        if len(field_errors) > 0:
            self.log_report.update_log_report(
                method_name,
                "warning",
                f"Missing fields in {table_name}:\n\t{field_errors}",
            )
        else:
            self.log_report.update_log_report(
                method_name,
                "valid",
                f"Successfully mapped fields in {', '.join(field_valid.keys())}.",
            )
        # Print report
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

    def get_multiqc_software_versions(self, file_list, j_data):
        """Reads multiqc html file, finds table containing software version info, and map it to j_data

        Args:
            file_list (list): A list containing the path to file multiqc_report.html.
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (one item per sample).

        Returns:
            j_data: updated j_data with software details mapped in it.
        """
        method_name = f"{self.get_multiqc_software_versions.__name__}"
        # Handle multiqc_report.html
        f_path = file_list[0]
        program_versions = {}

        with open(f_path, "r") as html_file:
            html_content = html_file.read()
        soup = BeautifulSoup(html_content, features="lxml")
        div_id = "mqc-module-section-software_versions"
        versions_div = soup.find("div", id=div_id)
        if versions_div:
            table = versions_div.find("table", class_="table")
            if table:
                rows = table.find_all("tr")
                for row in rows[1:]:  # skipping header
                    columns = row.find_all("td")
                    if len(columns) == 3:
                        program_name = columns[1].text.strip()
                        version = columns[2].text.strip()
                        program_versions[program_name] = version
                    else:
                        self.log_report.update_log_report(
                            method_name,
                            "error",
                            f"HTML entry error in {columns}. HTML table expected format should be \n<th> Process Name\n</th>\n<th> Software </th>\n.",
                        )
                        sys.exit(
                            self.log_report.print_log_report(method_name, ["error"])
                        )
            else:
                self.log_report.update_log_report(
                    method_name,
                    "error",
                    f"Unable to locate the table containing software versions in file {f_path} under div section {div_id}.",
                )
                sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        else:
            self.log_report.update_log_report(
                self.get_multiqc_software_versions.__name__,
                "error",
                f"Failed to locate the required '{div_id}' div section in the '{f_path}' file.",
            )
            sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        # Mapping multiqc sofware versions to j_data
        field_errors = {}
        for row in j_data:
            # Get sample name to track whether version assignment was successful or not.
            if not row.get("sequencing_sample_id"):
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f'Sequencing_sample_id missing in {row.get("collecting_sample_id")}... Skipping...',
                )
                continue
            sample_name = row["sequencing_sample_id"]
            # Append software version and name
            software_content_details = self.software_config["workflow_summary"].get(
                "content"
            )
            for content_key, content_value in software_content_details.items():
                for key, value in content_value.items():
                    # Add software versions
                    if "software_version" in content_key:
                        try:
                            row[key] = program_versions[value]
                        except KeyError as e:
                            field_errors[sample_name] = {value: e}
                            row[key] = "Not Provided [GENEPIO:0001668]"
                        continue
                    # Add software name
                    elif "software_name" in content_key:
                        try:
                            row[key] = value
                        except KeyError as e:
                            field_errors[sample_name] = {value: e}
                            row[key] = "Not Provided [GENEPIO:0001668]"
                        continue

        # update progress log
        if len(field_errors) > 0:
            self.log_report.update_log_report(
                method_name,
                "warning",
                f"Encountered field errors while mapping data: {field_errors}",
            )
        else:
            self.log_report.update_log_report(
                method_name, "valid", "Successfully field mapped data."
            )
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

    def add_fixed_values(self, j_data):
        """Add fixed values to j_data as defined in the bioinformatics configuration (definition: "fixed values")

        Args:
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (one item per sample).

        Returns:
            j_data: updated j_data with fixxed values added in it.
        """
        method_name = f"{self.add_fixed_values.__name__}"
        try:
            f_values = self.software_config["fixed_values"]
            for row in j_data:
                row["bioinfo_metadata_file"] = self.out_filename
                for field, value in f_values.items():
                    row[field] = value
            self.log_report.update_log_report(
                method_name, "valid", "Fields added successfully."
            )
        except KeyError as e:
            self.log_report.update_log_report(
                method_name, "warning", f"Error found while adding fixed values: {e}"
            )
            pass
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

    def add_bioinfo_files_path(self, files_found_dict, j_data):
        """Adds file paths essential for handling and mapping bioinformatics metadata to the j_data.
        For each sample in j_data, the function assigns the corresponding file path based on the identified files in files_found_dict.

        If multiple files are identified per configuration item (e.g., viralrecon.mapping_consensus → *.consensus.fa), each sample in j_data receives its respective file path.
        If no file path is located, the function appends "Not Provided [GENEPIO:0001668]" to indicate missing data.

        Args:
            files_found_dict (dict): A dictionary containing file paths identified for each configuration item.
            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (one item per sample).

        Returns:
            j_data: Updated j_data with file paths mapped for bioinformatic metadata.
        """
        method_name = f"{self.add_bioinfo_files_path.__name__}"
        sample_name_error = 0
        for row in j_data:
            row["bioinfo_metadata_file"] = self.out_filename
            if not row.get("sequencing_sample_id"):
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f'Sequencing_sample_id missing in {row.get("collecting_sample_id")}... Skipping...',
                )
                continue
            sample_name = row["sequencing_sample_id"]
            for key, values in files_found_dict.items():
                file_path = "Not Provided [GENEPIO:0001668]"
                if values:  # Check if value is not empty
                    for file in values:
                        if sample_name in file:
                            file_path = file
                            break  # Exit loop if match found
                path_key = f"{self.software_name}_filepath_{key}"
                row[path_key] = file_path
                if self.software_config[key].get("extract"):
                    self.extract_file(
                        file=file_path,
                        dest_folder=row.get("r1_fastq_filepath"),
                        sample_name=sample_name,
                        path_key=path_key,
                    )
        self.log_report.print_log_report(method_name, ["warning"])
        if sample_name_error == 0:
            self.log_report.update_log_report(
                method_name, "valid", "File paths added successfully."
            )
        self.log_report.print_log_report(method_name, ["valid", "warning"])
        return j_data

    def collect_info_from_lab_json(self):
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
        except ValueError:
            self.log_report.update_log_report(
                self.collect_info_from_lab_json.__name__,
                "error",
                f"Invalid lab-metadata json file: self.{self.readlabmeta_json_file}",
            )
            sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        return json_lab_data

    def get_sample_idx_colpos(self, config_key):
        """Extract which column contain sample names for that specific file from config

        Returns:
            sample_idx_possition: column number which contains sample names
        """
        try:
            sample_idx_colpos = self.software_config[config_key]["sample_col_idx"] - 1
        except (KeyError, TypeError):
            sample_idx_colpos = 0
            self.log_report.update_log_report(
                "get_sample_idx_colpos",
                "warning",
                f"No valid sample-index-column defined in '{self.software_name}.{config_key}'. Using default instead.",
            )
        return sample_idx_colpos

    def extract_file(self, file, dest_folder, sample_name=None, path_key=None):
        """Copy input file to the given destination, include sample name and key in log

        Args:
            file (str): Path the file that is going to be copied
            dest_folder (str): Folder with files from batch of samples
            sample_name (str, optional): Name of the sample in metadata. Defaults to None.
            path_key (str, optional): Metadata field for the file. Defaults to None.

        Returns:
            bool: True if the process was successful, else False
        """
        dest_folder = os.path.join(dest_folder, "analysis_results")
        os.makedirs(dest_folder, exist_ok=True)
        out_filepath = os.path.join(dest_folder, os.path.basename(file))
        if os.path.isfile(out_filepath):
            return True
        if file == "Not Provided [GENEPIO:0001668]":
            self.log_report.update_log_report(
                self.extract_file.__name__,
                "warning",
                f"File for {path_key} not provided in sample {sample_name}",
            )
            return False
        try:
            shutil.copy(file, out_filepath)
        except (IOError, PermissionError) as e:
            self.log_report.update_log_report(
                self.extract_file.__name__, "warning", f"Could not extract {file}: {e}"
            )
            return False
        return True

    def split_data_by_batch(self, j_data):
        """Split metadata from json for each batch of samples found according to folder location of the samples.

        Args:
            files_found_dict (dict): A dictionary containing file paths identified for each configuration item.
            j_data (list(dict)): List of dictionaries, one per sample, including metadata for that sample

        Returns:
            data_by_batch (dict(list(dict))): Dictionary containing parts of j_data corresponding to each
            different folder with samples (batch) included in the original json metadata used as input
        """
        unique_batchs = set([x.get("r1_fastq_filepath") for x in j_data])
        data_by_batch = {batch_dir: {} for batch_dir in unique_batchs}
        for batch_dir in data_by_batch.keys():
            data_by_batch[batch_dir]["j_data"] = [
                samp for samp in j_data if samp.get("r1_fastq_filepath") == batch_dir
            ]
        return data_by_batch

    def split_tables_by_batch(self, files_found_dict, batch_data, output_dir):
        """Filter table content to output a new table containing only the samples present in given metadata

        Args:
            files_found_dict (dict): A dictionary containing file paths identified for each configuration item.
            batch_data (list(dict)): Metadata corresponding to a single folder with samples (folder)
            output_dir (str): Output location for the generated tabular file
        """

        def extract_batch_rows_to_file(file):
            """Create a new table file only with rows matching samples in batch_data"""
            extdict = {".csv": ",", ".tsv": "\t", ".tab": "\t"}
            file_extension = os.path.splitext(file)[1]
            file_df = pd.read_csv(
                file, sep=extdict.get(file_extension), header=header_pos
            )
            sample_col = file_df.columns[sample_colpos]
            file_df[sample_col] = file_df[sample_col].astype(str)
            file_df = file_df[file_df[sample_col].isin(batch_samples)]
            output_path = os.path.join(
                output_dir, "analysis_results", os.path.basename(file)
            )
            file_df.to_csv(output_path, index=False, sep=extdict.get(file_extension))
            return

        method_name = self.split_tables_by_batch.__name__
        namekey = "sequencing_sample_id"
        batch_samples = [row.get(namekey) for row in batch_data]
        for key, files in files_found_dict.items():
            if not self.software_config[key].get("split_by_batch"):
                continue
            header_pos = self.software_config[key].get("header_row_idx", 1) - 1
            sample_colpos = self.get_sample_idx_colpos(key)
            for file in files:
                try:
                    extract_batch_rows_to_file(file)
                except Exception as e:
                    if self.software_config[key].get("required"):
                        log_type = "error"
                    else:
                        log_type = "warning"
                    self.log_report.update_log_report(
                        method_name,
                        log_type,
                        f"Could not create batch table for {file}: {e}",
                    )
        self.log_report.print_log_report(method_name, ["valid", "warning", "error"])
        return

    def create_bioinfo_file(self):
        """Create the bioinfodata json with collecting information from lab
        metadata json, mapping_stats, and more information from the files
        inside input directory.

        Returns:
            bool: True if the bioinfo file creation process was successful.
        """
        # Find and validate bioinfo files
        stderr.print("[blue]Scanning input directory...")
        files_found_dict = self.scann_directory()
        stderr.print("[blue]Validating required files...")
        self.validate_software_mandatory_files(files_found_dict)
        # Split files found based on each batch of samples
        data_by_batch = self.split_data_by_batch(self.j_data)
        # Add bioinfo metadata to j_data
        for batch_dir, batch_dict in data_by_batch.items():
            self.log_report.logsum.feed_key(batch_dir)
            stderr.print(f"[blue]Processing data from {batch_dir}")
            batch_data = batch_dict["j_data"]
            stderr.print("[blue]Adding bioinfo metadata to read lab metadata...")
            batch_data = self.add_bioinfo_results_metadata(
                files_found_dict, batch_data, batch_dir
            )
            stderr.print("[blue]Adding software versions to read lab metadata...")
            batch_data = self.get_multiqc_software_versions(
                files_found_dict["workflow_summary"], batch_data
            )
            stderr.print("[blue]Adding fixed values")
            batch_data = self.add_fixed_values(batch_data)
            # Adding files path
            stderr.print("[blue]Adding files path to read lab metadata")
            batch_data = self.add_bioinfo_files_path(files_found_dict, batch_data)
            self.split_tables_by_batch(files_found_dict, batch_data, batch_dir)
            lab_code = batch_dir.split("/")[-2]
            batch_date = batch_dir.split("/")[-1]
            tag = "bioinfo_lab_metadata_"
            batch_filename = tag + lab_code + "_" + batch_date + ".json"
            batch_filepath = os.path.join(batch_dir, batch_filename)
            relecov_tools.utils.write_json_fo_file(batch_data, batch_filepath)
            for sample in batch_data:
                self.log_report.logsum.feed_key(
                    key=batch_dir, sample=sample.get("sequencing_sample_id")
                )
            log.info("Created output json file: %s" % batch_filepath)
            stderr.print(f"[green]Created batch json file: {batch_filepath}")
        stderr.print("[blue]Writting output json file")
        os.makedirs(self.output_folder, exist_ok=True)
        file_path = os.path.join(self.output_folder, self.out_filename)
        relecov_tools.utils.write_json_fo_file(self.j_data, file_path)
        stderr.print(f"[green]Sucessful creation of bioinfo analyis file: {file_path}")
        self.log_report.logsum.create_error_summary(
            called_module="read-bioinfo-metadata", logs=self.log_report.logsum.logs
        )
        return True
