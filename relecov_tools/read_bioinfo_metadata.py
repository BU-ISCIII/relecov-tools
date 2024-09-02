#!/usr/bin/env python
import os
import sys
import logging
import rich.console
import re
from bs4 import BeautifulSoup

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
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
class BioinfoMetadata(BioinfoReportLog):
    def __init__(
        self,
        readlabmeta_json_file=None,
        input_folder=None,
        output_folder=None,
        software=None,
    ):
        # Init process log
        super().__init__()
        self.log_report = BioinfoReportLog()

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
        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = output_folder

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
        total_files = sum(len(files) for _, _, files in os.walk(self.input_folder))
        files_found = {}

        for topic_key, topic_scope in self.software_config.items():
            if "fn" not in topic_scope:  # try/except fn
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f"No 'fn' (file pattern) found in '{self.software_name}.{topic_key}'.",
                )
                continue
            for root, _, files in os.walk(self.input_folder, topdown=True):
                matching_files = [
                    os.path.join(root, file_name)
                    for file_name in files
                    if re.search(topic_scope["fn"], file_name)
                ]
                if len(matching_files) >= 1:
                    files_found[topic_key] = matching_files
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
                f"Scannig process succeed. Scanned {total_files} files.",
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

    def add_bioinfo_results_metadata(self, files_dict, j_data):
        """Adds metadata from bioinformatics results to j_data.
        It first calls file_handlers and then maps the handled
        data into j_data.

        Args:
            files_dict (dict{str:str}): A dictionary containing file paths found based on the definitions provided in the bioinformatic JSON file within the software scope (self.software_config).

            j_data (list(dict{str:str}): A list of dictionaries containing metadata lab (list item per sample).

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
            data_to_map = self.handling_files(files_dict[key])
            # Mapping data to j_data
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

    def handling_files(self, file_list):
        """Handles different file formats to extract data regardless of their structure. The goal is to extract the data contained in files specified in ${file_list}, using either 'standard' handlers defined in this class or pipeline-specific file handlers.
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
            Note: ensure that 'field1','field2','field3' corresponds with the values especifies in the 'content' section of each software configuration scope (see: conf/bioinfo_config.json).

        Args:
            file_list (list): A list of file path/s to be processed.

        Returns:
            data: A dictionary containing bioinfo metadata handled for each sample.
        """
        method_name = f"{self.add_bioinfo_results_metadata.__name__}:{self.handling_files.__name__}"
        file_name = self.software_config[self.current_config_key].get("fn")
        file_extension = os.path.splitext(file_name)[1]
        # Parsing key position
        try:
            self.software_config[self.current_config_key]["sample_col_idx"]
            sample_idx_possition = (
                self.software_config[self.current_config_key]["sample_col_idx"] - 1
            )
        except KeyError:
            sample_idx_possition = None
            self.log_report.update_log_report(
                method_name,
                "warning",
                f"No sample-index-column defined in '{self.software_name}.{self.current_config_key}'. Using default instead.",
            )
        # Parsing files
        func_name = self.software_config[self.current_config_key]["function"]
        if func_name is None:
            if file_name.endswith(".csv"):
                data = relecov_tools.utils.read_csv_file_return_dict(
                    file_name=file_list[0], sep=",", key_position=sample_idx_possition
                )
                return data
            elif file_name.endswith(".tsv") or file_name.endswith(".tab"):
                data = relecov_tools.utils.read_csv_file_return_dict(
                    file_name=file_list[0], sep="\t", key_position=sample_idx_possition
                )
            else:
                self.log_report.update_log_report(
                    method_name,
                    "error",
                    f"Unrecognized defined file name extension '{file_extension}' in '{file_name}'.",
                )
                sys.exit(self.log_report.print_log_report(method_name, ["error"]))
        else:
            try:
                # Dynamically import the function from the specified module
                utils_name = f"relecov_tools.assets.pipeline_utils.{self.software_name}"
                import_statement = f"from {utils_name} import {func_name}"
                exec(import_statement)
                # Get method name and execute it.
                data = eval(func_name + "(file_list)")
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
            sample_match = re.match(r"^[^_]+", row["sequence_file_R1_fastq"])
            if sample_match:
                sample_name = sample_match.group()
            else:
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f'Regex failed to find extract sample name from: {row["sequence_file_R1_fastq"]}. Skipping...',
                )
                continue
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
            sample_match = re.match(r"^[^_]+", row["sequence_file_R1_fastq"])
            if sample_match:
                sample_name = sample_match.group()
            else:
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f'Regex failed to find extract sample name from: {row["sequence_file_R1_fastq"]}. Skipping...',
                )
                continue

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
            sample_match = re.match(r"^[^_]+", row["sequence_file_R1_fastq"])
            if sample_match:
                sample_name = sample_match.group()
            else:
                self.log_report.update_log_report(
                    method_name,
                    "warning",
                    f'Regex failed to find extract sample name from: {row["sequence_file_R1_fastq"]}. Skipping...',
                )
                continue
            for key, value in files_found_dict.items():
                file_path = "Not Provided [GENEPIO:0001668]"
                if value:  # Check if value is not empty
                    for file in value:
                        if sample_name in file:
                            file_path = file
                            break  # Exit loop if match found
                else:
                    file_path = value[0]
                path_key = f"{self.software_name}_filepath_{key}"
                row[path_key] = file_path
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

    def create_bioinfo_file(self):
        """Create the bioinfodata json with collecting information from lab
        metadata json, mapping_stats, and more information from the files
        inside input directory.

        Returns:
            bool: True if the bioinfo file creation process was successful.
        """
        # Find and validate bioinfo files
        stderr.print("[blue]Sanning input directory...")
        files_found_dict = self.scann_directory()
        stderr.print("[blue]Validating required files...")
        self.validate_software_mandatory_files(files_found_dict)
        # Add bioinfo metadata to j_data
        stderr.print("[blue]Adding bioinfo metadata to read lab metadata...")
        self.j_data = self.add_bioinfo_results_metadata(files_found_dict, self.j_data)
        stderr.print("[blue]Adding software versions to read lab metadata...")
        self.j_data = self.get_multiqc_software_versions(
            files_found_dict["workflow_summary"], self.j_data
        )
        stderr.print("[blue]Adding fixed values")
        self.j_data = self.add_fixed_values(self.j_data)
        # Adding files path
        stderr.print("[blue]Adding files path to read lab metadata")
        self.j_data = self.add_bioinfo_files_path(files_found_dict, self.j_data)
        # Generate readlab + bioinfolab processed metadata.
        file_name = (
            "bioinfo_"
            + os.path.splitext(os.path.basename(self.readlabmeta_json_file))[0]
            + ".json"
        )
        stderr.print("[blue]Writting output json file")
        os.makedirs(self.output_folder, exist_ok=True)
        file_path = os.path.join(self.output_folder, file_name)
        relecov_tools.utils.write_json_fo_file(self.j_data, file_path)
        stderr.print("[green]Sucessful creation of bioinfo analyis file")
        return True
