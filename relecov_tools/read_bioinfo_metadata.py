#!/usr/bin/env python
import os
import sys
import logging
import glob
import rich.console
from datetime import datetime
from yaml import YAMLError
from bs4 import BeautifulSoup

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.long_table_parse import LongTableParse

# import relecov_tools.json_schema

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

# TODO: Create 2 master function that validates file presence/content and transfrom from csv,tsv,... to json.
# TODO: Cosider eval py + func property in json to be able to discriminate between collated files and sample-specific files.  
class BioinfoMetadata:
    def __init__(
        self,
        readlabmeta_json_file=None,
        input_folder=None,
        output_folder=None,
        software='viralrecon',
    ):
        if readlabmeta_json_file is None:
            readlabmeta_json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file that was created by the read-lab-metadata"
            )
        if not os.path.isfile(readlabmeta_json_file):
            log.error("json file %s does not exist ", readlabmeta_json_file)
            stderr.print(f"[red] file {readlabmeta_json_file} does not exist")
            sys.exit(1)
        self.readlabmeta_json_file = readlabmeta_json_file

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
        
        self.bioinfo_json_file = os.path.join(os.path.dirname(__file__), "conf", "bioinfo_config.json")
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
            log.error(
                "No configuration available for %s. Currently, the only available software options are: %s", self.software_name, ", ".join(available_software)
            )
            stderr.print(f"[red]No configuration available for {self.software_name}. Currently, the only available software options are:: {', '.join(available_software)}")
            sys.exit(1)
        self.log_report = {'error': {}, 'valid': {}, 'warning': {}}
    
    def get_available_software(self, json):
        """Get list of available software in configuration"""
        config = relecov_tools.utils.read_json_file(json)
        available_software = list(config.keys())
        return available_software
    
    def update_log_report(self, method_name, status, message):
        if status == 'valid':
            self.log_report['valid'].setdefault(method_name, []).append(message)
        elif status == 'error':
            self.log_report['error'].setdefault(method_name, []).append(message)
        elif status == 'warning':
            self.log_report['warning'].setdefault(method_name, []).append(message)
        else:
            raise ValueError("Invalid status provided.")

    def scann_directory(self):
        """Scanns bioinfo analysis directory and identifies files according to the file name patterns defined in the software configuration json."""
        total_files = sum(len(files) for _, _, files in os.walk(self.input_folder))
        files_found = {}

        for topic_key, topic_details  in self.software_config.items():
            if 'fn' not in topic_details: #try/except fn
                continue
            for root, _, files in os.walk(self.input_folder, topdown=True):
                matching_files = [os.path.join(root, file_name) for file_name in files if re.search(topic_details['fn'], file_name)]
                if len(matching_files) >= 1:
                    files_found[topic_key] = matching_files
        if len(files_found) < 1:
            self.update_log_report(
                self.scann_directory.__name__,
                'error', 
                f"No files found in {self.input_folder}"
            )
            log.error(
                "\tNo files found in %s according to %s file name patterns..",
                self.input_folder,
                os.path.basename(self.bioinfo_json_file)
            )
            stderr.print(f"\t[red]No files found in {self.input_folder} according to {os.path.basename(self.bioinfo_json_file)} file name patterns.")
            sys.exit(1)
        else:
            self.update_log_report(
                self.scann_directory.__name__,
                'valid', 
                "Scannig process succeed"
            )
            stderr.print(f"\t[green]Scannig process succeed (total scanned files: {total_files}).")
            return files_found

    def add_filepaths_to_software_config(self, files_dict):
        """
        Adds file paths to the software configuration JSON by creating the 'file_paths' property with files found during the scanning process.
        """
        cc = 0
        extended_software_config = self.software_config
        for key, value in files_dict.items():
            if key in extended_software_config:
                if len(value) != 0 or value:
                    extended_software_config[key]['file_paths'] = value
                    cc+=1
        if cc == 0:
            self.update_log_report(
                self.add_filepaths_to_software_config.__name__,
                'error', 
                "No files path added to configuration json"
            )
        else:
            self.update_log_report(
                self.add_filepaths_to_software_config.__name__,
                'valid', 
                "Files path added to configuration json"
            )
            stderr.print("\t[green]Files path added to their scope in bioinfo configuration file.")
        return extended_software_config

    def validate_software_mandatory_files(self, json):
        missing_required = []
        for key in json.keys():
            if json[key].get('required') is True:
                try:
                    json[key]['file_paths']
                    self.update_log_report(
                        self.validate_software_mandatory_files.__name__,
                        'valid', 
                        f"Found '{json[key]['fn']}'"
                    )
                except KeyError:
                    missing_required.append(key)
                    self.update_log_report(
                        self.validate_software_mandatory_files.__name__,
                        'error', 
                        f"Missing '{json[key]['fn']}'"
                    )
            else:
                continue
        if len(missing_required) >= 1:
            log.error("\tMissing required files:")
            stderr.print("[red]\tMissing required files:")
            for i in missing_required:
                log.error("\t- %s", i)
                stderr.print(f"[red]\t- {i} (file name expected pattern '{json[i]['fn']}')")
            sys.exit(1)
        else:
            stderr.print("[green]\tValidation passed.")
        return
    
    # TODO: ADD LOG REPORT
    def add_bioinfo_results_metadata(self, bioinfo_dict, j_data):
        """
        Adds metadata from bioinformatics results to the JSON data.
        
        This method iterates over each property in the provided bioinfo_dict, which contains information about file paths (discovered during the scanning process), along with their specific file configuration.
        
        If the property specifies files per sample, it maps metadata for each sample-specific file.
        If the property specifies collated files.
        """  
        for key in bioinfo_dict.keys():
            # This skip files that will be parsed with other methods
            if key == 'workflow_summary' or key == "fixed_values":
                continue
            
            # Verify files found are present in key[file_paths].
            try:
                bioinfo_dict[key]['file_paths']
            except KeyError:
                self.update_log_report(
                    self.add_bioinfo_results_metadata.__name__,
                    'warning', 
                    f"No file path found for '{self.software_name}.{key}'"
                )
                continue

            # Handling files
            data_to_map = self.handling_files(bioinfo_dict[key])
            
            # Adding data to j_data
            if data_to_map:
                j_data_mapped = self.mapping_over_table(
                    j_data, 
                    data_to_map, 
                    bioinfo_dict[key]['content'],
                    bioinfo_dict[key]['file_paths']
                )
            else:
                continue
        return j_data_mapped

    # TODO: Add log report(recover file format parsing errors)
    def handling_files(self, bioinfo_dict_scope):
        """Handles different file formats (sourced from ./metadata_homogenizer.py)
        """
        file_name = bioinfo_dict_scope['fn']
        file_extension = os.path.splitext(file_name)[1]

        # Parsing key position
        try:
            bioinfo_dict_scope['sample_col_idx']
            sample_idx_possition = bioinfo_dict_scope['sample_col_idx']-1
        except KeyError:
            sample_idx_possition = None
        
        # Parsing files
        func_name = bioinfo_dict_scope["function"]
        if func_name is None:
            if file_name.endswith('.csv'):
                data = relecov_tools.utils.read_csv_file_return_dict(
                    file_name=bioinfo_dict_scope['file_paths'][0],
                    sep=",",
                    key_position=sample_idx_possition
                )
                return data
            elif file_name.endswith('.tsv') or file_name.endswith('.tab'):
                data = relecov_tools.utils.read_csv_file_return_dict(
                    file_name=bioinfo_dict_scope['file_paths'][0],
                    sep="\t",
                    key_position=sample_idx_possition
                )
            else:
                stderr.print(f"[red]Unrecognized defined file name extension {file_extension} in {bioinfo_dict_scope['fn']}")
                sys.exit()
        else:
            try:
                # TODO: ADD stdout to identify which data is being added. 
                # Attempt to get the method by name
                method_to_call = getattr(self, func_name)
                data = method_to_call(bioinfo_dict_scope['file_paths'])
            except AttributeError as e:
                if "not found" in str(e):
                    stderr.print(f"[red]Function '{func_name}' not found in class.")
                return None
        return data


    def handle_csv_file(self, bioinfo_dict_scope):
        """handle csv/tsv file and map it with read lab metadata (j_data)"""
        map_data = relecov_tools.utils.read_csv_file_return_dict(
            file_name = bioinfo_dict_scope['file_paths'],
            sep = bioinfo_dict_scope['ff'],
            key_position = (bioinfo_dict_scope['sample_col_idx']-1)
        )
        return map_data

    def select_most_recent_files_per_sample(self, paths_list):
        """Selects the most recent file for each sample among potentially duplicated files.
            Input:
                - paths_list: a list of sample's file paths.
        """
        filename_groups = {}
        # Count occurrences of each filename and group files by sample names
        for file in paths_list:
            file_name = os.path.basename(file).split('.')[0]
            if file_name in filename_groups :
                filename_groups [file_name].append(file)
            else:
                filename_groups [file_name] = [file]
        
        # Filter out sample names with only one file
        duplicated_files = [(sample_name, file_paths) for sample_name, file_paths in filename_groups.items() if len(file_paths) > 1]

        # Iterate over duplicated files to select the most recent one for each sample
        for sample_name, file_paths in duplicated_files:
            stderr.print(f"More than one pangolin file found for sample {sample_name}. Selecting the most recent one.")

            # Sort files by modification time (most recent first)
            sorted_files = sorted(file_paths, key=lambda file_path: os.path.getmtime (file_path), reverse=True)

            # Select the most recent file
            selected_file = sorted_files[0]
            stderr.print(f"Selected file for sample {sample_name}: {selected_file}")

            # Remove other files for the same sample from the filtered_files dictionary
            filename_groups[sample_name] = [selected_file]

        # Update filename_groups with filtered files
        filename_groups = [(sample_name, file_path) for sample_name, file_paths in filename_groups.items() for file_path in file_paths]

        # Reformat variable to retrieve a list of file paths
        file_path_list = [sample_file_path for _, sample_file_path in filename_groups]
        return file_path_list

    def handle_multiqc_html_file(self, bioinfo_dict_scope, j_data):
        """Reads html file, finds table containing programs info, and map it to j_data"""
        program_versions = {}
        with open(bioinfo_dict_scope['file_paths'], 'r') as html_file:
            html_content = html_file.read()
        # Load HTML
        soup = BeautifulSoup(html_content, features="lxml")
        # Get version's div id
        div_id = "mqc-module-section-software_versions"
        versions_div = soup.find('div', id=div_id)
        # Get version's metadata data
        if versions_div:
            table = versions_div.find('table', class_='table')
            if table:
                rows = table.find_all('tr')
                for row in rows[1:]: #skipping header
                    columns = row.find_all('td')
                    if len(columns) == 3:
                        program_name = columns[1].text.strip()
                        version = columns[2].text.strip()
                        program_versions[program_name] = version
                    else:
                        stderr.print(f"[red] HTML entry error in {columns}. HTML table expected format should be \n<th> Process Name\n</th>\n<th> Software </th>\n.")
            else:
                stderr.print(f"[red] Missing table containing software versions in {bioinfo_dict_scope['file_paths']}.")
                sys.exit(1)
        else:
            log.error(f"Required div section 'mqc-module-section-software_versions' not found in file {bioinfo_dict_scope['file_paths']}.")
            stderr.print(f"[red] No div section  'mqc-module-section-software_versions' was found in {bioinfo_dict_scope['file_paths']}.")
            sys.exit(1)
                        
        # mapping mqc sofware versions to j_data
        field_errors = {}
        for row in j_data:
            sample_name = row["submitting_lab_sample_id"]
            for field, values in bioinfo_dict_scope['content'].items():
                try:
                    row[field] = program_versions[values]
                except KeyError as e:
                    field_errors[sample_name] = {field: e}
                    row[field] =  "Not Provided [GENEPIO:0001668]"
                    continue
        return j_data

    def mapping_over_table(self, j_data, map_data, mapping_fields, table_name):
        """Auxiliar function to iterate over table's content and map it to metadata (j_data)"""
        errors = []
        field_errors = {}
        for row in j_data:
            sample_name = row["submitting_lab_sample_id"]
            if sample_name in map_data:
                for field, value in mapping_fields.items():
                    try:
                        row[field] = map_data[sample_name][value]
                    except KeyError as e:
                        field_errors[sample_name] = {field: e}
                        row[field] = "Not Provided [GENEPIO:0001668]"
                        continue
            else:
                errors.append(sample_name)
                for field in mapping_fields.keys():
                    row[field] = "Not Provided [GENEPIO:0001668]"
        if errors:
            lenerrs = len(errors)
            log.error(
                "\t{0} samples missing in {1}:\n\t{2}".format(lenerrs, table_name, errors)
            )
            stderr.print(f"\t[red]{lenerrs} samples missing in {table_name}:\n\t{errors}")
        if field_errors:
            log.error("\tFields not found in {0}:\n\t{1}".format(table_name, field_errors))
            stderr.print(f"\t[red]Missing values in {table_name}:\n\t{field_errors}")
        return j_data

    # TODO: haven't improved yet
    def include_pangolin_data(self, dir_path, j_data):
        """Include pangolin data collecting form each file generated by pangolin"""
        mapping_fields = self.software_config["mapping_pangolin"]["content"]
        missing_pango = []
        for row in j_data:
            # Get read lab sample id
            if "-" in row["submitting_lab_sample_id"]:
                sample_name = row["submitting_lab_sample_id"].replace("-", "_")
            else:
                sample_name = row["submitting_lab_sample_id"]
            # Get the name of pangolin csv file/s
            f_name_regex = sample_name + ".pangolin*.csv"
            f_path = os.path.join(dir_path, f_name_regex)
            pango_files = glob.glob(f_path)
            # Parse pangolin files
            if pango_files:
                if len(pango_files) > 1:
                    stderr.print(
                        "[yellow]More than one pangolin file found for sample",
                        f"[yellow]{sample_name}. Selecting the most recent one",
                    )
                try:
                    pango_files = sorted(
                        pango_files,
                        key=lambda dt: datetime.strptime(dt.split(".")[-2], "%Y%m%d"),
                    )
                    row["lineage_analysis_date"] = pango_files[0].split(".")[-2]
                except ValueError:
                    log.error("\tNo date found in %s pangolin files", sample_name)
                    stderr.print(
                        f"\t[red]No date found in sample {sample_name}. Pangolin filenames:",
                        f"\n\t[red]{pango_files}",
                    )
                    stderr.print("\t[yellow]Using mapping analysis date instead")
                    # If no date in pangolin files, set date as analysis date
                    row["lineage_analysis_date"] = row["analysis_date"]
                f_data = relecov_tools.utils.read_csv_file_return_dict(
                    pango_files[0], sep=","
                )
                pang_key = list(f_data.keys())[0]
                for field, value in mapping_fields.items():
                    row[field] = f_data[pang_key][value]
            else:
                missing_pango.append(sample_name)
                for field in mapping_fields.keys():
                    row[field] = "Not Provided [GENEPIO:0001668]"
        if len(missing_pango) >= 1:
            stderr.print(
                f"\t[yellow]{len(missing_pango)} samples missing pangolin.csv file:"
            )
            stderr.print(f"\t[yellow]{missing_pango}")
        return j_data

    def handle_consensus_fasta(self, bioinfo_dict_scope, j_data):
        """Include genome length, name, file name, path and md5 by preprocessing
        each file of consensus.fa"""
        mapping_fields = bioinfo_dict_scope["content"]
        missing_consens = []
        consensus_dir_path = os.path.dirname(bioinfo_dict_scope['file_paths'][0])
        # FIXME: Replace  sequencing_sample_id
        for row in j_data:
            if "-" in row["submitting_lab_sample_id"]:
                sample_name = row["submitting_lab_sample_id"].replace("-", "_")
            else:
                sample_name = row["submitting_lab_sample_id"]
            f_name = sample_name + ".consensus.fa"
            f_path = os.path.join(consensus_dir_path, f_name)
            try:
                record_fasta = relecov_tools.utils.read_fasta_return_SeqIO_instance(
                    f_path
                )
            except FileNotFoundError as e:
                missing_consens.append(e.filename)
                for item in mapping_fields:
                    row[item] = "Not Provided [GENEPIO:0001668]"
                continue
            row["consensus_genome_length"] = str(len(record_fasta))
            row["consensus_sequence_name"] = record_fasta.description
            row["consensus_sequence_filepath"] = self.input_folder
            row["consensus_sequence_filename"] = f_name
            row["consensus_sequence_md5"] = relecov_tools.utils.calculate_md5(f_path)
            if row["read_length"].isdigit():
                base_calculation = int(row["read_length"]) * len(record_fasta)
                if row["submitting_lab_sample_id"] != "Not Provided [GENEPIO:0001668]":
                    row["number_of_base_pairs_sequenced"] = str(base_calculation * 2)
                else:
                    row["number_of_base_pairs_sequenced"] = str(base_calculation)
            else:
                row["number_of_base_pairs_sequenced"] = "Not Provided [GENEPIO:0001668]"
        # TODO: WAIT TO FIXED IN POST ADAPTATION PR
        conserrs = len(missing_consens)
        if conserrs >= 1:
            log.error(
                "\t{0} Consensus files missing:\n\t{1}".format(
                    conserrs, missing_consens
                )
            )
            stderr.print(f"\t[yellow]{conserrs} samples missing consensus file:")
            stderr.print(f"\n\t[yellow]{missing_consens}")
        return j_data

    def include_custom_data(self, j_data):
        """Include custom fields like variant-long-table path"""
        condition = os.path.join(self.input_folder, "*variants_long_table*.csv")
        f_path = relecov_tools.utils.get_files_match_condition(condition)
        if len(f_path) == 0:
            long_table_path = "Not Provided [GENEPIO:0001668]"
        else:
            long_table_path = f_path[0]
        for row in j_data:
            row["long_table_path"] = long_table_path
        return j_data

    def add_fixed_values(self, j_data):
        """include the fixed data defined in configuration or feed custom empty fields"""
        f_values = self.software_config["fixed_values"]
        for row in j_data:
            for field, value in f_values.items():
                row[field] = value
        return j_data

    def collect_info_from_lab_json(self):
        """Create the list of dictionaries from the data that is on json lab
        metadata file. Return j_data that is used to add the rest of the fields
        """
        try:
            json_lab_data = relecov_tools.utils.read_json_file(self.readlabmeta_json_file)
        except ValueError:
            log.error("%s invalid json file", self.readlabmeta_json_file)
            stderr.print(f"[red] {self.readlabmeta_json_file} invalid json file")
            sys.exit(1)
        return json_lab_data

    def create_bioinfo_file(self):
        """Create the bioinfodata json with collecting information from lab
        metadata json, mapping_stats, and more information from the files
        inside input directory
        """
        stderr.print("[blue]Sanning input directory...")
        files_found_dict = self.scann_directory()
        stderr.print("[blue]Adding files found to bioinfo config json...")
        software_config_extended = self.add_filepaths_to_software_config(files_found_dict)
        stderr.print("[blue]Validating required files...")
        self.validate_software_mandatory_files(software_config_extended)
        stderr.print("[blue]Reading lab metadata json")
        j_data = self.collect_info_from_lab_json()
        stderr.print(f"[blue]Adding metadata from {self.input_folder} into read lab metadata...")
        j_data = self.add_bioinfo_results_metadata(software_config_extended, j_data)
        #TODO: This should be refactor according to new file-handling implementation
        #stderr.print("[blue]Adding variant long table path")
        #j_data = self.include_custom_data(j_data)
        stderr.print("[blue]Adding fixed values")
        j_data = self.add_fixed_values(j_data)
        file_name = (
            "bioinfo_" + os.path.splitext(os.path.basename(self.readlabmeta_json_file))[0] + ".json"
        )
        stderr.print("[blue]Writting output json file")
        os.makedirs(self.output_folder, exist_ok=True)
        file_path = os.path.join(self.output_folder, file_name)
        relecov_tools.utils.write_json_fo_file(j_data, file_path)
        stderr.print("[green]Sucessful creation of bioinfo analyis file")
        return True
