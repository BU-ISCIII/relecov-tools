#!/usr/bin/env python
import os
import sys
import logging
import glob
import rich.console
import re
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

# FIXME: longtable parsing old method needs to be recovered. New implementation has errors while parsing it.
# TODO: Add method to validate bioinfo_config.json file requirements.
# TODO: Cosider eval py + func property in json to be able to discriminate between collated files and sample-specific files.
# TODO: replace submitting_lab_id by sequencing_sample_id
# TODO: manage bioinfo config and files_found paths in separate variables.
# TODO: improve method's description (specifically 'handling_files')
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
    # TODO: add better stdout/err to show which method is being called.
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

    # TODO: add log report
    def get_multiqc_software_versions(self, bioinfo_dict_scope, j_data):
        """Reads html file, finds table containing programs info, and map it to j_data"""
        # Handle multiqc_report.html
        f_path = bioinfo_dict_scope['file_paths'][0]
        program_versions = {}

        with open(f_path, 'r') as html_file:
            html_content = html_file.read()
        soup = BeautifulSoup(html_content, features="lxml")
        div_id = "mqc-module-section-software_versions"
        versions_div = soup.find('div', id=div_id)
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
                        
        # Mapping multiqc sofware versions to j_data
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

    # TODO: update log report
    def mapping_over_table(self, j_data, map_data, mapping_fields, table_name):
        """Auxiliar function to iterate over table's content and map it to metadata (j_data)"""
        errors = []
        field_errors = {}
        for row in j_data:
            sample_name = row["submitting_lab_sample_id"].replace("-", "_")
            if sample_name in map_data.keys():
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
            # work around when map_data comes from several per-sample tables instead of single table
            if len(table_name) > 1:
                table_name = (os.path.dirname(table_name[0]))
            log.error(
                    "\t{0} samples missing in {1}:\n\t{2}".format(lenerrs, table_name, errors)
                )
            stderr.print(f"\t[red]{lenerrs} samples missing in {table_name}:\n\t{errors}")        
        if field_errors:
            log.error("\tFields not found in {0}:\n\t{1}".format(table_name, field_errors))
            stderr.print(f"\t[red]Missing values in {table_name}:\n\t{field_errors}")
        return j_data

    # TODO: add log report
    def handle_pangolin_data(self, files_list):
        """Parse pangolin data (csv) into JSON and map it to each sample in the provided j_data.
        """
        # Handling pangolin data
        pango_data_processed = {}
        try:
            files_list_processed = relecov_tools.utils.select_most_recent_files_per_sample(files_list)
            for pango_file in files_list_processed:
                try:
                    pango_data = relecov_tools.utils.read_csv_file_return_dict(
                        pango_file, sep=","
                    )
                    # Add custom content in pangolin
                    pango_data_key = next(iter(pango_data))
                    pango_data[pango_data_key]['lineage_analysis_date'] = relecov_tools.utils.get_file_date(
                        pango_file
                    )

                    # Rename key in f_data
                    pango_data_updated = {key.split()[0]: value for key, value in pango_data.items()}
                    pango_data_processed.update(pango_data_updated)
                except (FileNotFoundError, IndexError) as e:
                    stderr.print(f"[red]Error processing file {pango_file}: {e}")
                    continue
        except Exception as e:
            stderr.print(f"[red]Error occurred while processing files: {e}")
            sys.exit()
        return pango_data_processed

    # TODO: add log report
    def handle_consensus_fasta(self, files_list):
        """Handling consensus fasta data (*.consensus.fa)"""
        consensus_data_processed = {}
        missing_consens = []
        for consensus_file in files_list:
            try:
                record_fasta = relecov_tools.utils.read_fasta_return_SeqIO_instance(
                    consensus_file
                )
            except FileNotFoundError as e:
                missing_consens.append(e.filename)
                continue
            sample_key = re.sub(self.software_config['mapping_consensus']['fn'], '', os.path.basename(consensus_file))

            # Update consensus data for the sample key
            consensus_data_processed[sample_key] = {
                'sequence_name': record_fasta.description,
                'genome_length': str(len(record_fasta)),
                'sequence_filepath': os.path.dirname(consensus_file),
                'sequence_filename': sample_key,
                'sequence_md5': relecov_tools.utils.calculate_md5(consensus_file),
                # TODO: Not sure this is correct. If not, recover previous version: https://github.com/BU-ISCIII/relecov-tools/blob/09c00c1ddd11f7489de7757841aff506ef4b7e1d/relecov_tools/read_bioinfo_metadata.py#L211-L218
                'number_of_base_pairs_sequenced': len(record_fasta.seq)
            }         

        # Report missing consensus
        conserrs = len(missing_consens)
        if conserrs >= 1:
            log.error(
                "\t{0} Consensus files missing:\n\t{1}".format(
                    conserrs, missing_consens
                )
            )
            stderr.print(f"\t[yellow]{conserrs} samples missing consensus file:")
            stderr.print(f"\n\t[yellow]{missing_consens}")
        return consensus_data_processed

    # TODO: add log report
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

    # TODO: this is too harcoded. Find a way to add file's path of required files when calling handlers functions. 
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
        stderr.print("[blue]Adding software versions to read lab metadata...")
        j_data = self.get_multiqc_software_versions(software_config_extended['workflow_summary'], j_data)
        # FIXME: this isn't refactored and requires to be reimplemented from older version of this module
        ##stderr.print("[blue]Adding variant long table path")
        ##j_data = self.include_custom_data(j_data)
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
