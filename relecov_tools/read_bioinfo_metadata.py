#!/usr/bin/env python
import os
import sys
import logging
import glob
import rich.console
from datetime import datetime
from tqdm import tqdm
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
        json_file=None,
        input_folder=None,
        output_folder=None,
        software='viralrecon',
    ):
        if json_file is None:
            json_file = relecov_tools.utils.prompt_path(
                msg="Select the json file that was created by the read-lab-metadata"
            )
        if not os.path.isfile(json_file):
            log.error("json file %s does not exist ", json_file)
            stderr.print(f"[red] file {json_file} does not exist")
            sys.exit(1)
        self.json_file = json_file

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

        # TODO: Available software list can be retrieved from conf/bioinfo_search_patterns.yml
        # TODO: Add error if software is not in the list (sys exit). Add output to global log.
        if software is None:
            software = relecov_tools.utils.prompt_path(
                msg="Select the software, pipeline or tool use in the bioinformatic analysis (available: 'viralrecon'): "
            )
        self.software_name = software
        json_file = os.path.join(os.path.dirname(__file__), "conf", "bioinfo_config.json")
        config = ConfigJson(json_file)
        self.software_config = config.get_configuration(self.software_name)

    # FIXME: This must be refacored. not used so far. 
    def get_software_required_files(self):
        """Load required software specific files and patterns"""
        self.required_file_name = {}
        self.required_file_content = {}

        for key, value in self.software_config.items():
            if 'required' in value and value['required']:
                self.required_file_name[key] = value.get('fn','')
                self.required_file_content[key] = value.get('content','')

    # TODO: Add report of files found/not-found to master log
    def scann_directory(self):
        """Scann bioinfo analysis directory and search for files present in bioinfo json config"""
        total_files = sum(len(files) for _, _, files in os.walk(self.input_folder))
        files_found = {}
        with tqdm(total=total_files, desc='\tScanning...') as pbar:
            for topic_key, topic_details  in self.software_config.items():
                if 'fn' not in topic_details: #try/except fn
                    continue
                for root, _, files in os.walk(self.input_folder, topdown=True):
                    matching_files = [os.path.join(root, file_name) for file_name in files if file_name.endswith(topic_details['fn'])]
                    if len(matching_files) == 1:
                        # Only one file match found, add it as a string (collated files)
                        files_found[topic_key] = matching_files[0]
                    elif len(matching_files) > 1:
                        # Multiple file matches found, add them as a list (per sample files)
                        files_found[topic_key] = matching_files
                    for _ in matching_files:
                        pbar.update(1)
        if len(files_found) < 1:
            log.error(
                "No files found in %s.", self.output_folder
            )
            stderr.print(f"[red] No files found in {self.output_folder}.")
            sys.exit(1)
        else:
            stderr.print("\tRetrieving files found ...")
            return files_found

    def extend_software_config(self, files_dict):
        """Inject files found (input dir) into software config JSON"""
        extended_json = self.software_config
        for key, value in files_dict.items():
            if key in extended_json:
                if isinstance(extended_json[key], list):
                    # If the existing value is a list, extend it with the new file paths
                    extended_json[key]['file_paths'].extend(value)
                else:
                    # If the existing value is not a list, create a new list with the file paths
                    extended_json[key]['file_paths'] = value
        return extended_json


    # TODO: Add validation to master log file.
    # TODO: Add checking file format based on config.
    # TODO: ¿Add content validation?. This might be better to be implemented when geting metadata from input files.
    def validate_software_mandatory_files(self, json):
        missing_required = []
        for key in json.keys():
            if json[key].get('required') is True:
                try:
                    json[key]['file_paths']
                except KeyError:
                    missing_required.append(key)
            else:
                continue
        if len(missing_required) >= 1:
            log.error("\tMissing required files:")
            stderr.print("[red]\tMissing required files:")
            for i in missing_required:
                log.error("[red]\t\t- %s", i)
                stderr.print(f"\t\t- {i}")
            sys.exit(1)
        else:
            stderr.print("[green]\tValidation passed :)")
        return

    # TODO: also version's files shoudld be paresed independiently.
    # TODO: Before arriving here we need to validate properties fiels on collated and persample files(~mandatory fields). 
    def add_bioinfo_results_metadata(self, bioinfo_dict, j_data):
        """Iterates over each property in the bioinfo_dict"""
        # TODO: add manatory fields: one for collated and one for sample-specific files
        # mandatory_fields = ['fn', 'ff', 'required', 'content', 'file_paths']
        for key in bioinfo_dict.keys():
            try:
                bioinfo_dict[key].get('file_paths')
            except KeyError:
                continue
            # Parses sample-specific files (i.e: SAMPLE1.consensus.fa)
            if isinstance(bioinfo_dict[key].get('file_paths'), list):
                stderr.print("")
                j_data_mapped = self.map_metadata_persample_files(
                    bioinfo_dict[key],
                    j_data
                )
            # Parses collated files (i.e: mapping_illumina_stats.tab)
            elif isinstance(bioinfo_dict[key].get('file_paths'), str):
                j_data_mapped = self.map_metadata_collated_files(
                    bioinfo_dict[key], 
                    j_data
                    )
        return j_data_mapped

    def map_metadata_persample_files(self, bioinfo_dict_scope, j_data):
        """"""
        file_name = bioinfo_dict_scope['fn']
        file_format = bioinfo_dict_scope['ff']
        if file_format == ',' and 'pangolin' in file_name:
            j_data_mapped = self.include_pangolin_data(j_data)
        elif file_format == 'fasta' and 'consensus' in file_name:
            j_data_mapped = self.handle_consensus_fasta(bioinfo_dict_scope, j_data)
        else:
            stderr.warning(f"[red]No available methods to parse file format '{file_format}' and file name '{file_name}'.")
            return
        return j_data_mapped
        
    # TODO: recover file format parsing errors
    def map_metadata_collated_files(self, bioinfo_dict_scope, j_data):
        """Handles different file formats in collated files, reads their content, and maps it to j_data"""
        # We will be able to add here as many handlers as we need
        file_extension_handlers = {
            "\t": self.handle_csv_file,
            ",": self.handle_csv_file,
            "html": self.handle_multiqc_html_file,
        }
        file_format = bioinfo_dict_scope['ff']
        if file_format in file_extension_handlers:
            handler_function = file_extension_handlers[file_format]
            j_data_mapped = handler_function(bioinfo_dict_scope, j_data)
            return j_data_mapped
        else:
            stderr.print(f"[red]Unrecognized defined file format {bioinfo_dict_scope['ff'] in {bioinfo_dict_scope['fn']}}")
            return None
    
    def handle_csv_file(self, bioinfo_dict_scope, j_data):
        """handle csv/tsv file and map it with read lab metadata (j_data)"""
        map_data = relecov_tools.utils.read_csv_file_return_dict(
            file_name = bioinfo_dict_scope['file_paths'],
            sep = bioinfo_dict_scope['ff'],
            key_position = (bioinfo_dict_scope['sample_col_idx']-1)
        )
        j_data_mapped = self.mapping_over_table(
            j_data, 
            map_data, 
            bioinfo_dict_scope['content'],
            bioinfo_dict_scope['file_paths']
        )
        return j_data_mapped

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
                        
        # Adding mqc sofware versions to j_data
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
        """Auxiliar function to iterate over variants and mapping tables to map metadata"""
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
                "{0} samples missing in {1}:\n{2}".format(lenerrs, table_name, errors)
            )
            stderr.print(f"[red]{lenerrs} samples missing in {table_name}:\n{errors}")
        if field_errors:
            log.error("Fields not found in {0}:\n{1}".format(table_name, field_errors))
            stderr.print(f"[red]Missing values in {table_name}\n:{field_errors}")
        return j_data

    def include_pangolin_data(self, j_data):
        """Include pangolin data collecting form each file generated by pangolin"""
        mapping_fields = self.software_config["mapping_pangolin"]["content"]
        missing_pango = []
        for row in j_data:
            if "-" in row["submitting_lab_sample_id"]:
                sample_name = row["submitting_lab_sample_id"].replace("-", "_")
            else:
                sample_name = row["submitting_lab_sample_id"]

            f_name_regex = sample_name + ".pangolin*.csv"
            f_path = os.path.join(self.input_folder, f_name_regex)
            pango_files = glob.glob(f_path)
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
                    log.error("No date found in %s pangolin files", sample_name)
                    stderr.print(
                        f"[red]No date found in sample {sample_name}",
                        f"[red]pangolin filenames: {pango_files}",
                    )
                    stderr.print("[Yellow] Using mapping analysis date instead")
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
                f"[yellow]{len(missing_pango)} samples missing pangolin.csv file:"
            )
            stderr.print(f"[yellow]{missing_pango}")
        return j_data

    def handle_consensus_fasta(self, bioinfo_dict_scope, j_data):
        """Include genome length, name, file name, path and md5 by preprocessing
        each file of consensus.fa"""
        mapping_fields = bioinfo_dict_scope["content"]
        missing_consens = []
        # FIXME: Replace  sequencing_sample_id
        for row in j_data:
            if "-" in row["submitting_lab_sample_id"]:
                sample_name = row["submitting_lab_sample_id"].replace("-", "_")
            else:
                sample_name = row["submitting_lab_sample_id"]
            f_name = sample_name + ".consensus.fa"
            f_path = os.path.join(self.input_folder, f_name)
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
                "{0} Consensus files missing:\n{1}".format(
                    conserrs, missing_consens
                )
            )
            stderr.print(f"[yellow]{conserrs} samples missing consensus file:")
            stderr.print(f"[yellow]\n{missing_consens}")
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
            json_lab_data = relecov_tools.utils.read_json_file(self.json_file)
        except ValueError:
            log.error("%s invalid json file", self.json_file)
            stderr.print(f"[red] {self.json_file} invalid json file")
            sys.exit(1)
        return json_lab_data

    def create_bioinfo_file(self):
        """Create the bioinfodata json with collecting information from lab
        metadata json, mapping_stats, and more information from the files
        inside input directory
        """
        stderr.print("[blue]Sanning input directory...")
        files_found = self.scann_directory()
        stderr.print("[blue]Extending bioinfo config json with files found...")
        software_config_extended = self.extend_software_config(files_found)
        stderr.print("[blue]Validating required files...")
        self.validate_software_mandatory_files(software_config_extended)
        stderr.print("[blue]Reading lab metadata json")
        j_data = self.collect_info_from_lab_json()
        stderr.print(f"[blue]Adding metadata from {self.input_folder} into read lab metadata...")
        j_data = self.add_bioinfo_results_metadata(software_config_extended, j_data)
        #stderr.print("[blue]Adding variant long table path")
        #j_data = self.include_custom_data(j_data)
        stderr.print("[blue]Adding fixed values")
        j_data = self.add_fixed_values(j_data)
        #file_name = (
        #    "bioinfo_" + os.path.splitext(os.path.basename#(self.json_file))[0] + ".json"
        #)
        #stderr.print("[blue]Writting output json file")
        #os.makedirs(self.output_folder, exist_ok=True)
        #file_path = os.path.join(self.output_folder, #file_name)
        #relecov_tools.utils.write_json_fo_file(j_data, #file_path)
        #stderr.print("[green]Sucessful creation of bioinfo #analyis file")
        return True
