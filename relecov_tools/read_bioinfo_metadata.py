#!/usr/bin/env python
# from itertools import islice

# from geopy.geocoders import Nominatim
# import json
import logging

# import yaml

# from turtle import pd
import rich.console
from itertools import islice

# from openpyxl import Workbook
import openpyxl
import os
import sys
import relecov_tools.utils

from relecov_tools.config_json import ConfigJson
import relecov_tools.json_schema

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class BioinfoMetadata:
    def __init__(self, metadata_file=None, input_folder=None, output_folder=None):
        if metadata_file is None:
            self.metadata_file = relecov_tools.utils.prompt_path(
                msg="Select the excel file which contains metadata"
            )
        else:
            self.metadata_file = metadata_file
        if not os.path.exists(self.metadata_file):
            log.error("Metadata file %s does not exist ", self.metadata_file)
            stderr.print(
                "[red] Metadata file " + self.metadata_file + " does not exist"
            )
            sys.exit(1)
        if input_folder is None:
            self.input_folder = relecov_tools.utils.prompt_path(
                msg="Select the input folder"
            )
        else:
            self.input_folder = output_folder
        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = output_folder

    def bioinfo_parse(self, file_name):
        """Fetch the metadata file folder  Directory to fetch metadata file
        file_name   metadata file name
        """

        wb_file = openpyxl.load_workbook(file_name, data_only=True)
        ws_metadata_lab = wb_file["METADATA_LAB"]
        config_json = ConfigJson()
        relecov_bioinfo_metadata = config_json.get_configuration(
            "relecov_bioinfo_metadata"
        )

        for row in islice(ws_metadata_lab.values, 4, ws_metadata_lab.max_row):
            # row = ws_metadata_lab[5]
            sample_name = row[5]
            fastq_r1 = row[47]
            fastq_r2 = row[48]

            bioinfo_dict = {}
            bioinfo_dict["sample_name"] = sample_name
            bioinfo_dict["fastq_r1"] = fastq_r1
            bioinfo_dict["fastq_r2"] = fastq_r2
        for key in relecov_bioinfo_metadata.keys():
            bioinfo_dict[key] = relecov_bioinfo_metadata[key]
        bioinfo_dict["consensus_sequence_filepath"] = self.input_folder
        bioinfo_dict["long_table_path"] = self.input_folder
        """
        # "dehosting_method_software_version" # NO HARCODED
        # "variant_calling_software_version" # NO HARCODED
        # "consensus_sequence_software_version" # NO HARCODED
        # "bioinformatics_protocol_software_version" # NO HARCODED
        # "preprocessing_software_version"# NO HARCODED
        # "mapping_software_version" # NO HARCODED
        # "lineage_analysis_software_version"
        # "lineage_name": "" mapping_illumina
        # "number_of_base_pairs_sequenced": "", # Input reads summary_variants_metrics_mqc.csv  * 2 * read length
        # "consensus_genome_length": "", script que cuente el numero de nucleotidos tamaño del fasta
        # "ns_per_100_kbp": "", summary_variants_metrics_mqc.csv
        # "reference_genome_accession": "",
        # bioinfo_dict["consensus_sequence_name"]=
        # bioinfo_dict["consensus_sequence_name_md5"]=
        # "variant_designation": "", pangolin csv parseo
        # "per_qc_filtered": "", tabla stats
        # "per_reads_host": "", tabla stats
        # "per_reads_virus": "", tabla stats
        # "per_unmapped": "", tabla stats
        # "per_genome _greater_10x": "",  tabla stats
        # "median_depth_of_coverage_value": "", tabla stats
        # "per_Ns": "", tabla stats
        # "number_of_variants_AF_greater_75percent": "", tabla stats
        # "number_of_variants_with_effect": "", tabla stats
        bioinfo_dict["long_table_path"] = self.input_folder
        """
        print(bioinfo_dict)
        import pdb

        pdb.set_trace()
