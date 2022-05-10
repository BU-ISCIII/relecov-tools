#!/usr/bin/env python
# from itertools import islice

# from geopy.geocoders import Nominatim
# import json
import logging
import yaml

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
            self.output_folder = output_folder
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
            bioinfo_dict["fastq_r1"] = fastq_r2
            bioinfo_dict["dehosting_method_software_name"] = relecov_bioinfo_metadata[
                "dehosting_method_software_name"
            ]  # software_versions.yml software_list["KRAKEN2_KRAKEN2"].keys(0)
            bioinfo_dict[
                "dehosting_method_software_version"
            ] = relecov_bioinfo_metadata[
                "dehosting_method_software_version"
            ]  # software_versions.yml software_list["KRAKEN2_KRAKEN2"].values(0)
            bioinfo_dict["assembly"] = None
            bioinfo_dict["if_assembly_other"] = None
            bioinfo_dict["assembly_params"] = None
            bioinfo_dict["variant_calling_software_name"] = relecov_bioinfo_metadata[
                "variant_calling_software_name"
            ]  # software_versions.yml software_list["IVAR_VARIANTS"].keys(0)
            bioinfo_dict["variant_calling_software_version"] = relecov_bioinfo_metadata[
                "variant_calling_software_version"  # software_versions.yml software_list["IVAR_VARIANTS"].values(0)
            ]
            bioinfo_dict["variant_calling_params"] = relecov_bioinfo_metadata[
                "variant_calling_params"
            ]
            # bioinfo_dict["consensus_sequence_name"]=
            # bioinfo_dict["consensus_sequence_name_md5"]=
            bioinfo_dict["consensus_sequence_filepath"] = self.input_folder

            bioinfo_dict["consensus_sequence_software_name"] = relecov_bioinfo_metadata[
                "consensus_sequence_software_name"
            ]  # software_versions.yml software_list["BCFTOOLS_CONSENSUS"].keys(0)
            bioinfo_dict[
                "consensus_sequence_software_version"
            ] = relecov_bioinfo_metadata[
                "consensus_sequence_software_version"  # software_versions.yml software_list["BCFTOOLS_CONSENSUS"].values(0)
            ]

            bioinfo_dict["if_consensus_other"] = None
            """
            "dehosting_method": "", RENAMED to dehosting_software_name y dehosting_software_version
            "if_assembly_other": "",
            "assembly_params": "", 
            "variant_calling": "", RENAMED to variant_calling_software_name y variant_calling_software_version
            "if_variant_calling_other": "",
            "variant_calling_params": "", 
            "consensus_sequence_name": "",
            "consensus_sequence_name_md5": "",
            "consensus_sequence_filepath": "",
            "consensus_sequence_software_name": "",
            "if_consensus_other": "",
            "consensus_sequence_software_version": "",

            "consensus_criteria": "", RENAMED to consensus_params
            "depth_of_coverage_threshold": "",
            "number_of_base_pairs_sequenced": "",
            "consensus_genome_length": "",
            "ns_per_100_kbp": "",
            "reference_genome_accession": "",
            "bioinformatics_protocol": "",
            "if_bioinformatic_protocol_is_other_specify": "",
            "bioinformatic_protocol_version": "",
            "commercial/open-source/both": "",
            "preprocessing": "",
            "if_preprocessing_other": "",
            "preprocessing_params": "",
            "mapping": "",
            "if_mapping_other": "",
            "mapping_params": "",
            "lineage_name": "",
            "lineage_analysis_software_name": "",
            "if_lineage_identification_other": "",
            "lineage_analysis_software_version": "",
            "variant_designation": "",
            "per_qc_filtered": "",
            "per_reads_host": "",
            "per_reads_virus": "",
            "per_unmapped": "",
            "per_genome _greater_10x": "",
            "median_depth_of_coverage_value": "",
            "per_Ns": "",
            "number_of_variants_AF_greater_75percent": "",
            "number_of_variants_with_effect": "",
            "long_table_path": ""

            """
            path_software_version = os.path.join(
                self.input_folder, "software_versions.yml"
            )
            with open(path_software_version) as file:
                software_list = yaml.load(file, Loader=yaml.FullLoader)

            print(bioinfo_dict)
            import pdb

            pdb.set_trace()
