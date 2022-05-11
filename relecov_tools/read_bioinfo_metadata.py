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
            bioinfo_dict["dehosting_method_software_name"] = relecov_bioinfo_metadata[
                "dehosting_method_software_name"
            ]
            bioinfo_dict[
                "dehosting_method_software_version"
            ] = relecov_bioinfo_metadata["dehosting_method_software_version"]
            bioinfo_dict["assembly"] = relecov_bioinfo_metadata["assembly"]
            bioinfo_dict["if_assembly_other"] = relecov_bioinfo_metadata[
                "if_assembly_other"
            ]
            bioinfo_dict["assembly_params"] = relecov_bioinfo_metadata[
                "assembly_params"
            ]
            bioinfo_dict["variant_calling_software_name"] = relecov_bioinfo_metadata[
                "variant_calling_software_name"
            ]
            bioinfo_dict["variant_calling_software_version"] = relecov_bioinfo_metadata[
                "variant_calling_software_version"
            ]
            bioinfo_dict["variant_calling_params"] = relecov_bioinfo_metadata[
                "variant_calling_params"
            ]
            # bioinfo_dict["consensus_sequence_name"]=
            # bioinfo_dict["consensus_sequence_name_md5"]=

            bioinfo_dict["consensus_sequence_filepath"] = self.input_folder

            bioinfo_dict["consensus_sequence_software_name"] = relecov_bioinfo_metadata[
                "consensus_sequence_software_name"
            ]
            bioinfo_dict[
                "consensus_sequence_software_version"
            ] = relecov_bioinfo_metadata["consensus_sequence_software_version"]
            bioinfo_dict["if_consensus_other"] = relecov_bioinfo_metadata[
                "if_consensur_other"
            ]
            bioinfo_dict["consensus_params"] = relecov_bioinfo_metadata[
                "consensus_params"
            ]
            bioinfo_dict["depth_of_coverage_threshold"] = relecov_bioinfo_metadata[
                "depth_of_coverage_threshold"
            ]
            bioinfo_dict["depth_of_coverage_threshold"] = relecov_bioinfo_metadata[
                "depth_of_coverage_threshold"
            ]
            # "number_of_base_pairs_sequenced": "",
            # "consensus_genome_length": "",
            # "ns_per_100_kbp": "",
            # "reference_genome_accession": "",
            bioinfo_dict[
                "bioinformatics_protocol_software_name"
            ] = relecov_bioinfo_metadata["bioinformatics_protocol_software_name"]
            bioinfo_dict[
                "bioinformatics_protocol_software_version"
            ] = relecov_bioinfo_metadata["bioinformatics_protocol_software_version"]
            bioinfo_dict[
                "if_bioinformatic_protocol_is_other_specify"
            ] = relecov_bioinfo_metadata["if_bioinformatic_protocol_is_other_specify"]
            bioinfo_dict["commercial_open_source_both"] = relecov_bioinfo_metadata[
                "commercial_open_source_both"
            ]
            bioinfo_dict["preprocessing_software_name"] = relecov_bioinfo_metadata[
                "preprocessing_software_name"
            ]
            bioinfo_dict["preprocessing_software_version"] = relecov_bioinfo_metadata[
                "preprocessing_software_version"
            ]
            bioinfo_dict["if_preprocessing_other"] = relecov_bioinfo_metadata[
                "if_preprocessing_other"
            ]
            bioinfo_dict["preprocessing_params"] = relecov_bioinfo_metadata[
                "preprocessing_params"
            ]
            bioinfo_dict["mapping_software_name"] = relecov_bioinfo_metadata[
                "mapping_software_name"
            ]
            bioinfo_dict["mapping_software_version"] = relecov_bioinfo_metadata[
                "mapping_software_version"
            ]
            bioinfo_dict["if_mapping_other"] = relecov_bioinfo_metadata[
                "if_mapping_other"
            ]
            bioinfo_dict["mapping_params"] = relecov_bioinfo_metadata["mapping_params"]
            # "lineage_name": ""
            bioinfo_dict["lineage_analysis_software_name"] = relecov_bioinfo_metadata[
                "lineage_analysis_software_name"
            ]
            bioinfo_dict[
                "lineage_analysis_software_version"
            ] = relecov_bioinfo_metadata["lineage_analysis_software_version"]
            bioinfo_dict["if_lineage_identification_other"] = relecov_bioinfo_metadata[
                "if_lineage_identification_other"
            ]
            # "variant_designation": "",
            # "per_qc_filtered": "",
            # "per_reads_host": "",
            # "per_reads_virus": "",
            # "per_unmapped": "",
            # "per_genome _greater_10x": "",
            # "median_depth_of_coverage_value": "",
            # "per_Ns": "",
            # "number_of_variants_AF_greater_75percent": "",
            # "number_of_variants_with_effect": "",
            bioinfo_dict["long_table_path"] = self.input_folder

            print(bioinfo_dict)
            import pdb

            pdb.set_trace()
