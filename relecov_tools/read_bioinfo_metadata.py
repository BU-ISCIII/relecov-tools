#!/usr/bin/env python
# from itertools import islice

import logging
import json
import datetime

# import os


import rich.console
from itertools import islice
import pandas as pd
import yaml

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
            self.input_folder = input_folder
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
        c = 0
        self.files_read_bioinfo_metadata = config_json.get_configuration(
            "files_read_bioinfo_metadata"
        )

        mapping_illumina_tab_path = os.path.join(
            self.input_folder, "mapping_illumina.tab"
        )
        summary_variants_metrics_path = os.path.join(
            self.input_folder, "summary_variants_metrics_mqc.csv"
        )
        variants_long_table_path = os.path.join(
            self.input_folder, "variants_long_table.csv"
        )
        consensus_genome_length_path = os.path.join(
            self.input_folder, "consensus_genome_length.csv"
        )
        software_versions_path = os.path.join(
            self.input_folder, "software_versions.yml"
        )
        pangolin_versions_path = os.path.join(self.input_folder, "pangolin_version.csv")
        self.md5_file_name = config_json.get_configuration("md5_file_name")
        md5_info_path = os.path.join(
            self.input_folder,
            self.md5_file_name,  # como hacer esto general para los servicios
        )

        mapping_illumina_tab = pd.read_csv(mapping_illumina_tab_path, sep="\t")
        summary_variants_metrics = pd.read_csv(summary_variants_metrics_path, sep=",")
        variants_long_table = pd.read_csv(variants_long_table_path, sep=",")
        consensus_genome_length = pd.read_csv(
            consensus_genome_length_path, header=None, sep=","
        )
        md5_info = pd.read_csv(md5_info_path, header=None, sep=",")
        pangolin_version_table = pd.read_csv(
            pangolin_versions_path, header=None, sep="\t"
        )
        pangolin_version_software = pangolin_version_table[1]

        with open(software_versions_path) as file:
            software_versions = yaml.load(file, Loader=yaml.FullLoader)

        self.mapping_illumina_tab_field_list = config_json.get_configuration(
            "mapping_illumina_tab_field_list"
        )
        bioinfo_list = {}

        for row in islice(ws_metadata_lab.values, 4, ws_metadata_lab.max_row):
            # row = ws_metadata_lab[5]
            sample_name = row[5]

            fastq_r1 = row[47]
            fastq_r2 = row[48]
            bioinfo_dict = {}
            bioinfo_dict["sample_name"] = str(sample_name)
            bioinfo_dict["fastq_r1"] = fastq_r1
            bioinfo_dict["fastq_r2"] = fastq_r2

            # inserting all keys from configuration.json  relecov_bioinfo_metadata into bioinfo_dict
            for key in relecov_bioinfo_metadata.keys():
                bioinfo_dict[key] = relecov_bioinfo_metadata[key]
            bioinfo_dict["consensus_sequence_filepath"] = self.input_folder
            bioinfo_dict["long_table_path"] = self.input_folder

            # fields from mapping_illumina.tab

            for key in self.mapping_illumina_tab_field_list.keys():

                bioinfo_dict[key] = str(
                    mapping_illumina_tab[self.mapping_illumina_tab_field_list[key]][c]
                )

            # fields from summary_variants_metrics_mqc.csv
            bioinfo_dict["number_of_base_pairs_sequenced"] = str(
                (summary_variants_metrics["# Input reads"][c] * 2)
            )
            bioinfo_dict["ns_per_100_kbp"] = str(
                summary_variants_metrics["# Ns per 100kb consensus"][c]
            )
            bioinfo_dict["qc_filtered"] = str(
                summary_variants_metrics["# Trimmed reads (fastp)"][c]
            )

            # fields from variants_long_table.csv
            bioinfo_dict["reference_genome_accession"] = str(
                variants_long_table["CHROM"][c]
            )
            bioinfo_dict["consensus_genome_length"] = str(
                consensus_genome_length.iloc[c, 0]
            )
            bioinfo_dict["consensus_sequence_R1_name"] = str(
                md5_info.iloc[c * 2, 0][34:60]
            )
            bioinfo_dict["consensus_sequence_R2_name"] = str(
                md5_info.iloc[c * 2 + 1, 0][34:60]
            )
            bioinfo_dict["consensus_sequence_R1_md5"] = str(
                md5_info.iloc[c * 2, 0][0:32]
            )
            bioinfo_dict["consensus_sequence_R2_md5"] = str(
                md5_info.iloc[c * 2 + 1, 0][0:32]
            )

            bioinfo_dict["dehosting_method_software_version"] = str(
                list(software_versions["KRAKEN2_KRAKEN2"].values())[0]
            )
            bioinfo_dict["variant_calling_software_version"] = str(
                list(software_versions["IVAR_VARIANTS"].values())[0]
            )
            bioinfo_dict["consensus_sequence_software_version"] = str(
                list(software_versions["BCFTOOLS_CONSENSUS"].values())[0]
            )

            bioinfo_dict["bioinformatics_protocol_software_version"] = str(
                software_versions["Workflow"]["nf-core/viralrecon"]
            )

            bioinfo_dict["preprocessing_software_version"] = str(
                list(software_versions["FASTP"].values())[0]
            )
            bioinfo_dict["mapping_software_version"] = str(
                list(software_versions["BOWTIE2_ALIGN"].values())[0]
            )

            bioinfo_dict["lineage_analysis_software_version"] = str(
                pangolin_version_software.iloc[c]
            )

            c_time = os.path.getctime(variants_long_table_path)
            dt_c = datetime.datetime.fromtimestamp(c_time)
            bioinfo_dict["analysis_date"] = str(dt_c)
            bioinfo_dict["lineage_identification_date"] = str(dt_c)

            """
            IDEA
            pango_time = os.path.getctime(
                self.input_folder + "/" + str(sample_name) + ".pangolin.csv"
            )
            dt_pango = datetime.datetime.fromtimestamp(pango_time)
            """
            bioinfo_list[str(sample_name)] = bioinfo_dict
            c = c + 1

        json_file = "bioinfo_metadata.json"
        output_path = os.path.join(self.output_folder, json_file)

        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(
                json.dumps(bioinfo_list, indent=4, sort_keys=True, ensure_ascii=False)
            )
