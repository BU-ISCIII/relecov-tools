#!/usr/bin/env python
# from itertools import islice

import logging
import json
import datetime
import re

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
    def __init__(
        self,
        metadata_file=None,
        input_folder=None,
        output_folder=None,
        mapping_illumina=None,
    ):
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
        if mapping_illumina is None:
            self.mapping_illumina = relecov_tools.utils.prompt_path(
                msg="Select the mapping illumina file"
            )
        else:
            self.mapping_illumina = mapping_illumina

        config_json = ConfigJson()
        relecov_schema = config_json.get_topic_data("json_schemas", "relecov_schema")
        relecov_sch_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", relecov_schema
        )
        self.configuration = config_json

        with open(relecov_sch_path, "r") as fh:
            self.relecov_sch_json = json.load(fh)
        self.schema_name = self.relecov_sch_json["schema"]
        self.schema_version = self.relecov_sch_json["version"]

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
            self.input_folder, self.mapping_illumina
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

        mapping_illumina_tab = pd.read_csv(
            mapping_illumina_tab_path, sep=",", encoding="utf-8"
        )
        summary_variants_metrics = pd.read_csv(
            summary_variants_metrics_path, sep=",", encoding="utf-8"
        )
        variants_long_table = pd.read_csv(
            variants_long_table_path,
            sep=",",
            encoding="utf-8",
            dtype={"SAMPLE": "string"},
        )
        consensus_genome_length = pd.read_csv(
            consensus_genome_length_path, header=None, sep="\t", encoding="utf-8"
        )
        md5_info = pd.read_csv(md5_info_path, header=None, sep=",", encoding="utf-8")
        pangolin_version_table = pd.read_csv(
            pangolin_versions_path, header=None, sep="\t", encoding="utf-8"
        )

        with open(software_versions_path) as file:
            software_versions = yaml.load(file, Loader=yaml.FullLoader)

        self.mapping_illumina_tab_field_list = config_json.get_configuration(
            "mapping_illumina_tab_field_list"
        )
        bioinfo_list = {}

        for row in islice(ws_metadata_lab.values, 4, ws_metadata_lab.max_row):
            # row = ws_metadata_lab[5]
            sample_name = row[5]
            print(sample_name)

            fastq_r1 = row[47]

            fastq_r2 = row[48]
            bioinfo_dict = {}
            bioinfo_dict["sample_name"] = str(sample_name)
            bioinfo_dict["sequence_file_R1_fastq"] = fastq_r1
            bioinfo_dict["sequence_file_R2_fastq"] = fastq_r2
            # inserting all keys from configuration.json  relecov_bioinfo_metadata into bioinfo_dict
            for key in relecov_bioinfo_metadata.keys():
                bioinfo_dict[key] = relecov_bioinfo_metadata[key]
            bioinfo_dict["consensus_sequence_filepath"] = self.input_folder
            bioinfo_dict["long_table_path"] = self.input_folder
            # fields from mapping_illumina.tab

            mapping_illumina_tab_sample = mapping_illumina_tab[
                mapping_illumina_tab["sample"].str.contains(bioinfo_dict["sample_name"])
            ]

            # for key in self.mapping_illumina_tab_field_list.keys():
            config_keys = list(self.mapping_illumina_tab_field_list.keys())

            for i in config_keys:
                bioinfo_dict[i] = str(
                    mapping_illumina_tab_sample[
                        self.mapping_illumina_tab_field_list[i]
                    ].values[0]
                )

            # fields from summary_variants_metrics_mqc.csv

            bioinfo_dict["number_of_base_pairs_sequenced"] = str(
                summary_variants_metrics.loc[
                    summary_variants_metrics["Sample"].str.contains(
                        bioinfo_dict["sample_name"]
                    )
                ]["# Input reads"].values[0]
                * 2
            )

            bioinfo_dict["ns_per_100_kbp"] = str(
                summary_variants_metrics.loc[
                    summary_variants_metrics["Sample"].str.contains(
                        bioinfo_dict["sample_name"]
                    )
                ]["# Ns per 100kb consensus"].values[0]
            )

            bioinfo_dict["qc_filtered"] = str(
                summary_variants_metrics.loc[
                    summary_variants_metrics["Sample"].str.contains(
                        bioinfo_dict["sample_name"]
                    )
                ]["# Trimmed reads (fastp)"].values[0]
            )
            # fields from variants_long_table.csv

            if os.path.isfile(
                self.input_folder + bioinfo_dict["sample_name"] + ".consensus.fa"
            ):
                chrom = variants_long_table.loc[
                    variants_long_table["SAMPLE"].str.contains(
                        bioinfo_dict["sample_name"]
                    )
                ]["CHROM"]
                bioinfo_dict["reference_genome_accession"] = str(chrom.values[0])
            else:
                bioinfo_dict["reference_genome_accession"] = "NC_045512.2"

            # fields from consensus_genome_length
            cons_array = consensus_genome_length.loc[
                consensus_genome_length[0].str.contains(bioinfo_dict["sample_name"])
            ]
            if len(cons_array) > 1:
                for i in cons_array:
                    if cons_array.values[i, 1] != 0:
                        bioinfo_dict["consensus_genome_length"] = str(
                            cons_array.values[i, 1]
                        )
            # fields from md5 file
            bioinfo_dict["consensus_sequence_R1_name"] = str(
                md5_info.loc[
                    md5_info[0].str.contains(bioinfo_dict["sample_name"])
                ].values[0, 2]
            )
            bioinfo_dict["consensus_sequence_R2_name"] = str(
                md5_info.loc[
                    md5_info[0].str.contains(bioinfo_dict["sample_name"])
                ].values[1, 2]
            )
            bioinfo_dict["consensus_sequence_R1_md5"] = str(
                md5_info.loc[
                    md5_info[0].str.contains(bioinfo_dict["sample_name"])
                ].values[0, 1]
            )
            bioinfo_dict["consensus_sequence_R2_md5"] = str(
                md5_info.loc[
                    md5_info[0].str.contains(bioinfo_dict["sample_name"])
                ].values[1, 1]
            )

            # fields from software version file
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
            # files from pangolin.csv file

            bioinfo_dict["lineage_analysis_software_version"] = str(
                pangolin_version_table.loc[
                    pangolin_version_table[0].str.contains(bioinfo_dict["sample_name"])
                ].values[0][1]
            )
            bioinfo_dict["variant_designation"] = str(
                pangolin_version_table.loc[
                    pangolin_version_table[0].str.contains(bioinfo_dict["sample_name"])
                ].values[0][2]
            )
            """
            last_modified = os.path.getctime(variants_long_table_path)
            last_modified_date = datetime.datetime.fromtimestamp(last_modified)
            bioinfo_dict["analysis_date"] = str(last_modified_date)

            """
            # get the date from pangolin files
            string = re.split("(\d+)", self.mapping_illumina)[1]
            year, month, day = int(string[:4]), int(string[4:6]), int(string[6:-1])
            x = datetime.datetime(year, month, day)
            bioinfo_dict["analysis_date"] = x.strftime("%b %d %Y %H:%M:%S")

            pango_file_path = (
                self.input_folder + bioinfo_dict["sample_name"] + ".pangolin.csv"
            )
            if os.path.isfile(pango_file_path):
                pango_last_modified = os.path.getctime(pango_file_path)
                pango_last_modified_date = datetime.datetime.fromtimestamp(
                    pango_last_modified
                )
                bioinfo_dict["lineage_identification_date"] = str(
                    pango_last_modified_date
                )

            bioinfo_list[str(sample_name)] = bioinfo_dict
            c = c + 1
            # adding schema_name and schema_version
            bioinfo_dict["schema_name"] = self.schema_name
            bioinfo_dict["schema_version"] = self.schema_version

        json_file = "bioinfo_metadata.json"
        output_path = os.path.join(self.output_folder, json_file)

        with open(output_path, "w", encoding="utf-8") as fh:
            fh.write(
                json.dumps(bioinfo_list, indent=4, sort_keys=True, ensure_ascii=False)
            )
