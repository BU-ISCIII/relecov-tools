#!/usr/bin/env python
# from itertools import islice


from importlib.resources import path
import logging


import rich.console
from itertools import islice
import pandas as pd


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
        c = 0
        mapping_illumina_tab_path = os.path.join(
            self.input_folder, "mapping_illumina.tab"
        )
        summary_variants_metrics_path = os.path.join(
            self.input_folder, "summary_variants_metrics_mqc.csv"
        )
        variants_long_table_path = os.path.join(
            self.input_folder, "variants_long_table.csv"
        )
        mapping_illumina_tab = pd.read_csv(mapping_illumina_tab_path, sep="\t")
        summary_variants_metrics = pd.read_csv(summary_variants_metrics_path, sep=",")
        variants_long_table = pd.read_csv(variants_long_table_path, sep=",")
        for row in islice(ws_metadata_lab.values, 4, ws_metadata_lab.max_row):
            # row = ws_metadata_lab[5]
            sample_name = row[5]
            fastq_r1 = row[47]
            fastq_r2 = row[48]
            bioinfo_dict = {}
            bioinfo_dict["sample_name"] = sample_name
            bioinfo_dict["fastq_r1"] = fastq_r1
            bioinfo_dict["fastq_r2"] = fastq_r2
            # inserting all keys from configuration.json  relecov_bioinfo_metadata into bioinfo_dict
            for key in relecov_bioinfo_metadata.keys():
                bioinfo_dict[key] = relecov_bioinfo_metadata[key]
            bioinfo_dict["consensus_sequence_filepath"] = self.input_folder
            bioinfo_dict["long_table_path"] = self.input_folder
            # fields from mapping_illumina.tab
            bioinfo_dict["linage_name"] = mapping_illumina_tab["Lineage"][c]
            bioinfo_dict["variant_designation"] = mapping_illumina_tab[
                "Variantsinconsensusx10"
            ][c]
            bioinfo_dict["per_qc_filtered"] = mapping_illumina_tab["Coverage>10x(%)"][c]
            # bioinfo_dict["per_reads_host"] = mapping_illumina_tab["%readshost"][c]
            # bioinfo_dict["per_reads_virus"] = mapping_illumina_tab["%readsvirus"][c]
            # bioinfo_dict["per_unmapped"] = mapping_illumina_tab["%unmapedreads"][c]
            bioinfo_dict["per_Ns"] = mapping_illumina_tab["%Ns10x"][c]
            bioinfo_dict["median_depth_of_coverage_value"] = mapping_illumina_tab[
                "medianDPcoveragevirus"
            ][c]
            bioinfo_dict[
                "number_of_variants_AF_greater_75percent"
            ] = mapping_illumina_tab["Variantsinconsensusx10"][c]
            bioinfo_dict["number_of_variants_with_effect"] = mapping_illumina_tab[
                "MissenseVariants"
            ][c]
            # fields from summary_variants_metrics_mqc.csv
            bioinfo_dict["number_of_base_pairs_sequenced"] = (
                summary_variants_metrics["# Input reads"][c] * 2
            )  # REVISAR SI ES ASÍ CON SARA
            bioinfo_dict["ns_per_100_kbp"] = summary_variants_metrics[
                "# Ns per 100kb consensus"
            ][c]
            # FALTA "consensus_genome_length": "", script que cuente el numero de nucleotidos tamaño del fasta
            # fields from variants_long_table.csv
            bioinfo_dict["reference_genome_accession"] = variants_long_table["CHROM"][c]
            bioinfo_dict["consensus_sequence_name"] = str(sample_name).join(
                ".consensus.fa"
            )
            c = +1
            import pdb

            pdb.set_trace()

        """                                                                                                                         
            f = open(path_illumina_tab, "r")
        lines = f.readlines()
        lineages = []
        lineage_index = lines[0].index("Lineage")
        for line in range(1, len(lines)):
            
            line_split = lines.split("\t")
            lineages.append(line_split[lineage_index])
        # "dehosting_method_software_version" # NO HARCODED
        # "variant_calling_software_version" # NO HARCODED
        # "consensus_sequence_software_version" # NO HARCODED
        # "bioinformatics_protocol_software_version" # NO HARCODED
        # "preprocessing_software_version"# NO HARCODED
        # "mapping_software_version" # NO HARCODED
        # bioinfo_dict["consensus_sequence_name"]=
        # bioinfo_dict["consensus_sequence_name_md5"]=
        """
