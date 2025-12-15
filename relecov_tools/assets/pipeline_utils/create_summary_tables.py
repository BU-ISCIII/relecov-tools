# =============================================================
# INTRODUCTION

# This script is run in order to organise data from the RELECOV analyses according to their corresponding epidemiological weeks and seasons.
# By default, all data will be stored in a folder called surveillance_files.
# Inside, the following items are stored:
# - epidemiological_data.xlsx: an excel file containing relevant information for all the samples. This information is also aggregated in another sheet (in the case of SARS data).
# - variant_data.csv: a .csv file containing information regarding the variants identified for all the samples.
# - consensus_files: a subfolder containing all the consensus.fa files obtained after the analysis of samples.
# This data is generated separately for SARS and flu samples.

# =============================================================

import os
import json
import argparse
import shutil
import pandas as pd
from collections import defaultdict
from datetime import datetime


# Function to determine the epidemiological week associated to a certain date.
def get_epi_week(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    year, week, weekday = date.isocalendar()
    return f"{year}-{week:02d}"


# Function to get the epidemiological season in format 'YYYY_YYYY' based on the collection date.
def get_epi_season(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    year, week, weekday = date.isocalendar()
    if week >= 40:
        season_start = year
        season_end = year + 1
    else:
        season_start = year - 1
        season_end = year
    return f"{season_start}_{season_end}"


# Function to search .json files, read them, extract the relevant information and generate tables.
def process_json_files(
    input_dir=None,
    metadata_list=None,
    long_table_list=None,
    metadata_file=None,
    long_table_file=None,
    output_dir="surveillance_files",
    specified_week=None,
    copy_fasta=False,
):
    os.makedirs(output_dir, exist_ok=True)

    # Root folders depending on the organism
    sars_output_dir = os.path.join(output_dir, "SARS")
    flu_output_dir = os.path.join(output_dir, "FLU")
    os.makedirs(sars_output_dir, exist_ok=True)
    os.makedirs(flu_output_dir, exist_ok=True)

    bioinfo_files = []
    if metadata_list:
        with open(metadata_list, "r", encoding="utf-8") as f:
            bioinfo_files = [line.strip() for line in f if line.strip()]
    elif metadata_file:
        bioinfo_files = [metadata_file]
    elif input_dir:
        bioinfo_files = [
            os.path.join(input_dir, filename)
            for filename in os.listdir(input_dir)
            if filename.startswith("bioinfo_lab_metadata_")
            and filename.endswith(".json")
        ]

    long_table_files = []
    if long_table_list:
        with open(long_table_list, "r", encoding="utf-8") as f:
            long_table_files = [line.strip() for line in f if line.strip()]
    elif long_table_file:
        long_table_files = [long_table_file]
    elif input_dir:
        long_table_files = [
            os.path.join(input_dir, filename)
            for filename in os.listdir(input_dir)
            if filename.startswith("long_table_") and filename.endswith(".json")
        ]

    all_data_sars = []
    all_data_flu = []
    sample_variant_data = {}
    season_fa_files_sars = defaultdict(list)  # {season: [paths]}
    season_variants_sars = defaultdict(list)  # {season: [variant dicts]}

    # Processing of bioinfo_lab_metadata_*.json
    for filepath in bioinfo_files:
        if not os.path.exists(filepath):
            print(
                f"Warning! The file {filepath} could not be found. Please make sure the path is correct."
            )
            continue

        with open(filepath, "r", encoding="utf-8") as file:
            try:
                data = json.load(file)
                for sample in data:
                    if "sample_collection_date" in sample:
                        week = get_epi_week(sample["sample_collection_date"])
                        season = get_epi_season(sample["sample_collection_date"])
                        if specified_week and week != specified_week:
                            continue

                        analysis_date = sample.get("bioinformatics_analysis_date", "-")

                        organism = sample.get("organism", "").lower()

                        if "influenza" in organism:
                            sample_data = {
                                "COLLECTING_LAB_SAMPLE_ID": sample.get(
                                    "collecting_lab_sample_id", "-"
                                ),
                                "SAMPLE_COLLECTION_DATE": sample.get(
                                    "sample_collection_date", "-"
                                ),
                                "WEEK": week,
                                "SEASON": season,
                                "TYPE_ASSIGNMENT": sample.get("type_assignment", "-"),
                                "SUBTYPE_ASSIGNMENT": sample.get(
                                    "subtype_assignment", "-"
                                ),
                                "COVERAGE_10X": sample.get(
                                    "per_genome_greater_10x", "-"
                                ),
                                "COLLECTING_INSTITUTION": sample.get(
                                    "collecting_institution", "-"
                                ),
                                "SUBMITTING_INSTITUTION": sample.get(
                                    "submitting_institution", "-"
                                ),
                                "SUBMITTING_INSTITUTION_ID": sample.get(
                                    "submitting_institution_id", "-"
                                ),
                                "CCAA": sample.get("geo_loc_state", "-"),
                                "PROVINCE": sample.get("geo_loc_region", "-"),
                                "ANALYSIS_DATE": analysis_date,
                                "SEQUENCING_SAMPLE_ID": str(
                                    sample.get("sequencing_sample_id", "-")
                                ),
                                "MICROBIOLOGY_LAB_SAMPLE_ID": sample.get(
                                    "microbiology_lab_sample_id", "-"
                                ),
                                "UNIQUE_SAMPLE_ID": sample.get("unique_sample_id", "-"),
                                "CONSENSUS_SEQUENCE_FILENAME": sample.get(
                                    "consensus_sequence_filename", "-"
                                ),
                                "GISAID_ACCESSION_ID": sample.get(
                                    "gisaid_accession_id", "-"
                                ),
                                "QC_TEST": sample.get("qc_test", "-"),
                            }
                            all_data_flu.append(sample_data)

                        elif (
                            "severe acute respiratory syndrome coronavirus 2"
                            in organism
                        ):
                            sample_data = {
                                "COLLECTING_LAB_SAMPLE_ID": sample.get(
                                    "collecting_lab_sample_id", "-"
                                ),
                                "SAMPLE_COLLECTION_DATE": sample.get(
                                    "sample_collection_date", "-"
                                ),
                                "WEEK": week,
                                "SEASON": season,
                                "LINEAGE": sample.get("lineage_assignment", "-"),
                                "PANGOLIN_SOFTWARE_VERSION": sample.get(
                                    "lineage_assignment_software_version", "-"
                                ),
                                "PANGOLIN_DATABASE_VERSION": sample.get(
                                    "lineage_assignment_database_version", "-"
                                ),
                                "COVERAGE_10X": sample.get(
                                    "per_genome_greater_10x", "-"
                                ),
                                "COLLECTING_INSTITUTION": sample.get(
                                    "collecting_institution", "-"
                                ),
                                "SUBMITTING_INSTITUTION": sample.get(
                                    "submitting_institution", "-"
                                ),
                                "SUBMITTING_INSTITUTION_ID": sample.get(
                                    "submitting_institution_id", "-"
                                ),
                                "CCAA": sample.get("geo_loc_state", "-"),
                                "PROVINCE": sample.get("geo_loc_region", "-"),
                                "ANALYSIS_DATE": analysis_date,
                                "SEQUENCING_SAMPLE_ID": str(
                                    sample.get("sequencing_sample_id", "-")
                                ),
                                "MICROBIOLOGY_LAB_SAMPLE_ID": sample.get(
                                    "microbiology_lab_sample_id", "-"
                                ),
                                "UNIQUE_SAMPLE_ID": sample.get("unique_sample_id", "-"),
                                "CONSENSUS_SEQUENCE_FILENAME": sample.get(
                                    "consensus_sequence_filename", "-"
                                ),
                                "GISAID_ACCESSION_ID": sample.get(
                                    "gisaid_accession_id", "-"
                                ),
                                "QC_TEST": sample.get("qc_test", "-"),
                            }
                            all_data_sars.append(sample_data)

                            # Consensus files: only for SARS
                            fa_path = sample.get("consensus_sequence_filepath")
                            if copy_fasta and fa_path and os.path.exists(fa_path):
                                season_fa_files_sars[season].append(fa_path)
                        else:
                            print(
                                f"Organism '{organism}' not recognized for sample {sample.get('collecting_lab_sample_id')}"
                            )

            except json.JSONDecodeError:
                print(
                    f"Error! Could not read {filepath} properly, please make sure the file is not corrupt."
                )

    if not all_data_sars and not all_data_flu:
        print("No bioinfo_lab_metadata_*.json files were found.")
        return

    # Processing of long_table_*.json
    for filepath in long_table_files:
        if not os.path.exists(filepath):
            print(
                f"Warning! The file {filepath} could not be found. Please check the path is correct."
            )
            continue

        with open(filepath, "r", encoding="utf-8") as file:
            try:
                data = json.load(file)
                for sample in data:
                    sample_id = sample.get("sample_name")
                    if sample_id:
                        sample_variant_data[sample_id] = sample
                        # Determination of the season for each sample
                        for entry in all_data_sars:
                            if str(entry["UNIQUE_SAMPLE_ID"]) == sample_id:
                                season = entry["SEASON"]
                                variant_entries = sample.get("variants", [])
                                for variant in variant_entries:
                                    try:
                                        af = float(variant.get("af", "0"))
                                    except ValueError:
                                        af = 0.0
                                    if af > 0.75:
                                        if season not in season_variants_sars:
                                            season_variants_sars[season] = []
                                        season_variants_sars[season].append(
                                            {
                                                "SAMPLE": variant.get("sample", "-"),
                                                "CHROM": variant.get("chromosome", "-"),
                                                "POS": variant.get("pos", "-"),
                                                "ALT": variant.get("alt", "-"),
                                                "REF": variant.get("ref", "-"),
                                                "FILTER": variant.get("Filter", "-"),
                                                "DP": variant.get("dp", "-"),
                                                "REF_DP": variant.get("ref_dp", "-"),
                                                "ALT_DP": variant.get("alt_dp", "-"),
                                                "AF": variant.get("af", "-"),
                                                "GENE": variant.get("gene", "-"),
                                                "EFFECT": variant.get("effect", "-"),
                                                "HGVS_C": variant.get("hgvs_c", "-"),
                                                "HGVS_P": variant.get("hgvs_p", "-"),
                                                "HGVS_P_1LETTER": variant.get(
                                                    "hgvs_p_1_letter", "-"
                                                ),
                                                "CALLER": variant.get("caller", "-"),
                                                "LINEAGE": variant.get("lineage", "-"),
                                            }
                                        )
                                break
            except json.JSONDecodeError:
                print(
                    f"Error! Could not read {filepath} properly, please make sure the file is not corrupt."
                )

    if not sample_variant_data:
        print("No long_table_*.json files were found.")
        return

    if all_data_sars:
        df_sars = pd.DataFrame(all_data_sars)
        excel_file_sars = os.path.join(sars_output_dir, "epidemiological_data.xlsx")

        # Handle consensus files (SARS only)
        if copy_fasta:
            for season, fa_paths in season_fa_files_sars.items():
                season_dir = os.path.join(sars_output_dir, f"season_{season}")
                season_consensus_dir = os.path.join(season_dir, "consensus_files")
                os.makedirs(season_consensus_dir, exist_ok=True)
                for fa_path in fa_paths:
                    dest_path = os.path.join(
                        season_consensus_dir, os.path.basename(fa_path)
                    )
                    shutil.copy(fa_path, dest_path)
                print(
                    f"Copied {len(fa_paths)} consensus.fa files to {season_consensus_dir}"
                )

        # Handle Excel file (read existing if present) - SARS
        existing_sample_ids_sars = set()
        existing_df_sars = pd.DataFrame()

        if os.path.exists(excel_file_sars):
            with pd.ExcelFile(excel_file_sars) as reader:
                existing_df_sars = reader.parse("per_sample_data", dtype=str)
                existing_sample_ids_sars = set(existing_df_sars["SEQUENCING_SAMPLE_ID"])

        # Only add new samples - SARS
        new_samples_df_sars = df_sars[
            ~df_sars["SEQUENCING_SAMPLE_ID"].astype(str).isin(existing_sample_ids_sars)
        ]

        if new_samples_df_sars.empty:
            print("No new SARS samples found.")
        else:
            # Combine data - SARS
            combined_samples_sars = pd.concat(
                [existing_df_sars, new_samples_df_sars], ignore_index=True
            )

            # Recreate aggregated data from all samples - SARS
            combined_agg_sars = (
                combined_samples_sars.groupby("LINEAGE")
                .size()
                .reset_index(name="NUMBER_SAMPLES")
            )

            # Write to Excel file - SARS
            with pd.ExcelWriter(excel_file_sars) as writer:
                combined_samples_sars.to_excel(
                    writer, sheet_name="per_sample_data", index=False
                )
                combined_agg_sars.to_excel(
                    writer, sheet_name="aggregated_data", index=False
                )

            print(f"Excel file for SARS updated: {excel_file_sars}")

        # Write season-specific variant_data.csv files (SARS only)
        for season, variant_rows in season_variants_sars.items():
            season_dir = os.path.join(sars_output_dir, f"season_{season}")
            os.makedirs(season_dir, exist_ok=True)
            variant_csv_path = os.path.join(season_dir, "variant_data.csv")
            df_variants = pd.DataFrame(variant_rows)
            df_variants.to_csv(variant_csv_path, index=False)
            print(
                f"Written variant data for SARS season {season} to {variant_csv_path}"
            )
    else:
        print("No SARS samples found.")

    # Flu data processing
    if all_data_flu:
        df_flu = pd.DataFrame(all_data_flu)
        excel_file_flu = os.path.join(flu_output_dir, "epidemiological_data.xlsx")

        # Handle Excel file (read existing if present) - Flu
        existing_sample_ids_flu = set()
        existing_df_flu = pd.DataFrame()

        if os.path.exists(excel_file_flu):
            with pd.ExcelFile(excel_file_flu) as reader:
                existing_df_flu = reader.parse("per_sample_data", dtype=str)
                existing_sample_ids_flu = set(existing_df_flu["SEQUENCING_SAMPLE_ID"])

        # Only add new samples - Flu
        new_samples_df_flu = df_flu[
            ~df_flu["SEQUENCING_SAMPLE_ID"].astype(str).isin(existing_sample_ids_flu)
        ]

        if new_samples_df_flu.empty:
            print("No new Flu samples found.")
        else:
            # Combine data - Flu
            combined_samples_flu = pd.concat(
                [existing_df_flu, new_samples_df_flu], ignore_index=True
            )

            # Write to Excel file - Flu
            with pd.ExcelWriter(excel_file_flu) as writer:
                combined_samples_flu.to_excel(
                    writer, sheet_name="per_sample_data", index=False
                )

            print(f"Excel file for Flu updated: {excel_file_flu}")
    else:
        print("No Flu samples found.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="JSON files are processed to generate consolidated tables with lineage and variant information"
    )
    parser.add_argument(
        "-i",
        "--input",
        help="Directory containing bioinfo_lab_metadata_*.json and long_table_*.json files",
    )
    parser.add_argument(
        "-b",
        "--metadata-list",
        help="Text file with paths to bioinfo_lab_metadata_*.json files",
    )
    parser.add_argument(
        "-l",
        "--long-table-list",
        help="Text file with paths to long_table_*.json files",
    )
    parser.add_argument(
        "-m",
        "--metadata-file",
        help="Direct path to a single bioinfo_lab_metadata_*.json file",
    )
    parser.add_argument(
        "-t",
        "--long-table-file",
        help="Direct path to a single long_table_*.json file",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="surveillance_files",
        help="Output directory (default: surveillance_files)",
    )
    parser.add_argument(
        "-w",
        "--week",
        help="Filter for specific epidemiological week (format: YYYY-WW)",
    )
    parser.add_argument(
        "-c",
        "--copy-fasta",
        action="store_true",
        help="Copy consensus.fa files to consensus_files subdirectory",
    )

    args = parser.parse_args()

    if not (args.input or args.metadata_list or args.metadata_file):
        parser.error(
            "Either --input, --metadata-list, or --metadata-file must be provided"
        )
    if not (args.input or args.long_table_list or args.long_table_file):
        parser.error(
            "Either --input, --long-table-list, or --long-table-file must be provided"
        )

    process_json_files(
        args.input,
        args.metadata_list,
        args.long_table_list,
        args.metadata_file,
        args.long_table_file,
        args.output,
        args.week,
        args.copy_fasta,
    )
