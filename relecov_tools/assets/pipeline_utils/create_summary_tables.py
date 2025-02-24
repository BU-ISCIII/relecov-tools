# =============================================================
# INTRODUCTION

# This script is run in order to organise data from the RELECOV analyses according to their corresponding epidemiological weeks.
# By default, all data will be stored in a folder called surveillance_files.
# Within this folder, different subfolders will be created, each one referring to a certain epidemiological week.
# Inside each subfolder, the following items are stored:
## - epidemiological_data.xlsx: an excel file containing lineage information for all the samples from a given week. This information is also aggregated in another sheet.
## - variant_data.csv: a .csv file containing information regarding the variants identified for all the samples associated to a given week.
## - consensus_files: a subfolder containing all the consensus.fa files obtained after the analysis of samples.

# =============================================================

# =============================================================
# EXAMPLES OF USE

# This script processes bioinfo_lab_metadata*.json and long_table_*.json files.
# This script can either read these files if they are all stored within the same location, or read .txt files which indicate the paths to these files.

# Use the -i option to indicate the path where these files are.
## Example: python3 create_summary_tables.py -i ./path

# If your files are located in different locations, use the -b and -l options to indicate the names of the .txt files that must contain the paths to the .json files.
## Example: python3 create_summary_tables.py -b bioinfo_files.txt -l long_table_files.txt
## Example of what .txt files look like (considering this script is being run from /data/bioinfoshare/UCCT_Relecov):
### COD-2402-AND-HUCSC/20240604104459/long_table_20241119092541.json
### COD-2402-AND-HUCSC/20240911160822/long_table_20241118182618.json
### COD-2403-CAT-HUVH/20240409103006/long_table_20240912110739.json

# If you want to copy the consensus.fa files into each subfolder, write the -c or --copy-fasta option when running the script.
## Example: python3 create_summary_tables.py -b bioinfo_files.txt -l long_table_files.txt -c

# If you want to generate data only in relation to a certain epidemiological week, use the -w option (using the YYYY-WW format).
## Example: python3 create_summary_tables.py -b bioinfo_files.txt -l long_table_files.txt -w 2025-01

# =============================================================

import os
import json
import argparse
import shutil
import pandas as pd
from datetime import datetime

# Function to determine the epidemiological week associated to a certain date.
def get_epi_week(date_str):
    date = datetime.strptime(date_str, "%Y-%m-%d")
    year, week, weekday = date.isocalendar()
    return f"{year}-{week:02d}"


# Function to search .json files in the paths indicated in the provided .txt files (specifically, bioinfo_lab_metadata and long_table .json files), read them, extract the relevant information and generate tables.
def process_json_files(
    input_dir=None,
    metadata_list=None,
    long_table_list=None,
    output_dir="surveillance_files",
    specified_week=None,
    copy_fasta=False,
):
    os.makedirs(output_dir, exist_ok=True)

    bioinfo_files = []
    if metadata_list:
        with open(metadata_list, "r", encoding="utf-8") as f:
            bioinfo_files = [line.strip() for line in f if line.strip()]
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
    elif input_dir:
        long_table_files = [
            os.path.join(input_dir, filename)
            for filename in os.listdir(input_dir)
            if filename.startswith("long_table_") and filename.endswith(".json")
        ]

    all_data = []
    fa_files = []
    sample_variant_data = {}

    # Processing of bioinfo_lab_metadata_*.json.
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
                        if specified_week and week != specified_week:
                            continue
                        all_data.append(
                            {
                                "HOSPITAL_ID": sample.get(
                                    "submitting_institution_id", "-"
                                ),
                                "HOSPITAL": sample.get("collecting_institution", "-"),
                                "PROVINCE": sample.get("geo_loc_region", "-"),
                                "SAMPLE_ID": sample.get("sequencing_sample_id", "-"),
                                "SAMPLE_COLLECTION_DATE": sample.get(
                                    "sample_collection_date", "-"
                                ),
                                "LINEAGE": sample.get("lineage_name", "-"),
                                "WEEK": week,
                            }
                        )

                        # Search of consensus.fa files.
                        fa_path = sample.get("viralrecon_filepath_mapping_consensus")
                        if copy_fasta and fa_path and os.path.exists(fa_path):
                            fa_files.append((fa_path, week))

            except json.JSONDecodeError:
                print(
                    f"Error! Could not read {filepath} properly, please make sure the file is not corrupt."
                )

    if not all_data:
        print("No bioinfo_lab_metadata_*.json files were found.")
        return

    # Processing of long_table_*.json.
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
            except json.JSONDecodeError:
                print(
                    f"Error! Could not read {filepath} properly, please make sure the file is not corrupt."
                )

    if not sample_variant_data:
        print("No long_table_*.json files were found.")
        return

    df = pd.DataFrame(all_data)
    weeks = df["WEEK"].unique()

    # Creation of epidemiological-week folders, the .xlsx file with lineage information per week and the .csv file with the variant information per week.
    for week in weeks:
        week_dir = os.path.join(output_dir, week)
        os.makedirs(week_dir, exist_ok=True)

        week_df = df[df["WEEK"] == week]
        aggregated_df = (
            week_df.groupby("LINEAGE").size().reset_index(name="NUMBER_SAMPLES")
        )

        excel_file = os.path.join(week_dir, "epidemiological_data.xlsx")
        with pd.ExcelWriter(excel_file) as writer:
            week_df.to_excel(writer, sheet_name="per_sample_data", index=False)
            aggregated_df.to_excel(writer, sheet_name="aggregated_data", index=False)

        print(f"Tables were stored in {week_dir}")

        # Copy of the consensus.fa files into a subfolder called consensus_files.
        if copy_fasta:
            consensus_dir = os.path.join(week_dir, "consensus_files")
            os.makedirs(consensus_dir, exist_ok=True)
            for fa_path, week_fa in fa_files:
                if week_fa == week:
                    dest_path = os.path.join(consensus_dir, os.path.basename(fa_path))
                    shutil.copy(fa_path, dest_path)
            print("Copy of consensus.fa files completed successfully")

        # Generation of the .csv files with variant data.
        variant_data = []
        for _, row in week_df.iterrows():
            sample_id = row["SAMPLE_ID"]
            if sample_id in sample_variant_data:
                variant_entries = sample_variant_data[sample_id].get("variants", [])
                for variant in variant_entries:
                    variant_data.append(
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
                            "HGVS_P_1LETTER": variant.get("hgvs_p_1_letter", "-"),
                            "CALLER": variant.get("caller", "-"),
                            "LINEAGE": variant.get("lineage", "-"),
                        }
                    )

        if variant_data:
            variant_df = pd.DataFrame(variant_data)
            variant_csv = os.path.join(week_dir, "variant_data.csv")
            variant_df.to_csv(variant_csv, index=False)
            print(f"Variant data stored in {variant_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="JSON files are processed in order to generate lineage and variant tables in relation to all samples associated to a given epidemiological week"
    )
    parser.add_argument(
        "-i",
        "--input",
        help="Directory that contains bioinfo_lab_metadata_*.json and long_table_*.json files (they all must be stored within the same directory)",
    )
    parser.add_argument(
        "-b",
        "--metadata-list",
        help=".txt file with paths pointing to the JSON files needed to create the .xlsx file for lineage data (bioinfo_lab_metadata_*.json)",
    )
    parser.add_argument(
        "-l",
        "--long-table-list",
        help=".txt file with paths pointing to the JSON files needed to create the .csv file for variant information (long_table_*.json)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="surveillance_files",
        help="Directory where tables are stored (surveillance_files by default)",
    )
    parser.add_argument(
        "-w", "--week", help="Epidemiological week of interest (use the YYYY-WW format)"
    )
    parser.add_argument(
        "-c",
        "--copy-fasta",
        action="store_true",
        help="Copy of all consensus.fa files into a subfolder called consensus_files (you must explicitly call this option)",
    )

    args = parser.parse_args()
    process_json_files(
        args.input,
        args.metadata_list,
        args.long_table_list,
        args.output,
        args.week,
        args.copy_fasta,
    )
