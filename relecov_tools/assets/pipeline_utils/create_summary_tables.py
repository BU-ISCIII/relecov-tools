# =============================================================
# INTRODUCTION

# This script is run in order to organise data from the RELECOV analyses according to their corresponding epidemiological weeks.
# By default, all data will be stored in a folder called surveillance_files.
# Within this folder, different subfolders will be created, each one referring to a certain epidemiological week.
# Inside each subfolder, the following items are stored:
# - epidemiological_data.xlsx: an excel file containing lineage information for all the samples from a given week. This information is also aggregated in another sheet.
# - variant_data.csv: a .csv file containing information regarding the variants identified for all the samples associated to a given week.
# - consensus_files: a subfolder containing all the consensus.fa files obtained after the analysis of samples.

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
    metadata_file=None,
    long_table_file=None,
    output_dir="surveillance_files",
    specified_week=None,
    copy_fasta=False,
):
    os.makedirs(output_dir, exist_ok=True)

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

                        analysis_date = sample.get("bioinformatics_analysis_date", "-")

                        all_data.append(
                            {
                                "HOSPITAL_ID": sample.get(
                                    "submitting_institution_id", "-"
                                ),
                                "HOSPITAL": sample.get("collecting_institution", "-"),
                                "PROVINCE": sample.get("geo_loc_region", "-"),
                                "ANALYSIS_DATE": analysis_date,
                                "PANGOLIN_SOFTWARE_VERSION": sample.get(
                                    "lineage_assignment_software_version", "-"
                                ),
                                "PANGOLIN_DATABASE_VERSION": sample.get(
                                    "lineage_assignment_database_version", "-"
                                ),
                                "SAMPLE_ID": str(
                                    sample.get("sequencing_sample_id", "-")
                                ),  # str to prevent from having issues between excel and pandas with digital and characters
                                "SAMPLE_COLLECTION_DATE": sample.get(
                                    "sample_collection_date", "-"
                                ),
                                "LINEAGE": sample.get("lineage_assignment", "-"),
                                "WEEK": week,
                            }
                        )

                        # Search of consensus.fa files.
                        fa_path = sample.get("consensus_sequence_filepath")
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
        excel_file = os.path.join(week_dir, "epidemiological_data.xlsx")

        existing_sample_ids = set()
        existing_week_df = pd.DataFrame()

        if os.path.exists(excel_file):
            # Check if xlsx file exists, read data and list the SAMPLE_ID
            with pd.ExcelFile(excel_file) as reader:
                existing_week_df = reader.parse(
                    "per_sample_data", dtype=str
                )  # str ensures no mofications are done between what excel has and what the dataframe reads
                existing_sample_ids = set(existing_week_df["SAMPLE_ID"])

        # only new samples are added
        week_df = week_df[~week_df["SAMPLE_ID"].astype(str).isin(existing_sample_ids)]

        if week_df.empty:
            print(f"No new samples for week {week}. Skipping.")
            continue

        # concatenate new records to those already in the excel file
        week_df = pd.concat([existing_week_df, week_df], ignore_index=True)
        aggregated_df = (
            week_df.groupby("LINEAGE").size().reset_index(name="NUMBER_SAMPLES")
        )

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

        if os.path.exists(variant_csv):
            existing_df = pd.read_csv(variant_csv, dtype=str)
            final_df = pd.concat([existing_df, variant_df], ignore_index=True)
            final_variant_df = final_df.drop_duplicates()

        else:
            final_variant_df = variant_df

        final_variant_df.to_csv(variant_csv, index=False)
        print(f"Variant data stored in {variant_csv}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="JSON files are processed in order to asssign epidemiological weeks to each  analysed sample and to generate tables with lineage and variant information for each sample "
    )
    parser.add_argument(
        "-i",
        "--input",
        help=" Directoy containing files called bioinfo_lab_metadata_*.json and long_table_*.json (both JSON files must be in the same directory)",
    )
    parser.add_argument(
        "-b",
        "--metadata-list",
        help="File .txt with paths to those JSON files to be processed for generating the table with information on aggregated and non-aggregated data (bioinfo_lab_metadata_*.json)",
    )
    parser.add_argument(
        "-l",
        "--long-table-list",
        help="File .txt with paths to those JSON files to be processed for generating the variant report in csv format (long_table_*.json)",
    )
    parser.add_argument(
        "-m",
        "--metadata-file",
        help="Direct path to a single bioinfo_lab_metadata_*.json file to process and to add to the tables if they already exist",
    )
    parser.add_argument(
        "-t",
        "--long-table-file",
        help="Direct path to a single long_table_*.json file to process and to add to the tables if they already exist",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="surveillance_files",
        help="Directory where the generated tables are saved (surveillance_files, by default)",
    )
    parser.add_argument(
        "-w",
        "--week",
        help="Epidemiological week of intestest (format: YYYY-WW)",
    )
    parser.add_argument(
        "-c",
        "--copy-fasta",
        action="store_true",
        help="Copy files consensus.fa in a subdirectory called consensus_files (this argument must be provided to enable the feature)",
    )

    args = parser.parse_args()

    # Validate that either input directory or file lists or individual files are provided
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
