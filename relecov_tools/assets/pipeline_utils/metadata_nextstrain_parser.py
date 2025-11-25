#!/usr/bin/env python

import os
import sys
import argparse
import json
import glob
import logging
from datetime import datetime

log = logging.getLogger(__name__)
timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
log_filename = f"metadata_nextstrain_parser_{timestamp}.log"

def parse_args(args=None):
    description = "Convert multiple JSON sample files to Nextstrain metadata TSV and concatenate consensus sequences."
    epilog = """Example usage: python metadata_nextstrain_parser.py --input-dir /path/to/json/files"""

    parser = argparse.ArgumentParser(description=description, epilog=epilog)
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing input JSON files (each containing an array of records)",
    )
    return parser.parse_args(args)


def extract_strain_from_filepath(filepath):
    """Extract strain name from consensus_sequence_filepath"""
    if (
        not filepath
        or filepath.startswith("Not Provided")
    ):
        return "unknown"

    # Get the filename from the path
    filename = os.path.basename(filepath)
    # Remove the .consensus.fa extension if present
    if filename.endswith(".consensus.fa"):
        return filename.replace(".consensus.fa", "")
    elif filename.endswith(".fa") or filename.endswith(".fasta"):
        return os.path.splitext(filename)[0]
    else:
        return filename


def clean_value(value):
    """Clean values by removing codes in brackets"""
    if isinstance(value, str) and " [" in value:
        return value.split(" [")[0]
    return value


def clean_gisaid_epi_isl(value):
    """Clean and unify GISAID EPI ISL values"""
    if value is None or value == "":
        return "Not Provided"

    value_str = str(value).strip()

    if value_str in ["", "NA", "No depositada", "Not Provided"]:
        return "Not Provided"

    if " [" in value_str:
        value_str = value_str.split(" [")[0]

    return value_str


def is_influenza_sample(record):
    """Check if the sample is influenza (should be excluded)"""
    organism = record.get("organism", "")
    return isinstance(organism, str) and organism.startswith("Influenza")


def read_consensus_sequence(filepath, strain_name):
    """Read consensus sequence from file and ensure proper FASTA format"""
    if (
        not filepath
        or filepath == "Not Provided [SNOMED:434941000124101]"
        or filepath.startswith("Not Provided")
    ):
        return None

    try:
        if not os.path.exists(filepath):
            message = f"Consensus file not found: {filepath}"
            log.warning(message)
            return None

        with open(filepath, "r") as f:
            content = f.read().strip()

        if not content:
            message = f"Empty consensus file: {filepath}"
            log.warning(message)
            return None

        # Ensure proper FASTA format
        lines = content.split("\n")
        if lines[0].startswith(">"):
            # File already has header, replace it with strain name
            return f">{strain_name}\n" + "\n".join(lines[1:])
        else:
            # File has no header, add one
            return f">{strain_name}\n{content}"

    except Exception as e:
        message = f"Error reading consensus file {filepath}: {e}"
        log.error(message)
        return None


def process_json_file(json_file, sequences_output_handle):
    """Process a single JSON file and write sequences to output"""
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Verify it's an array
        if not isinstance(data, list):
            message = f"{json_file} is not a JSON array. Skipping."
            log.warning(message)
            return []

        records = []
        influenza_count = 0
        sequences_written = 0

        for record in data:
            # Skip influenza samples
            if is_influenza_sample(record):
                influenza_count += 1
                continue

            # Extract strain from consensus_sequence_filepath
            consensus_path = record.get("consensus_sequence_filepath", "")
            strain = extract_strain_from_filepath(consensus_path)

            # Skip samples without valid consensus filepath
            if strain == "unknown":
                continue

            # Read and write consensus sequence
            sequence_data = read_consensus_sequence(consensus_path, strain)
            if sequence_data:
                sequences_output_handle.write(sequence_data + "\n")
                sequences_written += 1

            # Create TSV record with all required fields
            tsv_record = {
                "strain": strain,
                "virus": "ncov",
                "gisaid_epi_isl": clean_gisaid_epi_isl(
                    record.get("gisaid_accession_id", "NA")
                ),
                "pango_lineage": clean_value(record.get("lineage_assignment", "NA")),
                "date": clean_value(record.get("sample_collection_date", "NA")),
                "region": "Europe",
                "country": clean_value(record.get("geo_loc_country", "NA")),
                "division": clean_value(record.get("geo_loc_region", "NA")),
                "location": clean_value(record.get("geo_loc_city", "NA")),
                "region_exposure": "Europe",
                "country_exposure": clean_value(record.get("geo_loc_country", "NA")),
                "division_exposure": clean_value(record.get("geo_loc_region", "NA")),
                "segment": "genome",
                "length": clean_value(record.get("consensus_genome_length", "NA")),
                "host": clean_value(record.get("host_scientific_name", "NA")),
                "age": clean_value(record.get("host_age_years", "NA")),
                "sex": clean_value(record.get("host_gender", "NA")),
                "originating_lab": clean_value(
                    record.get("collecting_institution", "NA")
                ),
                "submitting_lab": clean_value(
                    record.get("submitting_institution", "NA")
                ),
                "authors": clean_value(record.get("authors", "NA")),
                "title": f"SARS-CoV-2 sequence from {clean_value(record.get('geo_loc_city', 'NA'))}, {clean_value(record.get('geo_loc_region', 'NA'))}",
                "latitude": clean_value(record.get("geo_loc_latitude", "NA")),
                "longitude": clean_value(record.get("geo_loc_longitude", "NA")),
            }
            records.append(tsv_record)

        if influenza_count > 0:
            message = f"Processed {json_file}: {len(records)} records, {sequences_written} sequences written ({influenza_count} influenza samples excluded)"
            log.info(message)
        else:
            message = f"Processed {json_file}: {len(records)} records, {sequences_written} sequences written"
            log.info(message)

        return records

    except Exception as e:
        message = f"Error processing {json_file}: {e}"
        log.error(message)
        return []


def main(args=None):
    # Logging
    logging.basicConfig(level=logging.INFO, format='[%(asctime)s] - [%(levelname)s] - %(message)s', handlers=[logging.FileHandler(log_filename), logging.StreamHandler(sys.stdout)])

    # Process args
    args = parse_args(args)

    # Define output directory
    output_dir = "nextstrain_metadata"

    # Check if output directory exists
    if not os.path.exists(output_dir):
        log.error(f"Output directory '{output_dir}' does not exist. Please create the directory first")
        sys.exit(1)

    # Get current date in YYYY-MM-DD format
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Generate output filenames with current date in the nextstrain_metadata directory
    metadata_output = os.path.join(output_dir, f"metadata_{current_date}.tsv")
    sequences_output = os.path.join(output_dir, f"sequences_{current_date}.fasta")
    lat_long_output = os.path.join(output_dir, f"metadata_lat_long_{current_date}.tsv")

    log.info(f"Output files will be generated in: {output_dir}/")
    log.info(f"  - Metadata: {os.path.basename(metadata_output)}")
    log.info(f"  - Sequences: {os.path.basename(sequences_output)}")
    log.info(f"  - Coordinates: {os.path.basename(lat_long_output)}")

    # Find all JSON files in the input directory that start with "bioinfo_lab_metadata"
    json_files = glob.glob(os.path.join(args.input_dir, "bioinfo_lab_metadata*.json"))

    if not json_files:
        log.error(f"No JSON files starting with 'bioinfo_lab_metadata' found in {args.input_dir}")
        log.info(f"Available files: {os.listdir(args.input_dir)}")
        sys.exit(1)

    log.info(f"Found {len(json_files)} JSON files starting with 'bioinfo_lab_metadata' to process")

    # Define the header
    header = [
        "strain",
        "virus",
        "gisaid_epi_isl",
        "pango_lineage",
        "date",
        "region",
        "country",
        "division",
        "location",
        "region_exposure",
        "country_exposure",
        "division_exposure",
        "segment",
        "length",
        "host",
        "age",
        "sex",
        "originating_lab",
        "submitting_lab",
        "authors",
        "title",
    ]

    # Process all JSON files and collect records
    all_records = []
    total_records = 0
    successful_files = 0
    total_influenza_excluded = 0
    total_no_consensus_excluded = 0

    # Open sequences output file once
    with open(sequences_output, "w", encoding="utf-8") as sequences_file:
        for json_file in json_files:
            records = process_json_file(json_file, sequences_file)
            if records is not None:  # records could be empty list (which is valid)
                all_records.extend(records)
                total_records += len(records)
                successful_files += 1

            # Count exclusions for summary
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                if isinstance(data, list):
                    for record in data:
                        if is_influenza_sample(record):
                            total_influenza_excluded += 1
                        elif (
                            extract_strain_from_filepath(
                                record.get("consensus_sequence_filepath", "")
                            )
                            == "unknown"
                        ):
                            total_no_consensus_excluded += 1
            except Exception as e:
                log.warning(f"Failed to count exclusions from {json_file}: {str(e)}")

    log.info(f"Successfully processed {successful_files} out of {len(json_files)} files")
    log.info(f"Total records collected: {total_records}")
    if total_influenza_excluded > 0:
        log.info(f"Influenza samples excluded: {total_influenza_excluded}")
    if total_no_consensus_excluded > 0:
        log.info(
            f"Samples without consensus filepath excluded: {total_no_consensus_excluded}"
        )

    if not all_records:
        message = "No valid records to write"
        log.error(message)
        sys.exit(1)

    # Write the global TSV file
    try:
        with open(metadata_output, "w", encoding="utf-8") as mdata:
            # Write header
            mdata.write("\t".join(header) + "\n")

            # Write all records (without latitude/longitude in the main TSV)
            for record in all_records:
                line = "\t".join(str(record.get(field, "NA")) for field in header)
                mdata.write(line + "\n")

        log.info(f"Successfully wrote {len(all_records)} records to {metadata_output}")
        log.info(f"Successfully wrote consensus sequences to {sequences_output}")

        # Generate additional lat_long file with coordinates
        with open(lat_long_output, "w", encoding="utf-8") as f_latlong:
            for record in all_records:
                # Use the actual coordinates from the JSON
                latitude = record.get("latitude", "NA")
                longitude = record.get("longitude", "NA")
                division = record.get("division", "NA")
                # Format: division[TAB]division_value[TAB]latitude[TAB]longitude
                f_latlong.write(f"division\t{division}\t{latitude}\t{longitude}\n")

        log.info(f"Generated {lat_long_output}")

        # Print summary of generated files
        log.info("\n=== SUMMARY ===")
        log.info("All output files have been generated in: " + output_dir + "/")
        log.info(f"✓ {os.path.basename(metadata_output)} - Metadata file for Nextstrain")
        log.info(
            f"✓ {os.path.basename(sequences_output)} - Concatenated consensus sequences"
        )
        log.info(f"✓ {os.path.basename(lat_long_output)} - Geographic coordinates data")

    except Exception as e:
        message = f"Error writing output file: {e}"
        log.error(message)
        sys.exit(1)


if __name__ == "__main__":
    sys.exit(main())
