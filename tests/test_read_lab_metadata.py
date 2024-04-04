#!/usr/bin/env python
import json
import glob
import argparse
from relecov_tools.read_lab_metadata import RelecovMetadata


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--metadata_file",
        type=str,
        help="Metadata file",
    )
    parser.add_argument("-s", "--sample_list_file", type=str, help="Samples data file")
    args = parser.parse_args()
    metadata_file = args.metadata_file
    sample_list_file = args.sample_list_file
    exec_read_lab = RelecovMetadata(
        metadata_file=metadata_file,
        sample_list_file=sample_list_file,
        output_folder="tests/output/",
    )
    exec_read_lab.create_metadata_json()
    assert_results()


def assert_results():
    processed_metadata = glob.glob("tests/output/processed_metadata_lab*.json")
    processed_metadata_content = json.loads(processed_metadata[0])
    orig_processed_content = json.loads("tests/data/processed_metadata.json")
    assert processed_metadata_content == orig_processed_content
