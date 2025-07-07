#!/usr/bin/env python
import os
import sys
import argparse
import yaml
import json
from relecov_tools.validate import Validate
from relecov_tools.download import Download
from relecov_tools.config_json import ConfigJson


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-j",
        "--json_file",
        type=str,
        required=True,
        help="Path to the input JSON file containing processed metadata.",
    )
    parser.add_argument(
        "-m",
        "--metadata",
        type=str,
        required=True,
        help="Path to the metadata Excel file.",
    )
    parser.add_argument(
        "--upload_files",
        action="store_true",
        help="If set, upload invalid files to the SFTP server.",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        required=True,
        help="Directory where output files will be written.",
    )
    parser.add_argument(
        "-s",
        "--json_schema_file",
        type=str,
        required=True,
        help="Path to the JSON schema file used for validation.",
    )
    parser.add_argument(
        "-r",
        "--registry",
        type=str,
        required=True,
        help="Path to the unique sample ID registry JSON file.",
    )
    parser.add_argument(
        "-l",
        "--logsum_file",
        type=str,
        required=True,
        help="Path to the previous process's log summary JSON file.",
    )
    args = parser.parse_args()
    print("Creating extra_config yaml")
    conf_file = generate_config_yaml()
    config_json = ConfigJson()
    config_json.include_extra_config(conf_file, config_name=None, force=True)
    print("Fixing filepaths in json file")
    update_json_filepaths(args.json_file)
    print("Initiating validate module")
    validation = Validate(**vars(args))
    validation.user = os.environ["TEST_USER"]
    validation.password = os.environ["TEST_PASSWORD"]
    validation.sftp_port = os.environ["TEST_PORT"]
    validation.execute_validation_process()
    invalid_sftp_folder = validation.remote_outfold.replace("./", "")
    clean_remote_test(invalid_sftp_folder)


def generate_config_yaml():
    """Generate the wrapper_config.yaml file with the desired structure."""
    config_data = {
        "download": {
            "user": "",
            "password": "",
            "conf_file": "",
            "output_dir": "",
            "download_option": "download_clean",
            "subfolder": "RELECOV",
        },
        "validate": {
            "json_file": "",
            "metadata": "",
            "output_dir": "",
            "excel_sheet": "",
            "json_schema_file": "relecov_tools/schema/relecov_schema.json",
            "registry": "tests/data/map_validate/unique_sampleid_registry.json",
        },
    }

    with open("extra_config.yaml", "w") as file:
        yaml.dump(config_data, file, default_flow_style=False)

    return "extra_config.yaml"


def clean_remote_test(invalid_sftp_folder):
    # First clean the repository.
    print("Initating sftp module")

    download = Download(
        user=os.environ["TEST_USER"],
        password=os.environ["TEST_PASSWORD"],
        download_option="download_only",
        output_dir="tests/data/map_validate/",
        subfolder="RELECOV",
    )
    print("Openning connection to sftp")
    download.relecov_sftp.sftp_port = os.environ["TEST_PORT"]
    if not download.relecov_sftp.open_connection():
        print("Could not open connection to remote sftp")
        sys.exit(1)

    print("Cleaning folders inside RELECOV")
    base_paths = ["COD-test-1/RELECOV", "COD-test-2/RELECOV"]
    for base_path in base_paths:
        remote_folders = download.relecov_sftp.list_remote_folders(
            base_path, recursive=True
        )
        remote_folders = sorted(remote_folders, reverse=True)
        for folder in remote_folders:
            folder = folder.replace("./", "")
            if folder == base_path or invalid_sftp_folder not in folder:
                continue

            filelist = download.relecov_sftp.get_file_list(folder)
            for file in filelist:
                print(f"Removing file {file} in {folder}")
                download.relecov_sftp.remove_file(file)

            if "tmp_processing" in folder or "invalid_samples" in folder:
                try:
                    print(f"Removing special folder {folder}")
                    download.relecov_sftp.remove_dir(folder)
                except Exception as e:
                    print(f"Could not remove folder {folder}: {e}")

        filelist_base = download.relecov_sftp.get_file_list(base_path)
        for file in filelist_base:
            print(f"Removing file {file} in {base_path}")
            download.relecov_sftp.remove_file(file)
    return


def update_json_filepaths(json_file):
    with open(json_file, "r") as fh:
        try:
            json_data = json.load(fh)
        except (UnicodeDecodeError, ValueError):
            raise
    updating_fields = ["sequence_file_path_R1", "sequence_file_path_R2"]
    for sample in json_data:
        for field in updating_fields:
            if field not in sample:
                continue
            sample[field] = os.path.join(os.environ["GITHUB_WORKSPACE"], sample[field])
    with open(json_file, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(json_data, indent=4, sort_keys=True, ensure_ascii=False))


if __name__ == "__main__":
    main()
