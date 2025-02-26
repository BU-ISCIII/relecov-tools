#!/usr/bin/env python
import os
import sys
import argparse

# import yml
from relecov_tools.download_manager import DownloadManager
from relecov_tools.dataprocess_wrapper import ProcessWrapper


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-d",
        "--download_option",
        type=str,
        help="Download option",
    )
    parser.add_argument("-t", "--target_folders", type=str, help="Target folders")
    args = parser.parse_args()

    val_dict = {
        "user": os.environ["TEST_USER"],
        "password": os.environ["TEST_PASSWORD"],
        "download_option": args.download_option,
        "output_location": os.environ["OUTPUT_LOCATION"],
        "target_folders": args.target_folders,
    }
    prepare_remote_test(**val_dict)


# def generate_config_yaml(user, password, download_option, target_folders):
#     """Generate the wrapper_config.yaml file with the desired structure."""
#     config_data = {
#         "download": {
#             "user": user,
#             "passwd": password,
#             "download_option": download_option,
#         },
#         "read-lab-metadata": {
#             "metadata_file": "tests/data/read_lab_metadata/metadata_lab_test.xlsx",
#             "sample_list_file": "tests/data/read_lab_metadata/samples_data_test.json",
#         },
#         "validate": {
#             "json_schema_file": "relecov_tools/schema/relecov_schema.json",
#         },
#     }

#     with open("wrapper_config.yaml", "w") as file:
#         yaml.dump(config_data, file, default_flow_style=False)

#     return "wrapper_config.yaml"


def prepare_remote_test(**kwargs):
    # First clean the repository.
    print("Initating sftp module")

    download_manager = DownloadManager(
        user=kwargs["user"],
        passwd=kwargs["password"],
        conf_file=None,
        download_option=kwargs["download_option"],
        output_location=kwargs["output_location"],
        target_folders=kwargs["target_folders"],
    )
    print("Openning connection to sftp")
    download_manager.relecov_sftp.sftp_port = os.environ["TEST_PORT"]
    if not download_manager.relecov_sftp.open_connection():
        print("Could not open connection to remote sftp")
        sys.exit(1)
    remote_folders = download_manager.relecov_sftp.list_remote_folders(
        ".", recursive=True
    )
    print("Connection opened with sftp")
    clean_folders = [folder.replace("./", "") for folder in remote_folders]
    print("Cleaning folders")
    for folder in clean_folders:
        if len(folder.split("/")) < 2:
            continue
        filelist = download_manager.relecov_sftp.get_file_list(folder)
        for file in filelist:
            download_manager.relecov_sftp.remove_file(file)
        print(f"Removing remote folder {folder}")
        download_manager.relecov_sftp.remove_dir(folder)

    # Upload the test dataset to the sftp.
    data_loc = "tests/data/sftp_handle"
    folder_files_dict = {folder: files for folder, _, files in os.walk(data_loc)}
    print("Uploading files to sftp...")
    for folder, files in folder_files_dict.items():
        if "datatest" in folder:
            remote_dir = "COD-test-1"
        elif "empty_test" in folder:
            remote_dir = "COD-test-2"
        else:
            continue
        base_folder = folder.split("/")[-1]
        download_manager.relecov_sftp.make_dir(os.path.join(remote_dir, base_folder))
        print(f"Uploading files from {base_folder}")
        for file in files:
            remote_path = os.path.join(remote_dir, base_folder, file)
            local_path = os.path.join(os.path.abspath(folder), file)
            download_manager.relecov_sftp.upload_file(local_path, remote_path)

    download_manager.relecov_sftp.close_connection()

    # print("Initiating wrapper configuration")
    # conf_file = generate_config_yaml(
    #     kwargs["user"],
    #     kwargs["password"],
    #     kwargs["download_option"],
    # )

    print("Initiating Wrapper")
    wrapper_manager = ProcessWrapper(
        config_file="wrapper_config.yaml",
        output_folder=kwargs["output_location"],
    )

    wrapper_manager.config_data["download"]["user"] = kwargs["user"]
    wrapper_manager.config_data["download"]["passwd"] = kwargs["password"]
    wrapper_manager.config_data["download"]["download_option"] = kwargs[
        "download_option"
    ]
    wrapper_manager.config_data["download"]["output_location"] = kwargs[
        "output_location"
    ]

    wrapper_manager.config_data["read-lab-metadata"][
        "metadata_file"
    ] = "tests/data/read_lab_metadata/metadata_lab_test.xlsx"
    wrapper_manager.config_data["read-lab-metadata"][
        "sample_list_file"
    ] = "tests/data/read_lab_metadata/samples_data_test.json"

    wrapper_manager.config_data["validate"][
        "json_schema_file"
    ] = "relecov_tools/schema/relecov_schema.json"

    wrapper_manager.download_params = wrapper_manager.clean_module_params(
        "DownloadManager", wrapper_manager.config_data["download"]
    )
    wrapper_manager.readmeta_params = wrapper_manager.clean_module_params(
        "RelecovMetadata", wrapper_manager.config_data["read-lab-metadata"]
    )
    wrapper_manager.validate_params = wrapper_manager.clean_module_params(
        "SchemaValidation", wrapper_manager.config_data["validate"]
    )

    def test_wrapper(wrapper_manager):
        wrapper_manager.run_wrapper()

    test_wrapper(wrapper_manager)


if __name__ == "__main__":
    main()
