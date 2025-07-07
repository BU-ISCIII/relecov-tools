#!/usr/bin/env python
import os
import sys
import argparse
from relecov_tools.download import Download


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
        "output_dir": os.environ["OUTPUT_LOCATION"],
        "target_folders": args.target_folders,
    }
    prepare_remote_test(**val_dict)


def prepare_remote_test(**kwargs):
    print("Initiating sftp module")
    download = Download(
        user=kwargs["user"],
        password=kwargs["password"],
        conf_file=None,
        download_option=kwargs["download_option"],
        output_dir=kwargs["output_dir"],
        target_folders=kwargs["target_folders"],
        subfolder="RELECOV",
    )

    print("Opening connection to sftp")
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
            if folder == base_path:
                continue

            filelist = download.relecov_sftp.get_file_list(folder)
            for file in filelist:
                print(f"Removing file {file}")
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

    data_loc = "tests/data/sftp_handle"
    folder_files_dict = {folder: files for folder, _, files in os.walk(data_loc)}
    print("Uploading files to sftp...")
    for folder, files in folder_files_dict.items():
        if "datatest" in folder:
            remote_dir = "COD-test-1/RELECOV"
        elif "empty_test" in folder:
            remote_dir = "COD-test-2/RELECOV"
        else:
            continue
        print(f"Uploading files from {folder}")
        for file in files:
            remote_path = os.path.join(remote_dir, file)
            local_path = os.path.join(os.path.abspath(folder), file)
            download.relecov_sftp.upload_file(local_path, remote_path)

    download.relecov_sftp.close_connection()

    def test_download(download):
        download.execute_process()

    test_download(download)


if __name__ == "__main__":
    main()
