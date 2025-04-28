#!/usr/bin/env python
import os
import sys
import argparse
from relecov_tools.download_manager import DownloadManager


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


def prepare_remote_test(**kwargs):
    print("Initiating sftp module")
    download_manager = DownloadManager(
        user=kwargs["user"],
        passwd=kwargs["password"],
        conf_file=None,
        download_option=kwargs["download_option"],
        output_location=kwargs["output_location"],
        target_folders=kwargs["target_folders"],
    )

    print("Opening connection to sftp")
    download_manager.relecov_sftp.sftp_port = os.environ["TEST_PORT"]
    if not download_manager.relecov_sftp.open_connection():
        print("Could not open connection to remote sftp")
        sys.exit(1)

    print("Cleaning folders inside RELECOV")
    base_paths = ["COD-test-1/RELECOV", "COD-test-2/RELECOV"]
    for base_path in base_paths:
        remote_folders = download_manager.relecov_sftp.list_remote_folders(
            base_path, recursive=True
        )
        remote_folders = sorted(remote_folders, reverse=True)
        for folder in remote_folders:
            folder = folder.replace("./", "")
            if folder == base_path:
                continue

            filelist = download_manager.relecov_sftp.get_file_list(folder)
            for file in filelist:
                print(f"Removing file {file}")
                download_manager.relecov_sftp.remove_file(file)

            if "tmp_processing" in folder or "invalid_samples" in folder:
                try:
                    print(f"Removing special folder {folder}")
                    download_manager.relecov_sftp.remove_dir(folder)
                except Exception as e:
                    print(f"Could not remove folder {folder}: {e}")

        filelist_base = download_manager.relecov_sftp.get_file_list(base_path)
        for file in filelist_base:
            print(f"Removing file {file} in {base_path}")
            download_manager.relecov_sftp.remove_file(file)

    data_loc = "tests/data/sftp_handle"
    folder_files_dict = {folder: files for folder, _, files in os.walk(data_loc)}
    print("Uploading files to sftp...")
    uploaded_folders = []
    for folder, files in folder_files_dict.items():
        if "datatest" in folder:
            remote_dir = "COD-test-1/RELECOV"
        elif "empty_test" in folder:
            remote_dir = "COD-test-2/RELECOV"
        else:
            continue
        base_folder = folder.split("/")[-1]
        remote_folder_path = os.path.join(remote_dir, base_folder)
        download_manager.relecov_sftp.make_dir(remote_folder_path)
        uploaded_folders.append(remote_folder_path)
        print(f"Uploading files from {base_folder}")
        for file in files:
            remote_path = os.path.join(remote_folder_path, file)
            local_path = os.path.join(os.path.abspath(folder), file)
            download_manager.relecov_sftp.upload_file(local_path, remote_path)

    download_manager.relecov_sftp.close_connection()

    def test_download(download_manager):
        print("Waiting for files to be ready in SFTP...")
        download_manager.relecov_sftp.open_connection()

        print("Checking if folders have been merged already...")
        remote_dirs = download_manager.relecov_sftp.list_remote_folders(
            ".", recursive=True
        )
        tmp_processing_present = any(
            "tmp_processing" in folder for folder in remote_dirs
        )

        if not tmp_processing_present:
            print("Merging subfolders manually before starting download...")
            folders_to_process = {}
            for folder in remote_dirs:
                folder = folder.replace("./", "")
                try:
                    files_in_folder = download_manager.relecov_sftp.get_file_list(
                        folder
                    )
                    if files_in_folder:
                        folders_to_process[folder] = files_in_folder
                except Exception:
                    continue

            if not folders_to_process:
                raise RuntimeError("No folders found to merge, cannot continue")

            download_manager.merge_subfolders(folders_to_process)

        print("Starting download process...")
        download_manager.execute_process()

    test_download(download_manager)


if __name__ == "__main__":
    main()
