#!/usr/bin/env python
import os
import sys
import argparse
from relecov_tools.sftp_handle import SftpHandle


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
    # First clean the repository.
    print("Initating sftp module")
    sftp_connection = SftpHandle(
        user=kwargs["user"],
        passwd=kwargs["password"],
        conf_file=None,
        user_relecov=None,
        password_relecov=None,
        download_option=kwargs["download_option"],
        output_location=kwargs["output_location"],
        target_folders=kwargs["target_folders"],
    )
    print("Openning connection to sftp")
    sftp_connection.sftp_port = os.environ["TEST_PORT"]
    if not sftp_connection.open_connection():
        print("Could not open connection to remote sftp")
        sys.exit(1)
    remote_folders = sftp_connection.list_remote_folders(".", recursive=True)
    clean_folders = [folder.replace("./", "") for folder in remote_folders]
    print("Cleaning folders")
    for folder in clean_folders:
        if len(folder.split("/")) < 2:
            continue
        filelist = sftp_connection.get_file_list(folder)
        for file in filelist:
            sftp_connection.client.remove(file)
        print(f"Removing remote folder {folder}")
        sftp_connection.client.rmdir(folder)

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
        sftp_connection.client.mkdir(os.path.join(remote_dir, base_folder))
        print(f"Uploading files from {base_folder}")
        for file in files:
            remotepath = os.path.join(remote_dir, base_folder, file)
            local_path = os.path.join(os.path.abspath(folder), file)
            sftp_connection.client.put(localpath=local_path, remotepath=remotepath)

    sftp_connection.close_connection()

    # Test download_module
    def test_download(sftp_connection):
        sftp_connection.execute_process()

    test_download(sftp_connection)


if __name__ == "__main__":
    main()
