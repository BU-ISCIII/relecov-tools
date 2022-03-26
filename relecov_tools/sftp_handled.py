#!/usr/bin/env python
from datetime import datetime
import logging
import rich.console
import paramiko
import sys
import os
import shutil
import yaml
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SftpHandle:
    def __init__(self, user=None, passwd=None, conf_file=None):
        """Initializes the sftp object"""
        config_json = ConfigJson()
        self.allowed_sample_ext = config_json.get_configuration(
            "allowed_sample_extensions"
        )

        if conf_file is None:
            self.server = config_json.get_topic_data("sftp_connection", "sftp_server")
            self.port = config_json.get_topic_data("sftp_connection", "sftp_port")
            self.storage_local_folder = config_json.get_configuration(
                "storage_local_folder"
            )
            self.metadata_tmp_folder = config_json.get_configuration(
                "tmp_folder_for_metadata"
            )
            self.abort_if_md5_mismatch = (
                True
                if config_json.get_configuration("abort_if_md5_mismatch") == "True"
                else False
            )
        else:
            if not os.path.isfile(conf_file):
                log.error("Configuration file %s does not exists", conf_file)
                stderr.print(
                    "[red] Configuration file does not exist. " + conf_file + "!"
                )
                sys.exit(1)
            with open(conf_file, "r") as fh:
                config = yaml.load(fh, Loader=yaml.FullLoader)
            try:
                self.sftp_server = config["sftp_server"]
                self.sftp_port = config["sftp_port"]
                try:
                    self.storage_local_folder = config["storage_local_folder"]
                except KeyError:
                    self.storage_local_folder = config_json.get_configuration(
                        "storage_local_folder"
                    )
                try:
                    self.metadata_tmp_folder = config["tmp_metadata_folder"]
                except KeyError:
                    self.metadata_tmp_folder = config_json.get_configuration(
                        "tmp_folder_for_metadata"
                    )
                self.user = config["user_sftp"]
                self.passwd = config["password"]
            except KeyError as e:
                log.error("Invalid configuration file %s", e)
                stderr.print("[red] Invalide configuration file {e} !")
                sys.exit(1)
        if user is None:
            self.user = relecov_tools.utils.text(msg="Enter the userid")
        else:
            self.user = user
        if passwd is None:
            self.passwd = relecov_tools.utils.password(msg="Enter your password")
        else:
            self.passwd = passwd
        self.client = None

    def open_connection(self):
        """Establish sftp connection"""
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            hostname=self.server,
            port=self.port,
            username=self.user,
            password=self.passwd,
            allow_agent=False,
            look_for_keys=False,
        )
        try:
            self.client = self.client.open_sftp()
            return True
        except paramiko.SSHException as e:
            log.error("Invalid Username/Password for %s:", e)
            return False

    def close_connection(self):
        """Closes SFTP connection"""
        self.client.close()
        return True

    def list_folders(self, folder_name):
        """Creates a directories list from the given path"""
        directory_list = []
        try:
            content_list = self.client.listdir(folder_name)
        except FileNotFoundError as e:
            log.error("Invalid folder at remote sftp %s", e)
            return False

        for content in content_list:
            try:
                self.client.listdir(content)
            except FileNotFoundError:
                continue
            directory_list.append(content)

        return directory_list

    def get_file_list(self, folder_name):
        """Return a tuple with file name and directory path"""
        file_list = []
        content_list = self.client.listdir(folder_name)
        for content in content_list:
            try:
                sub_folder_files = self.client.listdir(folder_name + "/" + content)
                for sub_folder_file in sub_folder_files:
                    file_list.append(
                        folder_name + "/" + content + "/" + sub_folder_file
                    )
            except FileNotFoundError:
                file_list.append(folder_name + "/" + content)

        return file_list

    def get_files_from_sftp_folder(self, folder, files_list):
        """Create the subfolder with the present date and fetch all files from
        the sftp server
        """
        result_data = {"unable_to_fetch": [], "fetched_files": []}
        date = datetime.today().strftime("%Y%m%d")
        local_folder_path = os.path.join(self.storage_local_folder, folder, date)
        result_data["local_folder"] = local_folder_path
        os.makedirs(local_folder_path, exist_ok=True)
        log.info("created the folder to download files %s", local_folder_path)
        self.open_connection()

        for file_list in files_list:
            try:
                self.client.get(
                    file_list,
                    os.path.join(local_folder_path, os.path.basename(file_list)),
                )
            except FileNotFoundError as e:
                log.error("Unable to fetch file %s ", e)
                result_data["Unable_to_fetch"].append(file_list)
                continue
            result_data["fetched_files"].append(os.path.basename(file_list))
        return result_data

    def verify_md5_checksum(self, local_folder, file_list):
        """Get the md5 value from sftp match with the generated at local
        folder
        """
        required_retransmition = []
        successful_files = []
        # fetch the md5 file if exists
        sftp_md5 = relecov_tools.utils.get_md5_from_local_folder(local_folder)
        if len(sftp_md5) > 0:
            # check md5 checksum for eac file
            for f_name, values in sftp_md5.items():
                f_path_name = os.path.join(local_folder, f_name)
                # Checksum value is stored in the index 1
                if values[1] == relecov_tools.utils.calculate_md5(f_path_name):
                    log.info(
                        "Successful file download for %s in folder %s",
                        f_name,
                        local_folder,
                    )
                    successful_files.append(f_name)
                else:
                    required_retransmition.append(f_name)
                    log.error("%s requested file re-sending", f_name)

        if len(set(file_list)) != len(sftp_md5) * 2:
            # create the md5 file from the ones not upload to server
            req_create_md5 = [
                v
                for v in file_list
                if (v not in successful_files and not v.endswith("*.md5"))
            ]
            sftp_md5.update(
                relecov_tools.utils.create_md5_files(local_folder, req_create_md5)
            )
        return sftp_md5, required_retransmition

    def create_tmp_files_with_metadata_info(self, local_folder, file_list, md5_data):
        """Copy metadata file from folder and create a file with the sample
        names
        """
        out_folder = self.metadata_tmp_folder
        os.makedirs(out_folder, exist_ok=True)
        prefix_file_name = "_".join(local_folder.split("/")[-2:])
        metadata_file = prefix_file_name + "_metadata_lab.xlsx"
        sample_data_file = os.path.join(out_folder, prefix_file_name + "samples.csv")
        try:
            shutil.copy(
                os.path.join(local_folder, "Metadata_lab.xlsx"),
                os.path.join(out_folder, metadata_file),
            )
        except OSError as e:
            log.error("Unable to copy Metadata file %s", e)
            stderr.print("[red] Unable to copy Metadata file")
        sample_data = []

        for key, values in md5_data.items():
            if key.endswith(tuple(self.allowed_sample_ext)):
                sample_data.append(key + "," + ",".join(values))
        if len(sample_data) == 0:
            log.error("There is no samples in folder %s", local_folder)
            stderr.print("[red] There is no samples for this local folder")
        else:
            with open(sample_data_file, "w") as fh:
                for sample in sample_data:
                    fh.write(sample + "\n")
        return

    def create_main_folders(self, root_directory_list):
        """Create the main folder structure if not exists"""
        for folder in root_directory_list:
            full_folder = os.path.join(self.storage_local_folder, folder)
            os.makedirs(full_folder, exist_ok=True)
        return True

    def delete_remote_files(self, folder, files):
        """Delete files from remote server """
        self.open_connection()
        for file in files:
            try:
                self.client.remove(file)
                log.info("%s Deleted from remote server", file)
            except FileNotFoundError:
                continue
        return

    def download_from_sftp(self):
        try:
            os.makedirs(self.storage_local_folder, exist_ok=True)
        except OSError as e:
            log.error("You do not have permissions to create folder %s", e)
            sys.exit(1)
        os.chdir(self.storage_local_folder)
        if not self.open_connection():
            log.error("Unable to establish connection towards sftp server")
            stderr.print("[red] Unable to establish sftp connection")
            sys.exit(1)

        root_directory_list = self.list_folders(".")
        if not root_directory_list:
            log.error("There is no folders under root directory")
            sys.exit(1)
        folders_to_download = {}
        # create_main_folders(root_directory_list)
        for folder in root_directory_list:
            list_files = self.get_file_list(folder)
            if len(list_files) > 0:
                folders_to_download[folder] = list_files
        if len(folders_to_download) == 0:
            log.info("Exiting download. There is no files on sftp to dowload")
            self.close_connection()
            sys.exit(0)

        for folder, files in folders_to_download.items():
            # get the files in each folder
            result_data = self.get_files_from_sftp_folder(folder, files)
            md5_files, req_retransmition = self.verify_md5_checksum(
                result_data["local_folder"], result_data["fetched_files"]
            )
            # retrasmision of files in folder
            if len(req_retransmition) > 0:
                restransmition_data = self.get_files_from_sftp_folder(
                    folder, req_retransmition
                )
                md5_ret_files, corrupted = self.verify_md5_checksum(
                    result_data["local_folder"], restransmition_data["fetched_files"]
                )
                md5_files.update(md5_ret_files)
                if len(corrupted) > 0 and self.abort_if_md5_mismatch:
                    log.error("Stopping because of corrupted files %s", corrupted)
                    stderr.print(
                        f"[red] Stop processing folder {folder} because of corrupted files {corrupted}"
                    )
                    continue
            self.create_tmp_files_with_metadata_info(
                result_data["local_folder"], result_data["fetched_files"], md5_files
            )
            self.delete_remote_files(folder, files)
        self.close_connection()
        return
