#!/usr/bin/env python
from datetime import datetime
import logging
import rich.console

import paramiko
import sys
import os
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
        if conf_file is None:
            config_json = ConfigJson()
            self.server = config_json.get_topic_data("sftp_connection", "sftp_server")
            self.port = config_json.get_topic_data("sftp_connection", "sftp_port")
            self.storage_local_folder = config_json.get_configuration(
                "storage_local_folder"
            )
        else:
            if not os.path.isfile(conf_file):
                stderr.print(
                    "[red] Configuration file does not exist. " + conf_file + "!"
                )
                sys.exit(1)
            with open(conf_file, "r") as fh:
                config = yaml.load(fh, Loader=yaml.FullLoader)
            try:
                self.sftp_server = config["sftp_server"]
                self.sftp_port = config["sftp_port"]
                self.storage_local_folder = config["storage_local_folder"]
                self.user = config["user_sftp"]
                self.passwd = config["password"]
            except KeyError as e:
                log.error("Invalid configuration file %s", e)
                stderr.print("[red] Invalide configuration file " + e + "!")
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
        """Stablish sftp connection"""
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
        os.makedirs(local_folder_path)
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
                result_data["unable_to_fetch"].append(file_list)
                continue
            result_data["fetched_files"].append(os.path.basename(file_list))

        return result_data

    def create_main_folders(self, root_directory_list):
        """Create the main folder structure if not exists"""
        for folder in root_directory_list:
            full_folder = os.path.join(self.storage_local_folder, folder)
            os.makedirs(full_folder, exist_ok=True)
        return True

    def download_from_sftp(self):
        try:
            os.makedirs(self.storage_local_folder, exist_ok=True)
        except OSError as e:
            log.error("You do not have permissions to create folder %s", e)
            sys.exit(1)
        os.chdir(self.storage_local_folder)
        if not self.open_connection():
            sys.exit(1)

        root_directory_list = self.list_folders(".")
        if not root_directory_list:
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
            result_data = self.get_files_from_sftp_folder(folder, files)
            sftp_folder_md5 = relecov_tools.utils.get_md5_from_local_folder(
                result_data["local_folder"]
            )
            local_md5 = relecov_tools.utils.calculate_md5(result_data["fetched_files"])
            # MD5 Checking
            if sftp_folder_md5:
                if sftp_folder_md5 == local_md5:
                    log.info(
                        "Successful file download for files in folder %s",
                        result_data["local_folder"],
                    )
                else:
                    log.error(
                        "MD5 does not match. Files could be corrupted atfolder %s",
                        result_data["local_folder"],
                    )
            else:
                log.info(
                    "Md5 file was not created by lab. Copy the local md5 into folder %s",
                    result_data["local_folder"],
                )
                file_name = os.path.join(
                    result_data["local_folder"], "generated_locally.md5"
                )
                relecov_tools.utils.save_md5(file_name, local_md5)
