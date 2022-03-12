#!/usr/bin/env python

import logging
import rich.console
import hashlib
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


def get_md5(file):
    """
    Get the MD5 of a file, following this schema:
    Open it, sequentially add more chunks of it to the hash,
    return the hash
    Usage:
        get_md5(file)
    Return:
        md5 hash of the given file
    """

    md5_object = hashlib.md5()
    with open(file, "rb") as infile:
        for block in iter(lambda: infile.read(4096), b""):
            md5_object.update(block)
        return md5_object.hexdigest()


class SftpHandle:
    def __init__(self, user=None, passwd=None, conf_file=None):
        """
        Initializes the Connection object and starts its host, port, user and key attributes.
        Declaration:
            sftp = SftpHandle(host, port, user, key)
        """
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
        """
        Uses the class attributes to make a SFTP connection
        Usage:
            sftp.open_connection()
        Return:
            True if connected succesfully
            False if failed connection
        """
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
        """
        Closes the SFTP connection if there is any
        Usage:
            sftp.close_connection()
        Return:
            -True if connection closed successfully
        """
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

    def create_main_folders(self, root_directory_list):
        """Create the main folder structure if not exists"""
        for folder in root_directory_list:
            full_folder = os.path.join(self.storage_local_folder, folder)
            os.makedirs(full_folder, exist_ok=True)
        return True

    def download(self):
        """
        Generates the download dict with create_download_dictionary
        Generates the directories in the keys, download the files in
        the values inside of them.

        Then, for each file, checks the md5 with the get_md5 function,
        and the size of the file with os path. This data is transferred
        to a dictionary

        Usage:
            sftp.download()
        Return:
            dicionary with key: filename, val: [md5, size]
        """
        filestats_dict = {}

        download_dict = self.create_download_dictionary()
        for directory, file_list in download_dict.items():
            os.mkdir(directory)
            for file in file_list:
                self.client.get(file, file)
                file_md5_hash = get_md5(file)
                file_size = os.path.getsize(file)
                filestats_dict[file] = [file_md5_hash, file_size]

        return filestats_dict

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
        for_downloading_folder = {}
        # create_main_folders(root_directory_list)
        for folder in root_directory_list:
            list_files = self.get_file_list(folder)
            if len(list_files) > 0:
                for_downloading_folder[folder] = list_files
