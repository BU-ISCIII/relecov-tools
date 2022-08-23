#!/usr/bin/env python
from datetime import datetime
from itertools import islice
import copy
import logging
import glob
import json
import rich.console
import paramiko
import sys
import os
import shutil
import yaml
import openpyxl
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.rest_api import RestApi

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SftpHandle:
    def __init__(
        self,
        user=None,
        passwd=None,
        conf_file=None,
        user_relecov=None,
        password_relecov=None,
    ):
        """Initializes the sftp object"""
        config_json = ConfigJson()
        self.allowed_sample_ext = config_json.get_configuration(
            "allowed_sample_extensions"
        )
        self.sftp_user = user
        self.sftp_passwd = passwd
        if conf_file is None:
            self.sftp_server = config_json.get_topic_data(
                "sftp_connection", "sftp_server"
            )
            self.sftp_port = config_json.get_topic_data("sftp_connection", "sftp_port")
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
                try:
                    self.abort_if_md5_mismatch = (
                        True if config["abort_if_md5_mismatch"] == "True" else False
                    )
                except KeyError:
                    self.abort_if_md5_mismatch = (
                        True
                        if config_json.get_configuration("abort_if_md5_mismatch")
                        == "True"
                        else False
                    )
                self.sftp_user = config["sftp_user"]
                self.sftp_passwd = config["sftp_passwd"]
                self.pp = config["allowed_sample_extensions"]
            except KeyError as e:
                log.error("Invalid configuration file %s", e)
                stderr.print("[red] Invalide configuration file {e} !")
                sys.exit(1)
        if self.sftp_user is None:
            self.sftp_user = relecov_tools.utils.prompt_text(msg="Enter the userid")
        if self.sftp_passwd is None:
            self.sftp_passwd = relecov_tools.utils.prompt_password(
                msg="Enter your password"
            )
        self.client = None
        external_servers = config_json.get_topic_data("external_url", "relecov")
        self.relecov_server = external_servers["server"]
        self.relecov_url = external_servers["url"]
        self.relecov_post_url = external_servers["sftp_info"]

    def open_connection(self):
        """Establish sftp connection"""
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            hostname=self.sftp_server,
            port=self.sftp_port,
            username=self.sftp_user,
            password=self.sftp_passwd,
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

    def create_tmp_files_with_metadata_info(
        self, local_folder, file_list, md5_data, metadata_file
    ):
        """Copy metadata file from folder and create a file with the sample
        names
        """
        out_folder = self.metadata_tmp_folder
        os.makedirs(out_folder, exist_ok=True)
        prefix_file_name = "_".join(local_folder.split("/")[-2:])
        new_metadata_file = "metadata_lab_" + prefix_file_name + ".xlsx"
        sample_data_file = "samples_data_" + prefix_file_name + ".json"
        sample_data_path = os.path.join(out_folder, sample_data_file)
        try:
            shutil.copy(
                os.path.join(local_folder, metadata_file),
                os.path.join(out_folder, new_metadata_file),
            )
        except OSError as e:
            log.error("Unable to copy Metadata file %s", e)
            stderr.print("[red] Unable to copy Metadata file")
            return False
        data = copy.deepcopy(file_list)
        for s_name, values in file_list.items():
            for f_type, f_name in values.items():
                if not f_name.endswith(tuple(self.allowed_sample_ext)):
                    stderr.print("[red] " + f_name + " has a not valid extension")
                if "_R1_" in f_name:
                    data[s_name]["r1_fastq_filepath"] = md5_data[f_name][0]
                    data[s_name]["fastq_r1_md5"] = md5_data[f_name][1]
                elif "_R2_" in f_name:
                    data[s_name]["r2_fastq_filepath"] = md5_data[f_name][0]
                    data[s_name]["fastq_r2_md5"] = md5_data[f_name][1]
                else:
                    pass
        with open(sample_data_path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False))

        return True

    def create_main_folders(self, root_directory_list):
        """Create the main folder structure if not exists"""
        for folder in root_directory_list:
            full_folder = os.path.join(self.storage_local_folder, folder)
            os.makedirs(full_folder, exist_ok=True)
        return True

    def find_metadata_file(self, local_folder):
        """Find excel extension file which contains metatada"""
        reg_for_xlsx = os.path.join(local_folder, "*.xlsx")
        ex_files = glob.glob(reg_for_xlsx)
        if len(ex_files) == 0:
            log.error("Excel file for metadata does not exist on %s", local_folder)
            stderr.print("[red] Metadata file does not exist on " + local_folder)
            return False
        if len(ex_files) > 1:
            log.error("Too many Excel files on folder %s", local_folder)
            stderr.print("[red] Metadata file does not exist on " + local_folder)
            return False
        return ex_files[0]

    def get_sample_fastq_file_names(self, local_folder, meta_f_path):
        """ """
        if not os.path.isfile(meta_f_path):
            log.error("Metadata file does not exists on %s", local_folder)
            stderr.print("[red] METADATA_LAB.xlsx do not exist in" + local_folder)
            return False
        wb_file = openpyxl.load_workbook(meta_f_path, data_only=True)
        ws_metadata_lab = wb_file["METADATA_LAB"]
        sample_file_list = {}
        # find out the index for file names
        for col in ws_metadata_lab[4]:
            if "Sequence file R1 fastq" == col.value:
                index_fastq_r1 = col.column - 1
            elif "Sequence file R2 fastq" == col.value:
                index_fastq_r2 = col.column - 1
        for row in islice(ws_metadata_lab.values, 4, ws_metadata_lab.max_row):
            if row[2] is not None:
                try:
                    s_name = str(row[2])
                except ValueError as e:
                    stderr.print("[red] Unable to convert to string. ", e)
                if s_name not in sample_file_list:
                    sample_file_list[s_name] = {}
                else:
                    print("Found duplicated sample ", s_name)
                if row[index_fastq_r1] is not None:
                    sample_file_list[s_name]["sequence_file_R1_fastq"] = row[
                        index_fastq_r1
                    ]
                else:
                    log.error(
                        "Fastq_R1 not defined in Metadata file for sample %s", s_name
                    )
                    stderr.print("[red] No fastq R1 file for sample " + s_name)
                    return False
                if row[index_fastq_r2] is not None:
                    sample_file_list[s_name]["sequence_file_R2_fastq"] = row[
                        index_fastq_r2
                    ]
        return sample_file_list

    def validate_download_files(self, sample_file_list, local_folder):
        """Check if download sample files are the ones defined on metadata file"""
        if not sample_file_list:
            return False

        for sample, files in sample_file_list.items():
            for file in files.values():
                if not os.path.isfile(os.path.join(local_folder, file)):
                    log.error("File %s does not exist", file)
                    stderr.print("[red] File " + file + " does not exist")
                    return False
        return True

    def delete_remote_files(self, folder, files):
        """Delete files from remote server"""
        self.open_connection()
        for file in files:
            try:
                self.client.remove(file)
                log.info("%s Deleted from remote server", file)
            except FileNotFoundError:
                continue
        return

    def create_json_with_downloaded_samples(self, sample_file_list, folder):
        """From the download information prepare a json file"""
        sample_dict = {}
        for sample in sample_file_list:
            sample_dict[sample] = {"folder": folder}
        return json.dumps(sample_dict, indent=4, sort_keys=True, ensure_ascii=False)

    def send_info_to_server(self, json_data):
        rest_api = RestApi(self.database_server, self.database_url)
        error_counts = 0
        for item in json_data:
            result = rest_api.post_request(item, "", self.relecov_post_url)
            if "ERROR" in result:
                error_counts += 1
                log.error("Request was not accepted %s", result["ERROR"])
                stderr.print(
                    "[red] Error " + result["ERROR"] + " when sending request}"
                )
        if error_counts == 0:
            log.info("Request was not accepted %s", result["ERROR"])
            stderr.print("[green] Successful upload information to server")
        return

    def delete_local_folder(self, local_folder):
        """Delete download folder because files does not complain requisites"""
        shutil.rmtree(local_folder, ignore_errors=True)
        return True

    def download(self):
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
            log.info("Processing folder %s", folder)
            stderr.print("Processing folder " + folder)
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
            meta_file = self.find_metadata_file(result_data["local_folder"])
            sample_file_list = self.get_sample_fastq_file_names(
                result_data["local_folder"], meta_file
            )
            if self.validate_download_files(
                sample_file_list, result_data["local_folder"]
            ):
                self.create_tmp_files_with_metadata_info(
                    result_data["local_folder"], sample_file_list, md5_files, meta_file
                )
                # Collect data to send the request to relecov_platform
                json_sample_data = self.create_json_with_downloaded_samples(
                    sample_file_list, folder
                )
                for record in json_sample_data:
                    pass
            else:
                log.info("Deleting local folder %s", result_data["local_folder"])
                self.delete_local_folder(result_data["local_folder"])

        self.close_connection()
        return
