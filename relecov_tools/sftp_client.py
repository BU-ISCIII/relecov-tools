import logging
import os
import paramiko
import rich.console
import stat
import sys
import time
from relecov_tools.config_json import ConfigJson
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SftpClient:
    """Class to handle SFTP connection with remote server. It uses paramiko library to establish
    the connection. The class can be used to upload and download files from the remote server.
    The class can be initialized with a configuration file and with the username and password.
    If the configuration file is not provided, the class will try to read the configuration from
    the environment variables. If configuration file is provided, the class will read the
    configuration from the file. The format of the configuration file should be a json file with
    the following keys:
    {
        "sftp_server": "server_name",
        "sftp_port": "port_number"
    }
    """

    def __init__(self, conf_file=None, username=None, password=None):
        if not conf_file:
            config_json = ConfigJson()
            self.sftp_server = config_json.get_topic_data("sftp_handle", "sftp_server")
            self.sftp_port = config_json.get_topic_data("sftp_handle", "sftp_port")
        else:
            config_json = conf_file
            if not os.path.isfile(conf_file):
                log.error("Configuration file %s does not exists", conf_file)
                stderr.print(
                    "[red] Configuration file does not exist. " + conf_file + "!"
                )
                sys.exit(1)
            j_data = relecov_tools.utils.read_json_file(conf_file)
            try:
                self.sftp_server = j_data["sftp_server"]
                self.sftp_port = j_data["sftp_port"]
            except KeyError as e:
                log.error("Could not find the key %s in the config file", e)
                stderr.print(
                    "[red] Could not find the key " + e + "in config file " + conf_file
                )
                sys.exit(1)
        self.user_name = username
        self.password = password
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    def reconnect_if_fail(n_times, sleep_time):
        def decorator(func):
            def retrier(self, *args, **kwargs):
                more_sleep_time = 0
                retries = 0
                while retries < n_times:
                    try:
                        return func(self, *args, **kwargs)
                    except Exception:
                        retries += 1
                        log.info("Connection lost. Trying to reconnect...")
                        time.sleep(more_sleep_time)
                        # Try extending sleep time before reconnecting in each step
                        more_sleep_time = more_sleep_time + sleep_time
                        self.open_connection()
                else:
                    log.error("Could not reconnect to remote client")
                return func(self, *args, **kwargs)

            return retrier

        return decorator

    def open_connection(self):
        """Establishing sftp connection"""
        log.info("Setting credentials for SFTP connection with remote server")
        self.client.connect(
            hostname=self.sftp_server,
            port=self.sftp_port,
            username=self.user_name,
            password=self.password,
            allow_agent=False,
            look_for_keys=False,
        )
        try:
            log.info("Trying to establish SFTP connection")
            self.sftp = self.client.open_sftp()
        except Exception as e:
            log.error("Could not establish SFTP connection: %s", e)
            stderr.print("[red]Could not establish SFTP connection")
            return False
        return True

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def list_remote_folders(self, folder_name, recursive=False):
        """Creates a directories list from the given client remote path

        Args:
            folder_name (str): folder name in remote path
            recursive (bool, optional): finds all subdirectories too. Defaults to False.

        Returns:
            directory_list(list(str)): Names of all folders within remote folder
        """
        log.info("Listing directories in %s", folder_name)
        directory_list = []
        try:
            content_list = self.sftp.listdir_attr(folder_name)
            subfolders = any(stat.S_ISDIR(item.st_mode) for item in content_list)
        except (FileNotFoundError, OSError) as e:
            log.error("Invalid folder at remote sftp %s", e)
            raise
        if not subfolders:
            return [folder_name]

        def recursive_list(folder_name):
            try:
                attribute_list = self.sftp.listdir_attr(folder_name)
            except (FileNotFoundError, OSError) as e:
                log.error("Invalid folder at remote sftp %s", e)
                raise
            for attribute in attribute_list:
                if stat.S_ISDIR(attribute.st_mode):
                    abspath = os.path.join(folder_name, attribute.filename)
                    directory_list.append(abspath)
                    recursive_list(abspath)
                else:
                    continue
            return directory_list

        if recursive:
            log.debug("Listing recursive")
            directory_list = recursive_list(folder_name)
            if folder_name != ".":
                directory_list.append(folder_name)
            return directory_list
        try:
            log.debug("Listing content in folders...")
            directory_list = [
                item.filename for item in content_list if stat.S_ISDIR(item.st_mode)
            ]
        except AttributeError:
            return False
        self.close_connection()
        return directory_list

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def get_file_list(self, folder_name, recursive=False):
        """Return a tuple with file name and directory path from remote

        Args:
            folder_name (str): name of folder in remote repository
            recursive (bool): either to list files recursively through subfolders

        Returns:
            file_list (list(str)): list of files in remote folder
        """
        log.info("Listing files in %s", folder_name)
        file_list = []
        try:
            content_list = self.sftp.listdir_attr(folder_name)
            for content in content_list:
                full_path = os.path.join(folder_name, content.filename)
                if stat.S_ISDIR(content.st_mode):
                    if recursive is True:
                        file_list.extend(self.get_file_list(full_path))
                elif stat.S_ISREG(content.st_mode):
                    file_list.append(full_path)
        except FileNotFoundError as e:
            log.error(f"Folder not found: {folder_name}")
            raise e
        return file_list

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def get_from_sftp(self, file, destination, exist_ok=False):
        """Download a file from remote sftp

        Args:
            file (str): path of the file in remote sftp
            destination (str): local path of the file after download
            exist_ok (bool): Skip download if file exists in local destination

        Returns:
            bool: True if download was successful, False if it was not
        """
        if os.path.exists(destination) and exist_ok:
            return True
        else:
            try:
                self.sftp.get(file, destination)
                return True
            except FileNotFoundError as e:
                log.error("Unable to fetch file %s ", e)
                try:
                    os.remove(destination)
                except OSError:
                    log.error(f"Could not delete {destination} after failed fetch")
                    pass
                return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def make_dir(self, folder_name):
        """Create a new directory in remote sftp

        Args:
            folder_name (str): name of the directory to be created

        Returns:
            bool: True if directory was created, False if it was not
        """
        try:
            self.sftp.mkdir(folder_name)
            return True
        except FileExistsError:
            log.error("Directory %s already exists", folder_name)
            stderr.print("[red]Directory already exists")
            return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def rename_file(self, old_name, new_name):
        """Rename a file in remote sftp

        Args:
            old_name (str): current name of the file
            new_name (str): new name of the file

        Returns:
            bool: True if file was renamed, False if it was not
        """
        try:
            self.sftp.rename(old_name, new_name)
            return True
        except FileNotFoundError as e:
            error_txt = f"Could not rename {old_name} to {new_name}: {e}"
            log.error(error_txt)
            stderr.print(f"[red]{error_txt}")
            return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def remove_file(self, file_name):
        """Remove a file from remote sftp

        Args:
            file_name (str): name of the file to be removed

        Returns:
            bool: True if file was removed, False if it was not
        """
        try:
            self.sftp.remove(file_name)
            log.info("%s Deleted from remote server", file_name)
            return True
        except FileNotFoundError:
            log.error("File %s not found", file_name)
            stderr.print("[red]File not found")
            return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def remove_dir(self, folder_name):
        """Remove a directory from remote sftp

        Args:
            folder_name (str): name of the directory to be removed

        Returns:
            bool: True if directory was removed, False if it was not
        """
        try:
            self.sftp.rmdir(folder_name)
            return True
        except FileNotFoundError:
            log.error("Directory %s not found", folder_name)
            stderr.print("[red]Directory not found")
            return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def upload_file(self, local_path, remote_file):
        """Upload a file to remote sftp

        Args:
            localpath (str): path of the file in local machine
            remote_file (str): path of the file in remote sftp

        Returns:
            bool: True if file was uploaded, False if it was not
        """
        try:
            self.sftp.put(local_path, remote_file)
            return True
        except FileNotFoundError as e:
            log.error(f"Could not upload file {local_path}: {e}")
            stderr.print(f"[red]Could not upload file {local_path}: {e}")
            return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def copy_within_sftp(self, src_path, dest_path, buffer_size=65536):
        """
        Copies a file within the SFTP server by reading and writing in blocks.

        Args:
            src_path (str): Path to the source file on the SFTP server.
            dest_path (str): Path where the file should be copied to on the SFTP server.
            buffer_size (int, optional): Block size for reading/writing in bytes. Default is 64 KB.

        Returns:
            bool: True if the copy was successful, False if it was not
        """
        try:
            log.info(f"Copying file within SFTP: {src_path} -> {dest_path}")
            with self.sftp.open(src_path, "rb") as src_file:
                with self.sftp.open(dest_path, "wb") as dest_file:
                    while True:
                        data = src_file.read(buffer_size)
                        if not data:
                            break
                        dest_file.write(data)
            log.info(f"File successfully copied within SFTP to: {dest_path}")
            return True
        except FileNotFoundError:
            log.error(f"Source file not found: {src_path}")
        except IOError as e:
            log.error(f"Error during SFTP copy operation: {e}")
        return False

    @reconnect_if_fail(n_times=3, sleep_time=30)
    def close_connection(self):
        log.info("Closing SFTP connection")
        try:
            self.sftp.close()
        except NameError:
            log.warning("Could not close sftp connection")
            return False
        log.info("SFTP connection closed")
        return True
