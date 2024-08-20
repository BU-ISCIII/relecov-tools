import logging
import os
import paramiko
import rich.console
import sys
from relecov_tools.config_json import ConfigJson
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SftpRelecov:
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
        if conf_file is None:
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

    def list_folders(self, remote_path):
        self.open_connection()
        """List all the folders in the remote path"""
        log.info("Listing folders in the remote path %s", remote_path)
        try:
            folders = self.sftp.listdir(remote_path)
        except FileNotFoundError:
            log.error("Remote path %s does not exist", remote_path)
            stderr.print("[red]Remote path does not exist")
            return None
        return folders

    def pepe(self, pepe):
        print(pepe)
      
    def close_connection(self):
        log.info("Closing SFTP connection")
        try:
            self.sftp.close()
            self.client.close()
        except NameError:
            return False
        log.info("SFTP connection closed")
        return True
