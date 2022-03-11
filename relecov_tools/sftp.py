"""
=============================================================
HEADER
=============================================================
INSTITUTION: BU-ISCIII
AUTHOR: Guillermo J. Gorines Cordero
MAIL: guillermo.gorines@urjc.es
VERSION: 0
CREATED: 11-2-2022
REVISED: 11-3-2022
REVISED BY: guillermo.gorines@urjc.es
DESCRIPTION:

    Includes the SFTP connection class, and its associated methods.

REQUIREMENTS:
    -Python
    -Paramiko

TO DO:
- Dowload the data (DONE)
- Check MD5 and File size (DONE)
- Testing
- Check minimal required Python version
- Delete testing inside this script

================================================================
END_OF_HEADER
================================================================
"""

# Imports

import hashlib
import paramiko
import sys
import os


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
    def __init__(self, host, port, user, key):
        """
        Initializes the Connection object and starts its host, port, user and key attributes.
        Declaration:
            sftp = SftpHandle(host, port, user, key)
        """
        self.host = host
        self.port = port
        self.user = user
        self.key = key
        self.client = None

    """
    def check(self):
        Check if there is a SFTP connection
        Usage:
            sftp.check()
        Return:
            True if a connection still exists
            False if connection doesnt exist (not established or timed out for instance)
        try:
            self.client.getcwd()
            return True
        except OSError as e:
            return False
    """

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
            hostname=self.host,
            port=self.port,
            username=self.user,
            password=self.key,
            allow_agent=False,
            look_for_keys=False,
        )

        self.client = self.client.open_sftp()

        return True

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

    def list_dirs(self, only_dirs=False):
        """
        Generates a list of directories inside the root
        of the SFTP.

        Usage:
            sftp.list_dirs(only_dirs=BOOL)
        Return:
            List with all the contents (dirs or files)

        """
        sftp_contents = self.client.listdir()

        if only_dirs:
            # get only items with no extension (dirs)
            dirs = [item for item in sftp_contents if len(item.split(".")) == 1]
            return dirs
        else:
            return sftp_contents

    def create_download_dictionary(self):
        """
        Generates a dictionary with key: directory in the SFTP
        and value: list with all the files inside that dir
        To do so, calls self.list_dirs().
        Usage:
            sftp.create_download_dictionary()
        Return:
            dictionary with key: dir, val: [contents of dir]

        """
        download_dict = {}

        to_download = self.list_dirs(only_dirs=True)
        for directory in to_download[0:1]:
            # get only files (strings with extension)
            item_list = [
                f"{directory}/{item}"
                for item in self.client.listdir(directory)
                if len(item.split(".")) > 1
            ]
            download_dict[directory] = item_list

        return download_dict

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


def main():
    pass
    return



if __name__ == "__main__":
    sys.exit(main())

# TESTING ZONE, must be deleted later
# This will NOT work
CLAVE = "RANDOM_KEY_FOR_TESTING"
HOST = "RANDOM_SFTP_FOR_TESTING"
PUERTO = 420
USUARIO = "ARTURITO"

my_sftp = SftpHandle(HOST, PUERTO, USUARIO, CLAVE)
contents = my_sftp.list_dirs()


# This will work
CLAVE = "Bioinfo%123"
HOST = "sftprelecov.isciii.es"
PUERTO = 22
USUARIO = "bioinfoadm"

my_sftp = SftpHandle(HOST, PUERTO, USUARIO, CLAVE)
