"""
=============================================================
HEADER
=============================================================
INSTITUTION: BU-ISCIII
AUTHOR: Guillermo J. Gorines Cordero
MAIL: guillermo.gorines@urjc.es
VERSION: 0
CREATED: 11-2-2022
REVISED: 11-2-2022
REVISED BY: guillermo.gorines@urjc.es
DESCRIPTION:

    Includes the SFTP connection class, and its associated methods.

REQUIREMENTS:
    -Python
    -Paramiko

TO DO:

-Check minimal required Python version
-Delete testing inside this script 

================================================================
END_OF_HEADER
================================================================
"""

# Imports

import paramiko
import sys


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
            sftp.open()
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
            sftp.close()
        Return:
            -True if connection closed successfully
            -False if connection closing failed
            -None if no connection was established
        """
        if self.check():
            try:
                self.client.close()
                return True
            except:
                return False
            
    def list_dirs(self, only_dirs=False):
        sftp_contents = self.client.listdir()
        
        if only_dirs:
            # get only items with no extension
            dirs = [item for item in sftp_contents if len(item.split(".")) == 1]
            return dirs
        else:
            return sftp_contents
    
    def create_download_dictionary(self):
        
        download_dict = {}
        
        to_download = self.list_dirs(only_dirs=True)
        for directory in to_download[0:1]:
            item_list = [f"{directory}/{item}" for item in self.client.listdir(directory) if len(item.split(".")) > 1]
            download_dict[directory] = item_list        
        
        return download_dict
        

    def download(self):
        download_dict = self.create_download_dictionary()
        for directory, file_list in download_dict.items():
            os.mkdir(directory)
            for file in file_list:
                self.client.get(file, file)        
        return

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

contents = my_sftp.list_dirs()


# testing the client attribute
# works fine by now
print(my_sftp.client.listdir())

# This will work
CLAVE = "Bioinfo%123"
HOST = "sftprelecov.isciii.es"
PUERTO = 22
USUARIO = "bioinfoadm"

my_sftp = SftpHandle(HOST, PUERTO, USUARIO, CLAVE)h