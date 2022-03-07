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

    def check(self):
        """
        Check if there is a SFTP connection
        Usage:
            sftp.check()
        Return:
            True if a connection still exists
            False if connection doesnt exist (not established or timed out for instance)
        """
        try:
            self.client.getcwd()
            return True
        except:
            return False

    def open(self):
        """
        Uses the class attributes to make a SFTP connection
        Usage:
            sftp.open()
        Return:
            True if connected succesfully
            False if failed connection
        """
        if not self.check():
            try:
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

                self.client = self.client.open()

                return True
            except:
                return False

    def close(self):
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

    def 


if __name__ == "__main__":
    sys.exit(main())


# TESTING ZONE, must be deleted later
CLAVE = "RANDOM_KEY_FOR_TESTING"
HOST = "RANDOM_SFTP_FOR_TESTING"
PUERTO = 420
USUARIO = "ARTURITO"

my_sftp = SftpHandle(HOST, PUERTO, USUARIO, CLAVE)
if not my_sftp.open():
    print("No connection!")
else:
    print(my_sftp.check())

# testing the client attribute
# works fine by now
print(my_sftp.client.listdir())
