'''
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

    Includes the SFTP connection class, and its methods.

REQUIREMENTS:
    -Python
    -Paramiko

TO DO:

-Check minimal required Python version

================================================================
END_OF_HEADER
================================================================
'''

# Imports

import paramiko

class SFTP_Connection:
    def __init__(self,host,port,user,key):
    '''
    Initializes the Connection object and starts its host, port, user and key attributes.
    Declaration:
        SFTP_Connection_object = SFTP_Connection(host,port,user,key)
    '''

    self.host = host
    self.port = port
    self.user = user
    self.key = key
    self.client = None       
        
    def open(self):
    '''
    Uses the class attributes to make a connection
    Usage:
        SFTP_Connection_object = 
    '''
        self.client = paramiko.SSHClient()
                self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                self.client.connect(hostname = self.host,
                                        port = self.port,
                                        username = self.user,
                                        password = self.key,
                                        allow_agent=False,
                                        look_for_keys=False)

                self.client = self.client.open_sftp()


