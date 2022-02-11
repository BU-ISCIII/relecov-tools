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
-Method to check connection 

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
        
    def check_connection(self):
    '''
    Check if there is a SFTP connection
    Usage:
        SFTP_Connection_object.check_connection()
    Return:
        True if a connection still exists
        False if connection doesnt exist (not established or timed out for instance)
    '''
    try:
        self.client.getcwd()
        return True
    except:
        return False
    
    def open_connection(self):
    '''
    Uses the class attributes to make a SFTP connection
    Usage:
        SFTP_Connection_object.open_connection()
    Return:
        None (by now)
    '''
        if self.check_connection():
            self.client = paramiko.SSHClient()
                    self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    self.client.connect(hostname = self.host,
                                            port = self.port,
                                            username = self.user,
                                            password = self.key,
                                            allow_agent=False,
                                            look_for_keys=False)

                    self.client = self.client.open_sftp()


    def close_connection(self):
    '''
    Closes the SFTP connection if there is any
    Usage:
        SFTP_Connection_object.close_connection()
    Return:
        None (by now)
    '''
    if self.check_connection():
        self.client.close()


