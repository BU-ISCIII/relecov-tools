"""
=============================================================
HEADER
=============================================================
INSTITUTION: BU-ISCIII
AUTHOR: Guillermo J. Gorines Cordero
MAIL: guillermo.gorines@urjc.es
VERSION: 0
CREATED: 7-3-2022
REVISED: 7-3-2022
REVISED BY: guillermo.gorines@urjc.es
DESCRIPTION:

    Includes the Email, and its associated methods.

REQUIREMENTS:
    -Python

TO DO:


================================================================
END_OF_HEADER
================================================================
"""

# Imports
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

class Email:
    def __init__(self, receiver, sender, password, subject):
        self.receiver = receiver
        self.sender = sender
        self.password = password
        self.subject = subject
        self.text = ""
        self.html = False

    def write_message(self, text):
        self.message = text
        return

    def send_message(self):
        msg = MIMEMultipart('alternative')
        msg['To'] = self.receiver
        msg['From'] = self.sender
        msg['Subject'] = self.subject

        s = smtplib.SMTP('localhost')
