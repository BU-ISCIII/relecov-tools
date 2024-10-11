import json
import logging
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

log = logging.getLogger(__name__)

class EmailSender:
    def __init__(self, validate_file, lab_info_file):
        self.validate_file = validate_file
        self.lab_info_file = lab_info_file

    def get_invalid_count(self):
        invalid_count = 0

        try:
            with open(self.validate_file, "r") as f:
                validate_data = json.load(f)

                for entry_key, entry_value in validate_data.items():
                    if "samples" in entry_value:
                        samples = entry_value["samples"]
                        for sample_key, sample_value in samples.items():
                            if "valid" in sample_value and not sample_value["valid"]:
                                invalid_count += 1
            return invalid_count
        except Exception as e:
            log.error("Error reading validate file: %s", e)
            return None

    def send_email(self, receiver_email, subject, body, attachments):
        sender_email = "solmos.buisciii@gmail.com" 
        sender_password = "nmqm oorh egkf yvbo" 

        try:
            msg = MIMEMultipart()
            msg["From"] = sender_email
            msg["To"] = receiver_email
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))
            
            for attachment in attachments: 
                try:
                    with open(attachment, "rb") as attachment_file:
                        part= MIMEBase("application", "octet-stream")
                        part.set_payload(attachment_file.read())
                        encoders.encode_base64(part)
                        part.add_header("Content-Disposition",
                                        f"attachment; filename={os.path.basename(attachment)}"
                        )
                        msg.attach(part)
                except Exception as e:
                    log.error(f"Error when attaching the file {attachment}: {e}")
                    print(f"Error when attaching the file {attachment}: {e}")

            server = smtplib.SMTP("smtp.gmail.com", 587)
            server.starttls()
            server.login(sender_email, sender_password)

            server.sendmail(sender_email, receiver_email, msg.as_string())
            server.quit()

            print("Mail sent successfully.")
        except Exception as e:
            log.error(f"Error sending mail: {e}")
            print(f"Error sending mail: {e}")