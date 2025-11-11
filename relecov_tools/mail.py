import logging
import os
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from jinja2 import Environment, FileSystemLoader

import relecov_tools.utils


log = logging.getLogger(__name__)


class MailSendError(RuntimeError):
    """Raised when sending an email fails."""


class Mail:
    def __init__(self, config=None, template_path=None, logger=None):
        self.config = config
        self.template_path = template_path
        self.yaml_cred_path = self.config.get("yaml_cred_path")
        self.log = logger or log

        if not self.config:
            raise ValueError("Configuration not loaded correctly.")

        if not os.path.exists(self.template_path):
            raise FileNotFoundError(
                f"The template file could not be found in path {self.template_path}."
            )

    def get_institution_info(
        self, institution_code, institutions_file="institutions.json"
    ):
        """
        Load the institution's information from the JSON file.
        """
        institutions_file = self.config.get(
            "institutions_guide_path", "institutions_guide.json"
        )
        institutions_data = relecov_tools.utils.read_json_file(institutions_file)

        if institutions_data and institution_code in institutions_data:
            return institutions_data[institution_code]
        else:
            print(f"No information found for code {institution_code}")
            self.log.warning(f"No information found for code {institution_code}")
            return None

    def render_email_template(
        self,
        additional_info="",
        invalid_count=None,
        submitting_institution_code=None,
        template_name=None,
        batch=None,
        password=None,
        zip_filename=None,
    ):

        institution_info = self.get_institution_info(submitting_institution_code)
        if not institution_info:
            raise ValueError("Error: Institution information could not be obtained.")

        institution_name = institution_info["institution_name"]

        template_vars_dict = {
            "submitting_institution": institution_name,
            "invalid_count": (
                invalid_count.get(submitting_institution_code, 0)
                if invalid_count
                else 0
            ),
            "additional_info": additional_info,
            "batch": batch,
            "password": password,
            "submitting_institution_code": submitting_institution_code,
            "zip_filename": zip_filename,
        }

        templates_base_dir = os.path.dirname(self.template_path)
        env = Environment(loader=FileSystemLoader(templates_base_dir))
        template = env.get_template(template_name)
        email_template = template.render(**template_vars_dict)

        return email_template

    def send_email(self, receiver_email, subject, body, attachments, email_psswd):

        if not isinstance(receiver_email, list):
            raise ValueError(
                f"receiver_emails should be a list, but it received: {type(receiver_email)}"
            )

        if not all(isinstance(email, str) for email in receiver_email):
            raise ValueError("All elements in receiver_emails must be strings.")

        self.log.info(
            "Preparing to send email to %s with subject '%s'",
            ", ".join(receiver_email),
            subject,
        )

        credentials = relecov_tools.utils.read_yml_file(self.yaml_cred_path)
        if not credentials:
            message = "No email credentials found."
            self.log.error(message)
            raise MailSendError(message)

        sender_email = self.config["email_host_user"]

        email_password = (
            email_psswd if email_psswd else credentials.get("email_password")
        )

        if not email_password:
            message = "The e-mail password could not be found."
            self.log.error(message)
            raise MailSendError(message)

        default_cc = "bioinformatica@isciii.es"
        msg = MIMEMultipart()
        msg["From"] = sender_email
        msg["To"] = ", ".join(receiver_email)
        msg["CC"] = default_cc
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain"))

        for attachment in attachments:
            with open(attachment, "rb") as attachment_file:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment_file.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(attachment)}",
                )
                msg.attach(part)
        all_recipients = receiver_email + [default_cc]
        try:
            server = smtplib.SMTP(self.config["email_host"], self.config["email_port"])
            server.starttls()
            server.login(sender_email, email_password)
            server.sendmail(sender_email, all_recipients, msg.as_string())
            server.quit()
            self.log.info("Mail sent successfully.")
            print("Mail sent successfully.")
        except smtplib.SMTPException as e:
            message = f"Error sending the mail to {receiver_email}: {e}"
            self.log.error(message)
            raise MailSendError(message) from e
