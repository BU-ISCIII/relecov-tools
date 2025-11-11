import logging
import os
import shutil
import smtplib

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

from jinja2 import Environment, FileSystemLoader

import relecov_tools.utils
from relecov_tools.base_module import BaseModule
from relecov_tools.config_json import ConfigJson
from relecov_tools.log_summary import LogSum


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

        default_cc = "abernabeu@externos.isciii.es"
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


class MailModule(BaseModule):
    """Execute the send-mail workflow with BaseModule logging."""

    def __init__(
        self,
        *,
        validate_file,
        receiver_email=None,
        attachments=None,
        template_path=None,
        email_psswd=None,
        additional_info="",
        template_name=None,
        stderr=None,
    ):
        super().__init__(called_module="mail")
        self.validate_file = validate_file
        self.receiver_email = receiver_email
        self.attachments = [str(att) for att in (attachments or [])]
        self.template_path = template_path
        self.email_psswd = email_psswd
        self.additional_info = additional_info or ""
        self.template_name = template_name
        self.stderr = stderr
        self.config_loader = ConfigJson(extra_config=True)
        self.config = self.config_loader.get_configuration("mail_sender")
        if not self.config:
            raise ValueError(
                "Error: The configuration for 'mail_sender' could not be loaded."
            )
        self.batch_dir = os.path.dirname(os.path.abspath(self.validate_file))
        self.batch_name = os.path.basename(self.batch_dir)

    def execute(self):
        self.log.info(f"Mail log stored at {self.final_log_path}")
        if self.stderr:
            self.stderr.print(
                f"[blue]Log file stored in configured path: {self.final_log_path}"
            )
        validate_data = relecov_tools.utils.read_json_file(self.validate_file)
        if not validate_data:
            raise ValueError("Error: Validation data could not be loaded.")

        submitting_code = self._get_first_key(validate_data)
        invalid_count = LogSum.get_invalid_count(validate_data)
        template_path = self._resolve_template_path()
        mail_client = Mail(self.config, template_path, logger=self.log)

        institution_info = mail_client.get_institution_info(submitting_code)
        if not institution_info:
            raise ValueError("Error: Could not obtain institution information.")

        email_body = mail_client.render_email_template(
            additional_info=self.additional_info,
            invalid_count=invalid_count,
            submitting_institution_code=submitting_code,
            template_name=self._resolve_template_name(),
            batch=self.batch_name,
        )

        if email_body is None:
            raise RuntimeError("Error: Could not generate mail.")

        final_receiver_email = self._resolve_receiver_email(
            self.receiver_email, institution_info.get("email_receiver", "")
        )
        if not final_receiver_email:
            raise ValueError("Error: Could not obtain the recipient's email address.")

        subject = (
            f"RELECOV - Informe de Validación de Muestras {self.batch_name} - "
            f"{institution_info['institution_name']}"
        )

        self.set_batch_id(self.batch_name)
        mail_client.send_email(
            final_receiver_email,
            subject,
            email_body,
            self.attachments,
            self.email_psswd,
        )

        batch_log = self._copy_log_to_batch()
        if batch_log:
            self.log.info(f"Mail log copied to batch folder: {batch_log}")
            if self.stderr:
                self.stderr.print(f"[green]Log copied to batch folder: {batch_log}")
        else:
            self.log.warning(
                f"Mail log could not be copied to batch folder {self.batch_dir}. Check permissions."
            )
            if self.stderr:
                self.stderr.print(
                    f"[red]Warning: log could not be copied to {self.batch_dir}. Check permissions."
                )

        return {
            "log_path": self.final_log_path,
            "batch_log_path": batch_log,
        }

    @staticmethod
    def _get_first_key(data):
        try:
            return list(data.keys())[0]
        except (AttributeError, IndexError):
            raise ValueError("Error: Could not determine submitting institution.")

    def _resolve_template_path(self):
        template_path = self.template_path or self.config.get(
            "delivery_template_path_file"
        )
        if not template_path or not os.path.exists(template_path):
            raise FileNotFoundError(
                "The template path could not be determined or does not exist. "
                "Please provide it via --template_path or define 'delivery_template_path_file' in the configuration."
            )
        return template_path

    def _resolve_template_name(self):
        if not self.template_name:
            raise ValueError("Error: Template name was not provided.")
        return self.template_name

    def _resolve_receiver_email(self, provided, default_from_json):
        if not provided:
            return [
                email.strip() for email in default_from_json.split(";") if email.strip()
            ]
        if isinstance(provided, str):
            return [email.strip() for email in provided.split(";") if email.strip()]
        if isinstance(provided, (list, tuple)):
            receivers = [str(email).strip() for email in provided if str(email).strip()]
            return receivers
        raise ValueError("Error: Invalid receiver_email format. Expect string or list.")

    def _copy_log_to_batch(self):
        log_file = self.get_log_file()
        if not log_file or not os.path.isfile(log_file):
            return None
        os.makedirs(self.batch_dir, exist_ok=True)
        destination = os.path.join(self.batch_dir, os.path.basename(log_file))
        if os.path.realpath(destination) == os.path.realpath(log_file):
            return destination
        try:
            shutil.copyfile(log_file, destination)
            return destination
        except OSError:
            return None


def run_mail(**kwargs):
    """Helper to execute the mail module from CLI."""
    module = MailModule(**kwargs)
    return module.execute()
