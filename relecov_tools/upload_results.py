#!/usr/bin/env python
import os
import logging
import pyzipper
import json
import relecov_tools.utils
import relecov_tools.sftp_client
import relecov_tools.mail
from secrets import token_hex
from datetime import datetime
from rich.console import Console
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class UploadSftp:
    def __init__(
        self,
        user=None,
        passwd=None,
        batch_id=None,
        template_path=None,
        project="Relecov",
    ):
        """Starts the SFTP upload process"""
        log.info(f"Beginning upload process for batch: {batch_id}")

        if not batch_id:
            raise ValueError("Error: You must provide a batch_id as an argument.")

        if not template_path:
            raise ValueError("Error: You must provide a template_path as an argument.")

        config_json = ConfigJson()
        config = config_json.get_configuration("mail_sender")
        sftp_config = config_json.get_configuration("sftp_handle")
        self.allowed_file_ext = config_json.get_topic_data(
            "sftp_handle", "allowed_file_extensions"
        )

        self.batch_id = batch_id
        self.sftp_user = user or relecov_tools.utils.prompt_text(
            msg="Enter the user id:"
        )
        self.sftp_passwd = passwd or relecov_tools.utils.prompt_password(
            msg="Enter your password:"
        )

        self.relecov_sftp = relecov_tools.sftp_client.SftpRelecov(
            username=self.sftp_user, password=self.sftp_passwd
        )

        stderr.print(f"User: {self.sftp_user}, Processing batch: {self.batch_id}")

        self.processed_batches = {}  # Dictionary to store results
        self.template_path = template_path
        self.email_sender = relecov_tools.mail.EmailSender(config, template_path)
        self.guide = config.get("institutions_guide_path")
        self.analysis_folder = sftp_config.get("analysis_results_folder")
        self.project = project
        if self.project not in ["Relecov", "Redlabra"]:
            raise ValueError("Error: Only valid projects: Relecov / Redlabra.")

    def find_cod_for_batch(self):
        """Find all COD* folders containing the batch_id"""
        base_dir = os.getcwd()  # This module should be run in root directory
        cod_dirs = [
            d for d in os.listdir(base_dir) if os.path.isdir(d) and d.startswith("COD")
        ]

        matching_cod = {}

        for cod in cod_dirs:
            batch_path = os.path.join(base_dir, cod, self.batch_id)
            if os.path.isdir(batch_path):
                matching_cod[cod] = {"batch": self.batch_id, "path": batch_path}

        if not matching_cod:
            raise FileNotFoundError(
                f"Batch {self.batch_id} was not found in any COD* folder."
            )

    def compress_results(self, batch_data, cod):
        """Compress the analysis_results folder with a random password"""
        batch_path = batch_data["path"]
        batch = batch_data["batch"]
        analysis_dir = os.path.join(batch_path, "analysis_results")

        if not os.path.exists(analysis_dir):
            stderr.print(f"[red]Folder analysis_results not found in {batch_path}")
            return None, None, None

        password = token_hex(8).encode()  # Creates a random password
        zip_filename = f"{cod}_{batch}_analysis_results_{self.project}_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
        zip_path = os.path.join(batch_path, zip_filename)

        with pyzipper.AESZipFile(
            zip_path, "w", compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES
        ) as zipf:
            zipf.setpassword(password)  # The password is assigned to ZIP file
            for root, _, files in os.walk(analysis_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, analysis_dir)
                    if arcname.startswith("."):
                        arcname = arcname[1:]  # Remove leading dot if present
                    zipf.write(file_path, arcname)

        stderr.print(
            f"[green]Compressed file: {zip_path} with password: {password.decode()}"
        )
        return (
            zip_path,
            password.decode(),
            zip_filename,
        )  # Converts the password to a string before returning it

    def upload_to_sftp(self, zip_path, cod):
        """Upload the compressed file to the SFTP server in the ANALYSIS_RESULTS folder inside the corresponding COD"""
        if not self.relecov_sftp.open_connection():
            stderr.print("[red]Could not connect to SFTP server")
            return False

        sftp = self.relecov_sftp.sftp

        # Define the remote path where the file is to be uploaded
        remote_dir = f"/{cod}/{self.analysis_folder}"
        remote_file_path = f"{remote_dir}/{os.path.basename(zip_path)}"

        try:
            # Check if the ANALYSIS_RESULTS folder exists inside the COD
            try:
                sftp.chdir(remote_dir)  # Try to change to the directory
            except FileNotFoundError:
                stderr.print(
                    f"[red]Directory {remote_dir} not found. Failed to upload file to sftp."
                )
                return False

            # Upload the compressed file
            stderr.print(f"[blue]Uploading {zip_path} to {remote_file_path} in SFTP...")
            success = self.relecov_sftp.upload_file(zip_path, remote_file_path)

            if success:
                stderr.print(f"[green]File successfully uploaded to {remote_file_path}")
                return True
            else:
                stderr.print(f"[red]Error uploading {zip_path} file")
                return False

        except Exception as e:
            log.error(f"Error uploading file to SFTP: {e}")
            stderr.print(f"[red]Unexpected error: {e}")
            return False

        finally:
            self.relecov_sftp.close_connection()

    def notify_lab(
        self,
        batch_name,
        lab_code,
        receiver_email,
        email_psswd=None,
        zip_passwd=None,
        zip_filename=None,
    ):
        """
        Sends an email notification to the laboratory with results details.
        """
        # Render the email content using the Jinja2 template
        email_body = self.email_sender.render_email_template(
            additional_info=f"Batch {batch_name} is ready.",
            submitting_institution_code=lab_code,
            template_name="jinja_template_results.j2",
            batch=batch_name,
            password=zip_passwd,
            zip_filename=zip_filename,
        )

        # Send the email
        self.email_sender.send_email(
            receiver_email=receiver_email,
            subject=f"Batch {batch_name} - Resultados del análisis",
            body=email_body,
            attachments=[],
            email_psswd=email_psswd,
        )

        print(f"Notification sent to {receiver_email} for batch {batch_name}.")

    def execute_process(self):
        """Runs the complete flow: search, compress, upload, logging and sending the email."""
        cod_batches = self.find_cod_for_batch()

        with open(self.guide, "r") as f:
            institutions_data = json.load(f)

        for cod, batch_data in cod_batches.items():
            stderr.print(f"Processing batch {batch_data['batch']} in {cod}")

            zip_path, password, zip_filename = self.compress_results(batch_data, cod)
            if zip_path:
                success = self.upload_to_sftp(zip_path, cod)
                if success:
                    if cod not in self.processed_batches:
                        self.processed_batches[cod] = []

                    self.processed_batches[cod].append(
                        {
                            "batch": batch_data["batch"],
                            "archivo": zip_filename,
                            "contraseña": password,
                            "fecha": datetime.now().isoformat(),
                        }
                    )

                    stderr.print(
                        f"[green]Process completed for {batch_data['batch']} in {cod}."
                    )

                    institution_info = institutions_data.get(cod)

                    if institution_info:
                        # After successful upload, send the results email
                        receiver_email = [institution_info["email_receiver"]]
                        lab_code = cod
                        self.notify_lab(
                            batch_data["batch"],
                            lab_code,
                            receiver_email,
                            zip_passwd=password,
                            zip_filename=zip_filename,
                        )
