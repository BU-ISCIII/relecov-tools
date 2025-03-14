#!/usr/bin/env python
import os
import sys
import logging
import pyzipper
import json
import paramiko
import relecov_tools.utils
import relecov_tools.sftp_client
from secrets import token_hex
from datetime import datetime
from rich.console import Console
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = Console(stderr=True, style="dim", highlight=False, force_terminal=relecov_tools.utils.rich_force_colors())


class UploadSftp:
    def __init__(self, user=None, passwd=None, batch_id=None):
        """Starts the SFTP upload process"""
        log.info(f"Beginning upload process for batch: {batch_id}")

        if not batch_id:
            stderr.print("[red]Error: You must provide a batch_id as an argument.")
            sys.exit(1)

        config_json = ConfigJson()
        self.allowed_file_ext = config_json.get_topic_data(
            "sftp_handle", "allowed_file_extensions"
        )

        self.batch_id = batch_id
        self.sftp_user = user or relecov_tools.utils.prompt_text(msg="Enter the user id:")
        self.sftp_passwd = passwd or relecov_tools.utils.prompt_password(msg="Enter your password:")

        self.relecov_sftp = relecov_tools.sftp_client.SftpRelecov(
            username=self.sftp_user, password=self.sftp_passwd
        )

        stderr.print(f"User: {self.sftp_user}, Processing batch: {self.batch_id}")

        self.processed_batches = {}  # Dictionary to store results

    def find_cod_for_batch(self):
        """Find all COD-* folders containing the batch_id"""
        base_dir = os.getcwd() #This module should be run in root directory
        cod_dirs = [d for d in os.listdir(base_dir) if os.path.isdir(d) and d.startswith("COD-")]

        matching_cod = {}

        for cod in cod_dirs:
            batch_path = os.path.join(base_dir, cod, self.batch_id)
            if os.path.isdir(batch_path):
                matching_cod[cod] = {
                    "batch": self.batch_id,
                    "path": batch_path
                }

        if not matching_cod:
            stderr.print(f"[red]Batch {self.batch_id} was not found in any COD-* folder.")
            sys.exit(1)

        return matching_cod

    def compress_results(self, batch_data):
        """Compress the analysis_results folder with a random password"""
        batch_path = batch_data["path"]
        batch = batch_data["batch"]
        analysis_dir = os.path.join(batch_path, "analysis_results")

        if not os.path.exists(analysis_dir):
            stderr.print(f"[red]Folder analysis_results not found in {batch_path}")
            return None, None, None

        password = token_hex(8).encode()  # Creates a random password
        zip_filename = f"{batch}_results_{datetime.now().strftime('%Y%m%d%H%M%S')}.zip"
        zip_path = os.path.join(os.getcwd(), zip_filename)

        with pyzipper.AESZipFile(zip_path, 'w', compression=pyzipper.ZIP_DEFLATED, encryption=pyzipper.WZ_AES) as zipf:
            zipf.setpassword(password)  # The password is assigned to ZIP file
            for root, _, files in os.walk(analysis_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, analysis_dir)
                    zipf.write(file_path, arcname)

        stderr.print(f"[green]Compressed file: {zip_path} with password: {password.decode()}")
        return zip_path, password.decode(), zip_filename  # Converts the password to a string before returning it

    def upload_to_sftp(self, zip_path, cod):
        """Upload the compressed file to the SFTP server in the ANALYSIS_RESULTS folder inside the corresponding COD"""
        if not self.relecov_sftp.open_connection():
            stderr.print("[red]Could not connect to SFTP server")
            return False

        sftp = self.relecov_sftp.sftp  # Obtener el objeto SFTP

        # Define the remote path where the file is to be uploaded
        remote_dir = f"/{cod}/ANALYSIS_RESULTS"
        remote_file_path = f"{remote_dir}/{os.path.basename(zip_path)}"

        try:
            # Check if the ANALYSIS_RESULTS folder exists inside the COD
            try:
                sftp.chdir(remote_dir)  # Try to change to the directory
            except FileNotFoundError:
                stderr.print(f"[yellow]Directory {remote_dir} not found. Creating...")
                sftp.mkdir(remote_dir)

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
            stderr.print(f"[red]Unespected error: {e}")
            return False

        finally:
            self.relecov_sftp.close_connection()

    def execute_process(self):
        """Runs the complete flow: search, compress, upload and logging"""
        cod_batches = self.find_cod_for_batch()

        for cod, batch_data in cod_batches.items():
            stderr.print(f"Processing batch {batch_data['batch']} in {cod}")

            zip_path, password, zip_filename = self.compress_results(batch_data)
            if zip_path:
                success = self.upload_to_sftp(zip_path, cod)
                if success:
                    if cod not in self.processed_batches:
                        self.processed_batches[cod] = []

                    self.processed_batches[cod].append({
                        "batch": batch_data["batch"],
                        "archivo": zip_filename,
                        "contraseña": password,
                        "fecha": datetime.now().isoformat()
                    })

                    stderr.print(f"[green]Process completed for {batch_data['batch']} in {cod}.")