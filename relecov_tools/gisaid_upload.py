import logging
import sys
import json

# from pyparsing import col
import rich.console

import pandas as pd
import os

# import ftplib
import relecov_tools.utils
from Bio import SeqIO

# from relecov_tools.config_json import ConfigJson


# import site


log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class GisaidUpload:
    def __init__(
        self,
        user=None,
        passwd=None,
        client_id=None,
        token=None,
        gisaid_json=None,
        fasta_path=None,
        output_path=None,
        frameshift=None,
        proxy_config=None,
        single=False,
    ):
        if (
            token is None
        ):  # borrar comentario: solo si no existe el token necesita user, passwd y client_id
            self.token = None
            print("Token is not introduced, creating a new one...")
            if user is None:
                self.user = relecov_tools.utils.prompt_text(
                    msg="Enter your username defined in GISAID"
                )
            else:
                self.user = user
            if passwd is None:
                self.passwd = relecov_tools.utils.prompt_password(
                    msg="Enter your password to GISAID"
                )
            else:
                self.passwd = passwd
            if client_id is None:
                self.client_id = relecov_tools.utils.prompt_password(
                    msg="Enter your client-ID to GISAID. Email clisupport@gisaid.org to request client-ID"
                )
            else:
                self.client_id = client_id
        else:
            self.token = token
        if gisaid_json is None:
            self.gisaid_json = relecov_tools.utils.prompt_path(
                msg="Select the GISAID json file to upload"
            )
        else:
            self.gisaid_json = self.gisaid_json
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the log files"
            )
        else:
            self.output_path = output_path
        if fasta_path is None:
            self.fasta_path = relecov_tools.utils.prompt_path(msg="Select path to fasta file/s")
        else:
            self.fasta_path = fasta_path
        if frameshift is None:
            self.frameshift = relecov_tools.utils.prompt_selection(
                msg="Select frameshift notification",
                choices=["catch_all", "catch_novel", "catch_none"]
            )
        else:
            self.frameshift = frameshift
        # Add proxy settings: username:password@proxy:port (optional)
        if proxy_config is None:
            # borrar comentario: este mensaje no me convence
            print("Proxy configuration is not set")
        else:
            self.proxy_config = proxy_config
        self.single = single

    # Metadatos

    def metadata_to_csv(self):
        "Transform metadata json to csv"
        data = relecov_tools.utils.read_json_file(self.gisaid_json)
        df_data = pd.DataFrame(data)
        df_data_path = os.path.join(self.output_path, "meta_gisaid.csv")
        df_data.to_csv(df_data_path)
        metagisaid = df_data_path
        return metagisaid

    # generar template con cli3
    # ADD TOKEN WARNING and file token  .authtoken
    # add bash from cli3
"""
    os.system(
        "cli3 upload --database EpiCoV --token ./gisaid.authtoken --metadata gisaid_template.csv  --fasta multi.fasta --frameshift (OPTIONAL, default: catch_all) --failed --proxy --log"
    )
    cli3 upload
    --database EpiCoV
    --token ./gisaid.authtoken
    --metadata gisaid_template.csv
    --fasta multi.fasta
    --frameshift (OPTIONAL, default: catch_all)
    --failed default creates file failed.out where the failed records will be
    --proxy
    --log default creates file failed.out where the log will be )
    
"""
    def create_multifasta(self):
        """Create multifasta from single fastas (if --single)"""
        if self.single:
            os.system(
                "cat %s > %s/multifasta.fasta" % (self.fasta_path, self.output_path)
            )
            multifasta = "%s/multifasta.fasta" % self.output_path
        else:
            multifasta = self.fasta_path
        return multifasta

    def change_headers(self, multifasta):
        """Transform multifasta ids/headers to GISAID format"""
        data = relecov_tools.utils.read_json_file(self.gisaid_json)
        virus_name = [name["covv_virus_name"] for name in data]
        multi_gis_path = os.path.join(self.output_path, "processed_multifasta_gisaid.fasta")
        with open(multifasta) as old_fasta, open(
            multi_gis_path, "a"
        ) as new_fasta:
            records = SeqIO.parse(old_fasta, "fasta")
            for record in records:
                for name in virus_name:
                    if record.id == name.split("/")[-2]:
                        record.id = name
            SeqIO.write(record, new_fasta, "fasta")
        fastagisaid = "%s/multifasta_gisaid.fasta" % self.output_path
        return fastagisaid

    def cli3_auth(self):
        """Create authenticate token"""
        os.system(
            "cli3 authenticate --username %s --password %s --client_id %s"
            % (self.user, self.passwd, self.client_id)
        )

    def cli3_upload(self):
        """Upload to GISAID"""
        if self.proxy_config is None:
            os.system(
                "cli3 upload --token %s --metadata %s --fasta %s --frameshift %s"
                % (
                    self.token,
                    self.metadata_to_csv(),
                    self.change_headers(self.create_multifasta()),
                    self.frameshift,
                )
            )
        else:
            os.system(
                "cli3 upload --token %s --metadata %s --fasta %s --frameshift %s --proxy %s"
                % (
                    self.token,
                    self.metadata_to_csv(),
                    self.change_headers(),
                    self.frameshift,
                    self.proxy_config,
                )
            )


    def gisaid_upload(self):
        """Upload to GISAID"""
        if token is None:
            self.cli3_auth()
        self.cli3_upload()


    """"
    Upload
    Subir con cli3
    Token
    Opción de configurar proxy
    def upload(self):
    Create the required files and upload to ENA
    self.convert_input_json_to_ena()
    self.create_structure_to_ena()
    """
