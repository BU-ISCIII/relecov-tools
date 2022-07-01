import logging

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
        gisaid_json=None,
        fasta_path=None,
        output_path=None,
    ):
        if user is None:
            self.user = relecov_tools.utils.prompt_text(
                msg="Enter your username defined in GISAID"
            )
        else:
            self.user = user
        # Add proxy settings: username:password@proxy:port (optional)
        if passwd is None:
            self.passwd = relecov_tools.utils.prompt_password(
                msg="Enter your password to GISAID"
            )
        else:
            self.passwd = passwd
        if self.source_json is None:
            self.source_json_file = relecov_tools.utils.prompt_path(
                msg="Select the GISAID json file to upload"
            )
        else:
            self.source_json_file = self.source_json
        if self.customized_project is None:
            self.customized_project = None
        else:
            self.customized_project = self.customized_project
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the log files"
            )
        else:
            self.output_path = output_path
        if gisaid_json is None:
            self.gisaid_json = relecov_tools.utils.prompt_path(
                msg="Select metadata json file"
            )
        else:
            self.gisaid_json = gisaid_json
        if fasta_path is None:
            self.fasta_path = relecov_tools.utils.prompt_path(
                msg="Select metadata json file"
            )
        else:
            self.fasta_path = fasta_path

    def convert_input_json_to_ena(self):
        """Split the input ena json, in samples and runs json"""
        pass

    # Metadatos

    def metadata_to_csv(self):
        "Transform metadata json to csv"
        data = relecov_tools.utils.read_json_file(self.metadata)
        df_data = pd.DataFrame(data)
        df_data.to_csv("meta_gisaid.csv")

    # Sequences
    # Unificar en multifasta
    def create_multifasta(self):
        """Create multifasta from single fastas"""
        os.system(
            "cat %s/*.fasta > %s/multifasta.fasta" % (self.fasta_path, self.output_path)
        )
        multifasta = "%s/multifasta.fasta" % self.output_path
        return multifasta

    def change_headers(self, multifasta):
        """Transform multifasta ids/headers to GISAID format"""
        data = relecov_tools.utils.read_json_file(self.gisaid_json)
        virus_name = [name["covv_virus_name"] for name in data]
        with open(multifasta) as old_fasta, open(
            "%s/multifasta_gisaid.fasta" % self.output_path, "w"
        ) as new_fasta:
            records = SeqIO.parse(old_fasta, "fasta")
            for record in records:
                for name in virus_name:
                    if record.id == name.split("/")[-2]:
                        record.id = name
            SeqIO.write(record, new_fasta, "fasta")

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