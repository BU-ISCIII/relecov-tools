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
                msg="Enter your username defined in ENA"
            )
        else:
            self.user = user
        if passwd is None:
            self.passwd = relecov_tools.utils.prompt_password(
                msg="Enter your password to ENA"
            )
        else:
            self.passwd = passwd
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the xml files"
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
            self.fasta_path = relecov_tools.utils.prompt_path(msg="Select path")
        else:
            # relecov_tools/gisaid_upload.py
            self.fasta_path = fasta_path

        if not os.path.isfile(self.source_json_file):
            log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"[red]json data file {self.source_json_file} does not exist")
            sys.exit(1)
            with open(self.source_json_file, "r") as fh:
                self.json_data = json.loads(fh.read())

    def convert_input_json_to_ena(self):
        """Split the input ena json, in samples and runs json"""
        pass

    # Metadatos

    def metadata_to_csv(self):
        "Transform metadata json to csv"
        data = relecov_tools.utils.read_json_file(self.metadata)
        df_data = pd.DataFrame(data)
        df_data.to_csv("meta_gisaid.csv")

    # generar template con cli3
    # ADD TOKEN WARNING and file token  .authtoken
    # add bash from cli3

    os.system(
        "cli3 upload --database EpiCoV --token ./gisaid.authtoken --metadata gisaid_template.csv  --fasta multi.fasta --frameshift (OPTIONAL, default: catch_all) --failed --proxy --log"
    )
    """
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

    # Upload
    # Subir con cli3

    # def upload(self):
    # """Create the required files and upload to ENA"""
    # self.convert_input_json_to_ena()
    # self.create_structure_to_ena()
