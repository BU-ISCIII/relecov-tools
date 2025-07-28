import logging

# from pyparsing import col
import rich.console
import sys
import pandas as pd
import os

import relecov_tools.utils
from Bio import SeqIO
from relecov_tools.config_json import ConfigJson


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
        password=None,
        client_id=None,
        token=None,
        gisaid_json=None,
        input_path=None,
        output_dir=None,
        frameshift=None,
        proxy_config=None,
        single=False,
        gzip=False,
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
            if password is None:
                self.passwd = relecov_tools.utils.prompt_password(
                    msg="Enter your password to GISAID"
                )
            else:
                self.passwd = password
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
            self.gisaid_json = gisaid_json
        if output_dir is None:
            self.output_dir = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the log files"
            )
        else:
            self.output_dir = output_dir
        if input_path is None:
            self.fasta_path = relecov_tools.utils.prompt_path(
                msg="Select path to fasta file/s"
            )
        else:
            self.fasta_path = input_path
        if frameshift is None:
            self.frameshift = relecov_tools.utils.prompt_selection(
                msg="Select frameshift notification",
                choices=["catch_all", "catch_novel", "catch_none"],
            )
        else:
            self.frameshift = frameshift
        # Add proxy settings: username:password@proxy:port (optional)
        if proxy_config is None:
            # borrar comentario: este mensaje no me convence
            self.proxy_config = None
            print("Proxy configuration is not set")
        else:
            self.proxy_config = proxy_config
        self.single = single
        self.gzip = gzip

    # Metadatos

    def complete_mand_fields(self, dataframe):
        """Complete mandatory empty fields with 'unknown'"""
        dataframe.loc[dataframe["covv_gender"] == "", "covv_gender"] = "unknown"
        dataframe.loc[dataframe["covv_patient_age"] == "", "covv_patient_age"] = (
            "unknown"
        )

        authors = [authors_field for authors_field in dataframe["covv_authors"]]
        if "" in authors or "unknown" in authors:
            log.error("Invalid value for author. This field is required in full")
            stderr.print(
                "[red] Invalid value for authors. This field is required in full, 'unknown' is not allowed"
            )
            sys.exit(1)

        dataframe.loc[dataframe["covv_subm_lab_addr"] == "", "covv_subm_lab_addr"] = (
            "unknown"
        )
        dataframe.loc[dataframe["covv_subm_lab"] == "", "covv_subm_lab"] = "unknown"
        dataframe.loc[dataframe["covv_orig_lab_addr"] == "", "covv_orig_lab_addr"] = (
            "unknown"
        )
        dataframe.loc[dataframe["covv_orig_lab"] == "", "covv_orig_lab"] = "unknown"
        dataframe.loc[dataframe["covv_patient_status"] == "", "covv_patient_status"] = (
            "unknown"
        )
        dataframe.loc[dataframe["covv_type"] == "", "covv_type"] = "betacoronavirus"
        dataframe.loc[dataframe["covv_passage"] == "", "covv_passage"] = "Original"

        config_json = ConfigJson()
        gisaid_config = config_json.get_configuration("upload_to_gisaid")[
            "GISAID_configuration"
        ]
        submitter_id = gisaid_config["submitter"]
        dataframe.loc[dataframe["submitter"] == "", "submitter"] = submitter_id

        bioinfo_config = config_json.get_configuration("bioinfo_analysis")
        assembly_method = bioinfo_config["fixed_values"][
            "bioinformatics_protocol_software_name"
        ]
        dataframe.loc[
            dataframe["covv_assembly_method"] == "", "covv_assembly_method"
        ] = assembly_method

        return dataframe

    def metadata_to_csv(self):
        """Transform metadata json to csv"""
        data = relecov_tools.utils.read_json_file(self.gisaid_json)
        df_data = pd.DataFrame(data)

        config_json = ConfigJson()
        fields = config_json.get_configuration("upload_to_gisaid")["gisaid_csv_headers"]

        col_df = list(df_data.columns)
        for field in fields:
            if field not in col_df:
                df_data.insert(4, field, "")

        config_lab_json = ConfigJson()
        lab_json_conf = config_lab_json.get_topic_data(
            "read_lab_metadata", "laboratory_data"
        )
        lab_json_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "conf", lab_json_conf["file"]
        )
        lab_json = relecov_tools.utils.read_json_file(lab_json_file)
        for lab in lab_json:
            for i in range(len(df_data)):
                if lab["collecting_institution"] == df_data["covv_orig_lab"][i]:
                    df_data["covv_location"][i] = " / ".join(
                        [
                            "Europe",
                            lab["geo_loc_country"],
                            lab["geo_loc_state"],
                            lab["geo_loc_city"],
                        ]
                    )

        df_data.replace("not provided", "unknown", inplace=True)
        df_data_comp = self.complete_mand_fields(df_data)
        df_data_path = os.path.join(self.output_dir, "meta_gisaid.csv")
        if not os.path.exists(self.output_dir):
            os.mkdir(self.output_dir)
        df_data_comp.to_csv(df_data_path, index=False)
        return df_data_path

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
            gather_fastas_path = os.path.join(self.fasta_path, "*.fa*")
            if self.gzip:
                os.system(
                    "zcat %s > %s/multifasta.fasta"
                    % (gather_fastas_path, self.output_dir)
                )
            else:
                os.system(
                    "cat %s > %s/multifasta.fasta"
                    % (gather_fastas_path, self.output_dir)
                )
            multifasta = "%s/multifasta.fasta" % self.output_dir

        else:
            if self.gzip:
                os.system(
                    "zcat %s > %s/multifasta.fasta" % (self.fasta_path, self.output_dir)
                )
                multifasta = "%s/multifasta.fasta" % self.output_dir
            else:
                multifasta = self.fasta_path
        return multifasta

    def change_headers(self, multifasta):
        """Transform multifasta ids/headers to GISAID format"""
        data = relecov_tools.utils.read_json_file(self.gisaid_json)
        virus_name = [name["covv_virus_name"] for name in data]
        multi_gis_path = os.path.join(
            self.output_dir, "processed_multifasta_gisaid.fasta"
        )
        with open(multifasta) as old_fasta, open(multi_gis_path, "a") as new_fasta:
            records = SeqIO.parse(old_fasta, "fasta")
            for record in records:
                for name in virus_name:
                    if name.split("/")[-2].split("-")[-1] in record.id:
                        record.id = name
                        record.description = name
                        SeqIO.write(record, new_fasta, "fasta")
        return multi_gis_path

    def cli3_auth(self):
        """Create authenticate token"""
        os.system(
            "cli3 authenticate --username %s --password %s --client_id %s"
            % (self.user, self.passwd, self.client_id)
        )
        self.token = "gisaid.authtoken"

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
                    self.change_headers(self.create_multifasta()),
                    self.frameshift,
                    self.proxy_config,
                )
            )

    def gisaid_upload(self):
        """Upload to GISAID"""
        if self.token is None:
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
