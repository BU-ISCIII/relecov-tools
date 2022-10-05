# from distutils.errors import LibError
import logging

# from re import template

# from pyparsing import col
import xml.etree.ElementTree as ET
import rich.console
import json


import pandas as pd


import sys
import os

import ftplib
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

from ena_upload.ena_upload import extract_targets

# from ena_upload.ena_upload import submit_data
from ena_upload.ena_upload import run_construct
from ena_upload.ena_upload import construct_submission
from ena_upload.ena_upload import send_schemas
from ena_upload.ena_upload import process_receipt
from ena_upload.ena_upload import update_table

# from ena_upload.ena_upload import make_update
# from ena_upload.ena_upload import process_receipt

# from ena_upload.ena_upload import save_update
import site

pd.options.mode.chained_assignment = None

template_path = os.path.join(site.getsitepackages()[0], "ena_upload", "templates")
template_path = os.path.join(os.getcwd(), "relecov_tools/templates")

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class EnaUpload:
    def __init__(
        self,
        user=None,
        passwd=None,
        center=None,
        source_json=None,
        dev=None,
        customized_project=None,
        action=None,
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
        if center is None:
            self.center = relecov_tools.utils.prompt_text(msg="Enter your center name")
        else:
            self.center = center
        if source_json is None:
            self.source_json_file = relecov_tools.utils.prompt_path(
                msg="Select the ENA json file to upload"
            )
        else:
            self.source_json_file = source_json
        if dev is None:
            self.dev = relecov_tools.utils.prompt_yn_question(
                msg="Do you want to test upload data?"
            )
        else:
            self.dev = dev

        if customized_project is None:
            self.customized_project = None
        else:
            self.customized_project = customized_project
        if action is None:
            self.action = relecov_tools.utils.prompt_selection(
                msg="Select the action to upload to ENA",
                choices=["add", "modify", "cancel", "release"],
            )
        else:
            self.action = action.upper()
        if output_path is None:
            self.output_path = relecov_tools.utils.prompt_path(
                msg="Select the folder to store the xml files"
            )
        else:
            self.output_path = output_path

        if not os.path.isfile(self.source_json_file):
            log.error("json data file %s does not exist ", self.source_json_file)
            stderr.print(f"[red]json data file {self.source_json_file} does not exist")
            sys.exit(1)
        with open(self.source_json_file, "r") as fh:
            self.json_data = json.loads(fh.read())

    def convert_input_json_to_ena(self):
        """Split the input ena json, in samples and runs json"""
        pass

    def create_dataframe(self, dataframe_name, fields_config, df_schemas):
        df_list = []
        config_json = ConfigJson()
        for index in range(len(dataframe_name)):

            fields = config_json.get_configuration("ENA_fields")[fields_config[index]]
            dataframe_name[index] = df_schemas[fields]
            df_list.append(dataframe_name[index])

        return df_list

    def rename_cols_df(self, dataframe_name, dataframe, col_name_og, col_name_final):
        col_dict = {}

        for index in range(len(col_name_og)):
            col_dict[col_name_og[index]] = col_name_final[index]

        dataframe_name = dataframe.rename(columns=col_dict)

        return dataframe_name

    def create_structure_to_ena(self):
        """Convert json to dataframe required by ena-upload-cli package"""

        squema = self.source_json_file
        fh_squema = open(squema)
        squema_json = json.load(fh_squema)
        fh_squema.close()
        config_json = ConfigJson()

        df_schemas = pd.DataFrame(squema_json)

        dataframe_name_list = ["df_study", "df_samples", "df_run", "df_experiments"]
        dataframe_fileds_config_list = [
            "df_study_fields",
            "df_samples_fields",
            "df_run_fields",
            "df_experiment_fields",
        ]
        # Creating the dataframes df_Study, df_samples, df_run and df_experiment
        df_list = self.create_dataframe(
            dataframe_name_list, dataframe_fileds_config_list, df_schemas
        )

        df_study = df_list[0]
        df_samples = df_list[1]
        df_run = df_list[2]
        df_experiments = df_list[3]

        # df_schema
        """
        collecting_institution collection_date     collector_name  ...                                        study_title    study_type taxon_id
0      Hospital Clínic de Barcelona      2021-05-10  Inmaculada Casas   ...  RELECOV Spanish Network for genomics surveillance  Surveillance  2697049
1      Hospital Clínic de Barcelona      2021-05-07   Inmaculada Casas  ...  RELECOV Spanish Network for genomics surveillance  Surveillance  2697049
        """
        # Parsing the dataframes specifically according to ENA requirements #
        # df_study
        df_study["alias"] = df_study["study_alias"]
        df_study["title"] = df_study["study_title"]
        df_study.insert(3, "status", self.action)

        """
        # df_study
        alias                                              title    study_type status                                     study_abstract
0   RELECOV  RELECOV Spanish Network for genomics surveillance  Surveillance    ADD  RELECOV Spanish Network for genomics surveillance
1   RELECOV  RELECOV Spanish Network for genomics surveillance  Surveillance    ADD  RELECOV Spanish Network for genomics surveillance
        """

        # df_samples
        df_samples = self.rename_cols_df(
            "df_samples",
            df_samples,
            config_json.get_configuration("ENA_fields")["rename_sample_list_og"],
            config_json.get_configuration("ENA_fields")["rename_sample_list_final"],
        )
        df_samples.insert(3, "status", self.action)
        checklist = config_json.get_configuration("ENA_fields")["checklist"]
        df_samples.insert(4, "ENA_CHECKLIST", checklist)

        """
        # df_samples
        alias      title taxon_id host health state  ...                                  scientific_name     collector name           collecting institution    isolate
0   212164375  212164375  2697049                    ...  Severe acute respiratory syndrome coronavirus 2  Inmaculada Casas      Hospital Clínic de Barcelona  212164375
1   212163777  212163777  2697049                    ...  Severe acute respiratory syndrome coronavirus 2   Inmaculada Casas     Hospital Clínic de Barcelona  212163777
2   212153091  212153091  2697049                    ...  Severe acute respiratory syndrome coronavirus 2   Inmaculada Casas     Hospital Clínic de Barcelona  212153091
        """
        # df_run
        df_run.insert(1, "sequence_file_R1_fastq", "None")
        df_run.insert(2, "sequence_file_R2_fastq", "None")
        for i in range(len(df_schemas)):
            df_run.loc[i, "sequence_file_R1_fastq"] = df_schemas.loc[
                i, "sequence_file_R1_fastq"
            ]

            df_run.loc[i, "sequence_file_R2_fastq"] = df_schemas.loc[
                i, "sequence_file_R2_fastq"
            ]
        df_run.insert(3, "status", self.action)

        df_run = df_run.rename(columns={"sequence_file_R1_md5": "file_checksum"})

        for i in range(len(df_run)):
            df_run.loc[i, "alias"] = (
                str(df_run.loc[i, "sequence_file_R1_fastq"])
                + "_"
                + str(df_run.loc[i, "sequence_file_R2_fastq"])
            )
            df_run.loc[i, "experiment_alias"] = (
                str(df_run.loc[i, "sequence_file_R1_fastq"])
                + "_"
                + str(df_run.loc[i, "sequence_file_R2_fastq"])
            )

        df_run.insert(5, "file_name", df_run["sequence_file_R1_fastq"])
        df_run2 = df_run.copy()
        df_run2["file_name"] = df_run["sequence_file_R2_fastq"]
        df_run2["file_checksum"] = df_run["sequence_file_R2_md5"]
        df_run_final = pd.concat([df_run, df_run2])
        df_run_final.reset_index()

        """
        # df_run
        experiment_alias      sequence_file_R1_fastq      sequence_file_R2_fastq  ... file_type                     file_checksum                      fastq_r2_md5
0        214821_S12  214821_S12_R1_001.fastq.gz  214821_S12_R2_001.fastq.gz  ...     fastq  372ca8b10a8eeb7a04107634baf340ab  7f5081eec1b64b171402b66f37fe640d
1        214821_S12  214822_S13_R1_001.fastq.gz  214822_S13_R2_001.fastq.gz  ...     fastq  b268d7be80e80455bec0807b5961d23c  a15be39feaf73eec9b2c026717878bba
2        214821_S12   214823_S1_R1_001.fastq.gz   214823_S1_R2_001.fastq.gz  ...     fastq  c16bdbfc03c354496fcfb2c107e3cbf6  4d9b80b977a75bf7e2a4282ca910d94a
        """
        # df_experiments

        df_experiments["instrument_model"] = df_experiments[
            "instrument_model"
        ].str.lower()
        df_experiments["sample_alias"] = df_samples["alias"]
        df_experiments["title"] = df_study["study_abstract"]
        df_experiments["study_alias"] = df_study["alias"]
        df_experiments.insert(3, "status", self.action)

        for i in range(len(df_experiments)):
            df_experiments.loc[i, "alias"] = (
                str(df_run.loc[i, "sequence_file_R1_fastq"])
                + "_"
                + str(df_run.loc[i, "sequence_file_R2_fastq"])
            )

        """
        # df_experiments example
        experiment_alias                                              title study_alias  ... library_layout instrument_model                                              alias
0        214821_S12  RELECOV Spanish Network for genomics surveillance     RELECOV  ...         PAIRED   Illumina MiSeq  214821_S12_R1_001.fastq.gz_214821_S12_R2_001.f...
1        214821_S12  RELECOV Spanish Network for genomics surveillance     RELECOV  ...         PAIRED   Illumina MiSeq  214822_S13_R1_001.fastq.gz_214822_S13_R2_001.f...
2        214821_S12  RELECOV Spanish Network for genomics surveillance     RELECOV  ...         PAIRED   Illumina MiSeq  214823_S1_R1_001.fastq.gz_214823_S1_R2_001.fas...

        """

        ena_config = config_json.get_configuration("ENA_fields")["ENA_configuration"]
        schema_dataframe = {}
        schema_dataframe["sample"] = df_samples
        schema_dataframe["run"] = df_run_final
        schema_dataframe["experiment"] = df_experiments
        schema_targets = extract_targets(self.action, schema_dataframe)

        if ena_config["study_id"] is not None:
            schema_dataframe["study"] = df_study

        if (
            self.action == "ADD"
            or self.action == "add"
            or self.action == "MODIFY"
            or self.action == "modify"
        ):
            file_paths = {}
            file_paths_r2 = {}
            for path in df_schemas["r1_fastq_filepath"]:
                file_paths[os.path.basename(path)] = os.path.abspath(path)

            for path in df_schemas["r2_fastq_filepath"]:
                file_paths_r2[os.path.basename(path)] = os.path.abspath(path)

            file_paths.update(file_paths_r2)

            # Submit data to webin ftp server. It should only upload fastq files in case the action is ADD.
            # When the action is MODIFY rthe fastq are already submitted

            if self.action == "ADD" or self.action == "add":
                session = ftplib.FTP("webin2.ebi.ac.uk", self.user, self.passwd)

                for filename, path in file_paths.items():

                    print("Uploading path " + path + " and filename: " + filename)

                    try:
                        file = open(path, "rb")  # file to send
                        g = session.storbinary(f"STOR {filename}", file)
                        print(g)  # send the file
                        file.close()  # close file and FTP
                    except BaseException as err:

                        print(f"ERROR: {err}")
                        # print("ERROR: If your connection times out at this stage, it propably is because of a firewall that is in place. FTP is used in passive mode and connection will be opened to one of the ports: 40000 and 50000.")

                g2 = session.quit()
                print(g2)

            """
            THE ENA_UPLOAD_CLI METHOD DOES NOT WORK (below)
            chec = submit_data(file_paths, self.passwd, self.user)
            print(chec)
            When ADD/MODIFY:
            requires source XMLs for 'run', 'experiment', 'sample', 'experiment'
            schema_xmls record XMLs for all these schema and following 'submission'
            """
            #
            tool = config_json.get_configuration("ENA_fields")["tool"]

            schema_xmls = run_construct(
                template_path, schema_targets, self.center, checklist, tool
            )

            submission_xml = construct_submission(
                template_path, self.action, schema_xmls, self.center, checklist, tool
            )
            schema_xmls["submission"] = submission_xml

            tree = ET.parse(schema_xmls["run"])
            root = tree.getroot()

            for files in root.iter("FILE"):
                if "R2" in files.attrib["filename"]:

                    H = df_run_final.loc[
                        df_run_final["sequence_file_R2_fastq"]
                        == files.attrib["filename"]
                    ].values[0][8]
                    files.set("checksum", H)

            tree.write(schema_xmls["run"])

            if self.dev:
                url = "https://wwwdev.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"
            else:
                url = "https://www.ebi.ac.uk/ena/submit/drop-box/submit/?auth=ENA"

            print(f"\nSubmitting XMLs to ENA server: {url}")

            receipt = send_schemas(schema_xmls, url, self.user, self.passwd).text
            if not os.path.exists(self.output_path):
                os.mkdir(self.output_path)
            receipt_dir = os.path.join(self.output_path, "receipt.xml")
            print(f"Printing receipt to {receipt_dir}")

            with open(f"{receipt_dir}", "w") as fw:
                fw.write(receipt)
            try:
                schema_update = process_receipt(receipt.encode("utf-8"), self.action)
            except ValueError:
                log.error("There was an ERROR during submission:")
                sys.exit(receipt)

            if self.action in ["ADD", "MODIFY"] or self.action in ["add", "modify"]:
                schema_dataframe = update_table(
                    schema_dataframe, schema_targets, schema_update
                )

    def upload(self):
        """Create the required files and upload to ENA"""
        self.convert_input_json_to_ena()
        self.create_structure_to_ena()
