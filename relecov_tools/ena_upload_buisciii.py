import os
import logging
import rich.console
from email import utils
import json as j
import xml.etree.cElementTree as e

import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class XmlCreation:
    def __init__(self, source_json=None, output_path=None, action=None):
        if source_json is None:
            self.source_json = utils.prompt_source_path()
        else:
            self.source_json = source_json
        if output_path is None:
            self.output_path = utils.prompt_destination_path()
        else:
            self.output_path = output_path
        if action is None:
            self.action = "ADD"
        else:
            self.action = action

        def xml_study(
            self,
        ):
            """
            1.From validated json to xml study- submission.xml and project.xml
            1.1 Upload study info

            2. From validated json to xml samples - submission.xml and samples.xml
            2.2 Upload samples info

            3. From sftp upload runs (FASTQ files programmatic)- experiments.xmlm, runs.xml and submission.xml
            4. From sftp upload  sequences (FASTA files programmatic) - json using webin-cli-rest
            """

            # Load validated json
            with open(self.source_json) as json_format_file:
                json_data = j.load(json_format_file)

            # Create output directory

            try:
                # Create target Directory
                os.mkdir(self.output_path)
                print("Directory ", self.output_path, " Created ")
            except FileExistsError:
                print("Directory ", self.output_path, " already exists")

            # 1. From validated json to xml study- submission.xml and project.xml

            # submission.xml
            os.chdir("xml_files/")
            if self.action.upper == "ADD":
                # submission add
                submission_file = "submission_add.xml"
            if self.action.upper() == "MODIFY":
                # submission modify
                submission_file = "submission_modify.xml"

            # project_relecov.xml
            os.chdir("../conf")
            dict_conf = j.loads("configuration.json")
            r = e.Element("PROJECT_SET")
            project = e.SubElement(r, "PROJECT")
            project.set("alias", dict_conf["project_relecov_xml"]["alias"])
            e.SubElement(project, "TITLE").text = dict_conf["project_relecov_xml"][
                "TITLE"
            ]
            e.SubElement(project, "DESCRIPTION").text = dict_conf[
                "project_relecov_xml"
            ]["DESCRIPTION"]
            submission = e.SubElement(project, "SUBMISSION_PROJECT")
            e.SubElement(submission, "SEQUENCING_PROJECT")
            a = e.ElementTree(r)
            a.write(os.path.join(self.output_path, "study", "project_relecov.xml"))

            # 1.1 Upload study info
            """
            import requests
            from requests.structures import CaseInsensitiveDict

            url = "https://reqbin.com/echo/post/json"

            headers = CaseInsensitiveDict()
            headers["Content-Type"] = "application/json"
            headers["Authorization"] = "Basic bG9naW46cGFzc3dvcmQ="

            data = '{"login":"my_login","password":"my_password"}'


            resp = requests.post(url, headers=headers, data=data)

            print(resp.status_code)
            """

            #  2. From validated json to xml samples - submission.xml and samples.xml

        def xml_samples():
            # submission.xml
            os.chdir("../xml_files/")
            if self.action.upper == "ADD":
                # submission add
                submission_file = "submission_add.xml"
            if self.action.upper() == "MODIFY":
                # submission modify
                submission_file = "submission_modify.xml"

            # samples_relecov.xml
            os.chdir("../schema/")
            json_data = j.loads("to_ena.json")
            os.chdir("../conf")
            dict_conf = j.loads("configuration.json")

            data_keys = list(json_data.keys())
            r = e.Element("SAMPLE_SET")
            sample = e.SubElement(r, "SAMPLE")
            sample.set(
                "alias",
                "Programmatic Test SARS-CoV-2 Sample" + str(json_data["sample_name"]),
            )
            e.SubElement(sample, "TITLE").text = "SARS-CoV-2 Sample" + str(
                json_data["sample_name"]
            )
            sample_name = e.SubElement(sample, "SAMPLE_NAME")
            e.SubElement(sample_name, "TAXON_ID").text = dict_conf["fixed_data"][
                "tax_id"
            ]
            e.SubElement(sample_name, "SCIENTIFIC_NAME").text = dict_conf["fixed_data"][
                "scientific_name"
            ]
            e.SubElement(sample, "DESCRIPTION").text = "SARS-CoV-2 Sample" + str(
                json_data["sample_name"]
            )
            sample_attributes = e.SubElement(sample, "SAMPLE_ATTRIBUTES")
            for i in json_data:
                sample_attribute = e.SubElement(sample_attributes, "SAMPLE_ATTRIBUTE")
                e.SubElement(sample_attribute, "TAG").text = str(i)
                e.SubElement(sample_attribute, "VALUE").text = json_data[i]
            a = e.ElementTree(r)
            a.write(os.path.join(self.output_path, "samples", "samples_relecov.xml"))

            # 2.2 Upload samples info
            """
            import requests
            from requests.structures import CaseInsensitiveDict

            url = "https://reqbin.com/echo/post/json"

            headers = CaseInsensitiveDict()
            headers["Content-Type"] = "application/json"
            headers["Authorization"] = "Basic bG9naW46cGFzc3dvcmQ="

            data = '{"login":"my_login","password":"my_password"}'


            resp = requests.post(url, headers=headers, data=data)

            print(resp.status_code)
            """

            # 3. From sftp upload runs (FASTQ files programmatic)- experiments.xmlm, runs.xml and submission.xml
            # 4. From sftp upload  sequences (FASTA files programmatic) - json using webin-cli-rest
