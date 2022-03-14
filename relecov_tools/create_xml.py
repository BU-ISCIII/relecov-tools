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


class xml_creation:
    def __init__(self, source_path=None, output_path=None, action=None):
        if source_path is None:
            self.source_path = utils.prompt_source_path()
        else:
            self.source_path = source_path
        if output_path is None:
            self.output_path = utils.prompt_destination_path()
        else:
            self.output_path = output_path
        if action is None:
            self.action = "ADD"
        else:
            self.action = action

        def xml(
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
            with open(self.source_path) as json_format_file:
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
            if self.action.upper == "ADD":
                # submission add
                r = e.Element("SUBMISSION_SET")
                sample = e.SubElement(r, "SUBMISSION")
                actions = e.SubElement(sample, "ACTIONS")
                action = e.SubElement(actions, "ACTION")
                e.SubElement(action, "ADD")
                a = e.ElementTree(r)
                a.write(os.path.join(self.output_path, "study", "submission.xml"))
            if self.action.upper() == "MODIFY":
                # submission modify
                r = e.Element("SUBMISSION_SET")
                sample = e.SubElement(r, "SUBMISSION")
                actions = e.SubElement(sample, "ACTIONS")
                action = e.SubElement(actions, "ACTION")
                e.SubElement(action, "MODIFY")
                a = e.ElementTree(r)
                a.write(os.path.join(self.output_path, "study", "submission.xml"))

            # project_relecov.xml
            r = e.Element("PROJECT_SET")
            project = e.SubElement(r, "PROJECT")
            project.set("alias", "RELECOV")
            e.SubElement(
                project, "TITLE"
            ).text = "Example project for ENA submission RELECOV"
            e.SubElement(
                project, "DESCRIPTION"
            ).text = (
                "This study was created as part of an ENA submissions example RELECOV"
            )
            submission = e.SubElement(project, "SUBMISSION_PROJECT")
            e.SubElement(submission, "SEQUENCING_PROJECT")
            a = e.ElementTree(r)
            a.write(os.path.join(self.output_path, "study", "project_relecov.xml"))

            # 1.1 Upload study info

            #  2. From validated json to xml samples - submission.xml and samples.xml

            # submission.xml
            if self.action.upper == "ADD":
                # submission add
                r = e.Element("SUBMISSION_SET")
                sample = e.SubElement(r, "SUBMISSION")
                actions = e.SubElement(sample, "ACTIONS")
                action = e.SubElement(actions, "ACTION")
                e.SubElement(action, "ADD")
                a = e.ElementTree(r)
                a.write(os.path.join(self.output_path, "samples", "submission.xml"))
            if self.action.upper() == "MODIFY":
                # submission modify
                r = e.Element("SUBMISSION_SET")
                sample = e.SubElement(r, "SUBMISSION")
                actions = e.SubElement(sample, "ACTIONS")
                action = e.SubElement(actions, "ACTION")
                e.SubElement(action, "MODIFY")
                a = e.ElementTree(r)
                a.write(os.path.join(self.output_path, "samples", "submission.xml"))

            # samples_relecov.xml
            data_keys = list(json_data.keys())
            r = e.Element("SAMPLE_SET")
            sample = e.SubElement(r, "SAMPLE")
            sample.set("alias", "SARS Sample 1 programmatic")
            e.SubElement(sample, "TITLE").text = "SARS Sample 1"
            sample_name = e.SubElement(sample, "SAMPLE_NAME")
            e.SubElement(sample_name, "TAXON_ID").text = "2697049"
            e.SubElement(
                sample_name, "SCIENTIFIC_NAME"
            ).text = "Severe acute respiratory syndrome coronavirus 2"
            e.SubElement(sample, "DESCRIPTION").text = "SARS-CoV-2 Sample #1"
            sample_attributes = e.SubElement(sample, "SAMPLE_ATTRIBUTES")
            for i in json_data:
                sample_attribute = e.SubElement(sample_attributes, "SAMPLE_ATTRIBUTE")
                e.SubElement(sample_attribute, "TAG").text = str(i)
                e.SubElement(sample_attribute, "VALUE").text = json_data[i]
            a = e.ElementTree(r)
            a.write(os.path.join(self.output_path, "samples", "samples_relecov.xml"))

            # 2.2 Upload samples info

            # 3. From sftp upload runs (FASTQ files programmatic)- experiments.xmlm, runs.xml and submission.xml
            # 4. From sftp upload  sequences (FASTA files programmatic) - json using webin-cli-rest
