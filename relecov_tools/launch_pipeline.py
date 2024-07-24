import json
import os
import sys
import shutil
import datetime
import logging
import rich.console

import relecov_tools.utils


log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

class LaunchPipeline:
    def __init__(
        self,
        input_folder=None,
        validated_file=None,
        template=None,
        output_folder=None,
        pipeline_conf_file = None
    ):
        current_date = datetime.date.today().strftime("%Y%m%d")
        if input_folder is None:
            self.input_folder = relecov_tools.utils.prompt_path(
                msg="Select the folder which contains the fastq file of samples"
            )
        else:
            self.input_folder = input_folder
        if not os.path.exists(self.input_folder):
            log.error("Input folder %s does not exist ", self.input_folder)
            stderr.print(
                "[red] Input folder " + self.input_folder + " does not exist"
            )
            sys.exit(1)
        if template is None:
            self.template = relecov_tools.utils.prompt_path(
                msg="Select the path which contains the template structure"
            )
        else:
            self.template = template
        if not os.path.exists(self.template):
            log.error("Template folder %s does not exist ", self.template)
            stderr.print(
                "[red] Template folder " + self.template + " does not exist"
            )
            sys.exit(1)
        if validated_file is None:
            self.validated_file = relecov_tools.utils.prompt_path(
                msg="Select the json file which contains the validate samples"
            )
        else:
            self.validated_file = validated_file
        if not os.path.exists(self.validated_file):
            log.error("Json validate file %s does not exist ", self.validated_file)
            stderr.print(
                "[red] Json validate file " + self.validated_file + " does not exist"
            )
            sys.exit(1)
        if pipeline_conf_file is None:
            self.pipeline_conf_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "conf", "pipeline_config.json"
            )
        if not os.path.exists(self.pipeline_conf_file):
            log.error("Pipeline config file %s does not exist ", self.pipeline_conf_file)
            stderr.print(
                "[red] Pipeline config file " + self.pipeline_conf_file + " does not exist"
            )
            sys.exit(1)
        data = relecov_tools.utils.read_json_file(self.pipeline_conf_file)
        if not "analysis_name" in data:
            log.error("Invalid pipeline config file %s ", self.pipeline_conf_file)
            stderr.print(
                "[red] Invalid pipeline config file " + self.pipeline_conf_file
            )
            sys.exit(1)
        else:
            analysis_name = data["analysis_name"]
        if output_folder is None:
            output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        self.output_folder = os.path.join(output_folder, current_date, analysis_name)
        if os.path.exists(self.output_folder):
            msg = "Analysis folder already exists and it will be deleted. Do you want to continue? Y/N"
            confirmation = relecov_tools.utils.prompt_yn_question(msg)
            if confirmation.lower() != "y":
                sys.exit(1)
        try:
            os.makedirs(self.output_folder)
        except OSError as e:
            log.error("Unable to create output folder %s ", e)
            stderr.print("[red] Unable to create output folder " + e)
            sys.exit(1)
        if not os.path.exists(self.output_folder):
            log.error("Output folder %s does not exist ", self.output_folder)
            stderr.print(
                "[red] Output folder " + self.output_folder + " does not exist"
            )
            sys.exit(1)

        self.sample_ids = []

    def join_valid_items(self):
        subfolders = [f.path for f in os.scandir(self.input_folder) if f.is_dir()]
        latest_subfolders = []
        for subfolder in subfolders:
            subfolder_date = os.path.basename(subfolder)
            try:
                subfolder_date = datetime.datetime.strptime(subfolder_date, "%Y%m%d").date()
                latest_subfolders.append(subfolder)
            except ValueError:
                pass
        # Now you have the latest subfolder for each date in latest_subfolders
        # You can continue with your code logic here

    def pipeline_exc(self):
        # copy template folder and subfolders in output folder
        shutil.copytree(self.template, self.output_folder)
        with open(self.validated_file) as fh:
            data = json.load(fh)
        # iterate over the list of the items
        for item in data:
            sequencing_sample_id = item["sequencing_sample_id"]
            # join r1_fastq_file_path and sequencing_file_R1_fastq
            r1_file_path = os.path.join(item["r1_fastq_file_path"], item["sequencing_file_R1_fastq"])
            if  "sequencing_file_R2_fastq" in item:
                r2_file_path = os.path.join(item["r2_fastq_file_path"], item["sequencing_file_R2_fastq"])
            else:
                r2_file_path = None
            
        
    