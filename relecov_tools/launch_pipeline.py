import json
import re
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
        template=None,
        output_folder=None,
        pipeline_conf_file=None,
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
            stderr.print("[red] Input folder " + self.input_folder + " does not exist")
            sys.exit(1)
        if template is None:
            self.template = relecov_tools.utils.prompt_path(
                msg="Select the path which contains the template structure"
            )
        else:
            self.template = template
        if not os.path.exists(self.template):
            log.error("Template folder %s does not exist ", self.template)
            stderr.print("[red] Template folder " + self.template + " does not exist")
            sys.exit(1)
        if pipeline_conf_file is None:
            self.pipeline_conf_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "conf",
                "pipeline_config.json",
            )
        if not os.path.exists(self.pipeline_conf_file):
            log.error(
                "Pipeline config file %s does not exist ", self.pipeline_conf_file
            )
            stderr.print(
                "[red] Pipeline config file "
                + self.pipeline_conf_file
                + " does not exist"
            )
            sys.exit(1)
        data = relecov_tools.utils.read_json_file(self.pipeline_conf_file)
        if (
            not "analysis_name" in data
            or not "sample_stored_folder" in data
            or not "sample_link_folder" in data
        ):
            log.error("Invalid pipeline config file %s ", self.pipeline_conf_file)
            stderr.print(
                "[red] Invalid pipeline config file " + self.pipeline_conf_file
            )
            sys.exit(1)

        if output_folder is None:
            output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        self.output_folder = os.path.join(
            output_folder, current_date + "_" + self.analysis_name
        )
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
        self.analysis_name = os.path.join(self.output_folder, data["analysis_name"])
        self.analysis_folder = os.path.join(self.analysis_name, data["analysis_folder"])
        self.copied_sample_folder = os.path.join(
            self.analysis_name, data["sample_stored_folder"]
        )
        self.linked_sample_folder = os.path.join(
            self.analysis_folder, data["sample_link_folder"]
        )

        self.sample_ids = []

    def join_valid_items(self):
        def get_latest_lab_folder(self):
            lab_folders = [f.path for f in os.scandir(self.input_folder) if f.is_dir()]
            lab_latest_folders = {}
            latest_date = datetime.datetime.strptime("20220101", "%Y%m%d").date()
            for lab_folder in lab_folders:
                existing_upload_folders = False
                last_folder_date = datetime.datetime.strptime(
                    "20220101", "%Y%m%d"
                ).date()
                scan_folder = os.path.join(self.input_folder, lab_folder)
                lab_sub_folders = [
                    f.path for f in os.scandir(scan_folder) if f.is_dir()
                ]
                for lab_sub_folder in lab_sub_folders:
                    f_name = os.path.basename(lab_sub_folder)
                    f_date_match = re.match(r"(^\d{8}).*", f_name)
                    if not f_date_match:
                        continue
                    f_date = f_date_match.group(1)
                    try:
                        sub_f_date = datetime.datetime.strptime(f_date, "%Y%m%d").date()
                    except ValueError:
                        continue
                    if sub_f_date > last_folder_date:
                        last_folder_date = sub_f_date
                        latest_folder_name = os.path.join(lab_sub_folder, f_name)
                        existing_upload_folders = True
                if existing_upload_folders:
                    lab_latest_folders[lab_folder] = {
                        "path": latest_folder_name,
                        "date": last_folder_date,
                    }
                    if last_folder_date > latest_date:
                        latest_date = last_folder_date
            return lab_latest_folders, latest_date

        upload_lab_folders, latest_date = get_latest_lab_folder(self)
        samples_data = []
        for lab, data_folder in upload_lab_folders.items():
            # check if laboratory folder is the latest date to process
            if data_folder["date"] != latest_date:
                continue
            # fetch the validate file and get sample id and r1 and r2 file path
            validate_files = [
                f
                for f in os.listdir(data_folder["path"])
                if f.startswith("validated_") and f.endswith(".json")
            ]
            if not validate_files:
                continue
            for validate_file in validate_files:
                validate_file_path = os.path.join(data_folder["path"], validate_file)
                with open(validate_file_path) as fh:
                    data = json.load(fh)
                for item in data:
                    sample = {}
                    sample["sample_ids"] = item["sequencing_sample_id"]
                    sample["r1_fastq_file_path"] = item["r1_fastq_file_path"]
                    if "r2_fastq_file_path" in item:
                        sample["r2_fastq_file_path"] = item["r2_fastq_file_path"]
                    samples_data.append(sample)
            log.info("Collecting samples for  %s", lab)
            stderr.print("[blue] Collecting samples for " + lab)
        return samples_data

    def pipeline_exc(self):
        # copy template folder and subfolders in output folder
        shutil.copytree(self.template, self.output_folder)
        # collect json with all validated samples
        samples_data = self.join_valid_items()

        # iterate over the sample_data to copy the fastq files in the output folder
        for item in samples_data:
            sequencing_r1_sample_id = item["sequencing_sample_id"] + "_R1" + ".fastq.gz"
            sequencing_r2_sample_id = item["sequencing_sample_id"] + "_R2" + ".fastq.gz"
            # join r1_fastq_file_path and sequencing_file_R1_fastq
            r1_file_path = os.path.join(
                item["r1_fastq_file_path"], item["sequencing_file_R1_fastq"]
            )
            # copy sequencing files into the output folder
            raw_folder = os.path.join(self.output_folder, self.copied_sample_folder)
            shutil.copy(r1_file_path, raw_folder)
            # create simlink for the sample
            sample_r1_link_path = os.path.join(
                self.linked_sample_folder, sequencing_r1_sample_id
            )
            os.symlink(r1_file_path, sample_r1_link_path)
            if r2_file_path:
                sample_r2_link_path = os.path.join(
                    self.linked_sample_folder, sequencing_r2_sample_id
                )
                os.symlink(r2_file_path, sample_r2_link_path)
            if "sequencing_file_R2_fastq" in item:
                r2_file_path = os.path.join(
                    item["r2_fastq_file_path"], item["sequencing_file_R2_fastq"]
                )
            else:
                r2_file_path = None
            # copy sequencing files into the output folder
