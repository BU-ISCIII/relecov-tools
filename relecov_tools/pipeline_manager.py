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
            pipeline_conf_file = os.path.join(
                os.path.dirname(os.path.realpath(__file__)),
                "conf",
                "configuration.json",
            )
        if not os.path.exists(pipeline_conf_file):
            log.error("Pipeline config file %s does not exist ", pipeline_conf_file)
            stderr.print(
                "[red] Pipeline config file " + pipeline_conf_file + " does not exist"
            )
            sys.exit(1)
        conf_settings = relecov_tools.utils.read_json_file(pipeline_conf_file)
        try:
            data = conf_settings["launch_pipeline"]
            # get_topic_data("launch_pipeline", "analysis_name")
        except KeyError:
            log.error("Invalid pipeline config file %s ", pipeline_conf_file)
            stderr.print("[red] Invalid pipeline config file " + pipeline_conf_file)
        if (
            "analysis_name" not in data
            or "sample_stored_folder" not in data
            or "sample_link_folder" not in data
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
        # Create the output folder if not exists
        try:
            os.makedirs(output_folder, exist_ok=True)
        except OSError or FileExistsError as e:
            log.error("Unable to create output folder %s ", e)
            stderr.print("[red] Unable to create output folder ", e)
            sys.exit(1)
        # Update the output folder with the current date and analysis name

        self.output_folder = os.path.join(
            output_folder, current_date + "_" + data["analysis_name"]
        )
        if os.path.exists(self.output_folder):
            msg = "Analysis folder already exists and it will be deleted. Do you want to continue? Y/N"
            confirmation = relecov_tools.utils.prompt_yn_question(msg)
            if confirmation is False:
                sys.exit(1)
            shutil.rmtree(self.output_folder)

        self.analysis_folder = os.path.join(self.output_folder, data["analysis_folder"])
        self.copied_sample_folder = os.path.join(
            self.output_folder, data["sample_stored_folder"]
        )
        self.linked_sample_folder = os.path.join(
            self.analysis_folder, data["sample_link_folder"]
        )

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
                        latest_folder_name = lab_sub_folder
                        existing_upload_folders = True
                if existing_upload_folders:
                    lab_latest_folders[lab_folder] = {
                        "path": latest_folder_name,
                        "date": last_folder_date,
                    }
                    if last_folder_date > latest_date:
                        latest_date = last_folder_date
            log.info("Latest date to process is %s", latest_date)
            stderr.print("[blue] Collecting samples from ", latest_date)
            return lab_latest_folders, latest_date

        upload_lab_folders, latest_date = get_latest_lab_folder(self)
        samples_data = []
        for lab, data_folder in upload_lab_folders.items():
            # check if laboratory folder is the latest date to process
            if data_folder["date"] != latest_date:
                continue
            # fetch the validate file and get sample id and r1 and r2 file path
            validate_files = [
                os.path.join(data_folder["path"], f)
                for f in os.listdir(data_folder["path"])
                if f.startswith("validated_lab_metadata") and f.endswith(".json")
            ]
            if not validate_files:
                continue
            for validate_file in validate_files:
                validate_file_path = os.path.join(data_folder["path"], validate_file)
                with open(validate_file_path) as fh:
                    data = json.load(fh)
                for item in data:
                    sample = {}
                    sample["sequencing_sample_id"] = item["sequencing_sample_id"]
                    sample["r1_fastq_file_path"] = os.path.join(
                        item["r1_fastq_filepath"], item["sequence_file_R1_fastq"]
                    )
                    if "r2_fastq_file_path" in item:
                        sample["r2_fastq_file_path"] = os.path.join(
                            item["r1_fastq_filepath"], item["sequence_file_R2_fastq"]
                        )
                    samples_data.append(sample)
            lab_code = lab.split("/")[-1]
            log.info("Collecting samples for  %s", lab_code)
            stderr.print("[blue] Collecting samples for ", lab_code)
        return samples_data

    def pipeline_exc(self):
        # copy template folder and subfolders in output folder
        shutil.copytree(self.template, self.output_folder)
        # create the 00_reads folder
        os.makedirs(self.linked_sample_folder, exist_ok=True)
        # collect json with all validated samples
        samples_data = self.join_valid_items()

        # iterate over the sample_data to copy the fastq files in the output folder
        file_errors = []
        copied_samples = 0
        if len(samples_data) == 0:
            stderr.print("[yellow] No samples were found. Deleting analysis folder")
            shutil.rmtree(self.analysis_folder)
            sys.exit(0)
        for item in samples_data:
            # fetch the file extension
            ext_found = re.match(r".*(fastq.*|bam)", item["r1_fastq_file_path"])
            ext = ext_found.group(1)
            sequencing_r1_sample_id = item["sequencing_sample_id"] + "_R1." + ext
            # copy r1 sequencing file into the output folder
            raw_folder = os.path.join(self.analysis_folder, self.copied_sample_folder)

            try:
                shutil.copy(item["r1_fastq_file_path"], raw_folder)
                # create simlink for the r1
                sample_r1_link_path = os.path.join(
                    self.linked_sample_folder, sequencing_r1_sample_id
                )
                os.symlink(item["r1_fastq_file_path"], sample_r1_link_path)
            except FileNotFoundError as e:
                log.error("File not found %s", e)
                file_errors.append(item["r1_fastq_file_path"])
                continue
            copied_samples += 1
            # check if there is a r2 file
            if "r2_fastq_file_path" in item:
                sequencing_r2_sample_id = item["sequencing_sample_id"] + "_R2." + ext
                try:
                    shutil.copy(item["r2_fastq_file_path"], raw_folder)
                    sample_r2_link_path = os.path.join(
                        self.linked_sample_folder, sequencing_r2_sample_id
                    )
                    os.symlink(item["r2_fastq_file_path"], sample_r2_link_path)
                except FileNotFoundError as e:
                    log.error("File not found %s", e)
                    file_errors.append(item["r2_fastq_file_path"])
                    continue
        if len(file_errors) > 0:
            stderr.print(
                "[red] Files do not found. Unable to copy",
                "[red] " + str(len(file_errors)),
                "[red]sample files",
            )
            msg = "Do you want to delete analysis folder? Y/N"
            confirmation = relecov_tools.utils.prompt_yn_question(msg)
            if confirmation:
                shutil.rmtree(self.output_folder)
                sys.exit(1)
        stderr.print("[green] Samples copied: ", copied_samples)
        stderr.print("[blue] Pipeline launched successfully")
        return


class ResultUpload:
    def __init__(self, input_folder=None, conf_file=None):
        if input_folder is None:
            self.input_folder = relecov_tools.utils.prompt_path(
                msg="Select the folder which contains the results"
            )
        else:
            self.input_folder = input_folder
        if not os.path.exists(self.input_folder):
            log.error("Input folder %s does not exist ", self.input_folder)
            stderr.print("[red] Input folder " + self.input_folder + " does not exist")
            sys.exit(1)

        conf_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "conf",
            "configuration.json",
        )
        if not os.path.exists(conf_file):
            log.error("Configuration file %s does not exist ", self.conf_file)
            stderr.print(
                "[red] Pipeline config file "
                + self.pipeline_conf_file
                + " does not exist"
            )
            sys.exit(1)
        conf_settings = relecov_tools.utils.read_json_file(conf_file)
        try:
            data = conf_settings["pipelines"]["relecov"]
        except KeyError:
            log.error("Invalid pipeline config file %s ", self.pipeline_conf_file)
            stderr.print(
                "[red] Invalid pipeline config file " + self.pipeline_conf_file
            )
        stderr.print(f"[blue] Configuration file loaded  {data}")
