import datetime
import json
import logging
import os
import re
import shutil
import sys
from collections import Counter

import rich.console
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class PipelineManager:
    def __init__(
        self,
        input_folder=None,
        template=None,
        output_folder=None,
        pipeline_conf_file=None,
    ):
        self.current_date = datetime.date.today().strftime("%Y%m%d")
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
            config_data = conf_settings["pipeline_manager"]
        except KeyError:
            log.error("Invalid pipeline config file %s ", pipeline_conf_file)
            stderr.print("[red] Invalid pipeline config file " + pipeline_conf_file)
        if (
            "analysis_user" not in config_data
            or "analysis_group" not in config_data
            or "analysis_folder" not in config_data
            or "sample_stored_folder" not in config_data
            or "sample_link_folder" not in config_data
            or "doc_folder" not in config_data
        ):
            log.error("Invalid pipeline config file %s ", pipeline_conf_file)
            stderr.print("[red] Invalid pipeline config file " + pipeline_conf_file)
            sys.exit(1)
        self.config_fata = config_data
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

        self.output_folder = output_folder
        self.out_folder_namevar = f"{self.current_date}_{config_data['analysis_group']}_%s_{config_data['analysis_user']}"
        self.analysis_folder = config_data["analysis_folder"]
        self.copied_sample_folder = config_data["sample_stored_folder"]
        self.linked_sample_folder = config_data["sample_link_folder"]
        self.doc_folder = config_data["doc_folder"]

    def join_valid_items(self):
        """Join validated metadata for the latest batches downloaded into a single one

        Args:

        Returns:
            join_validate (list(dict)): List of dictionaries containing all the samples
            found in each validated_lab_metadata.json form the scanned folders
            latest_date (str): Latest batch date found in the scanned folder
        """

        def get_latest_lab_folder(self):
            """Get latest folder with the newest date

            Args:

            Returns:
                lab_latest_folders: list of paths with the latest folders
                latest_date: latest date in the folders

            """
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
        join_validate = list()
        for lab, data_folder in upload_lab_folders.items():
            lab_code = lab.split("/")[-1]
            log.info("Collecting samples for  %s", lab_code)
            stderr.print("[blue] Collecting samples for ", lab_code)
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
                join_validate.extend(data)
        log.info("Found a total of %s samples", str(len(join_validate)))
        stderr.print(f"Found a total of {len(join_validate)} samples")
        return join_validate, latest_date

    def copy_process(self, samples_data, output_folder):
        """Copies all the necessary samples files in the given samples_data list
        to the output folder. Also creates symbolic links into the link folder
        given in config_file.

        Args:
            samples_data (list(dict)): samples_data from self.create_samples_data()
            output_folder (str): Destination folder to copy files

        Returns:
            samp_errors (dict): Dictionary where keys are sequencing_sample_id and values
            the files that received an error while trying to copy.
        """
        samp_errors = {}
        links_folder = os.path.join(
            output_folder, self.analysis_folder, self.linked_sample_folder
        )
        os.makedirs(links_folder, exist_ok=True)
        for sample in samples_data:
            sample_id = sample["sequencing_sample_id"]
            # fetch the file extension
            ext_found = re.match(r".*(fastq.*|bam)", sample["r1_fastq_filepath"])
            if not ext_found:
                log.error("No valid file extension found for %s", sample_id)
                samp_errors[sample_id].append(sample["r1_fastq_filepath"])
                continue
            ext = ext_found.group(1)
            seq_r1_sample_id = sample["sequencing_sample_id"] + "_R1." + ext
            # copy r1 sequencing file into the output folder self.analysis_folder
            sample_raw_r1 = os.path.join(
                output_folder, self.copied_sample_folder, seq_r1_sample_id
            )
            log.info("Copying sample %s", sample)
            stderr.print("[blue] Copying sample: ", sample["sequencing_sample_id"])
            try:
                shutil.copy(sample["r1_fastq_filepath"], sample_raw_r1)
                # create simlink for the r1
                r1_link_path = os.path.join(links_folder, seq_r1_sample_id)
                r1_link_path_ori = os.path.join("../../RAW", seq_r1_sample_id)
                os.symlink(r1_link_path_ori, r1_link_path)
            except FileNotFoundError as e:
                log.error("File not found %s", e)
                samp_errors[sample_id] = []
                samp_errors[sample_id].append(sample["r1_fastq_filepath"])
                if "r2_fastq_filepath" in sample:
                    samp_errors[sample_id].append(sample["r2_fastq_filepath"])
                continue
            # check if there is a r2 file
            if "r2_fastq_filepath" in sample:
                seq_r2_sample_id = sample["sequencing_sample_id"] + "_R2." + ext
                sample_raw_r2 = os.path.join(
                    output_folder,
                    self.copied_sample_folder,
                    seq_r2_sample_id,
                )
                try:
                    shutil.copy(sample["r2_fastq_filepath"], sample_raw_r2)
                    r2_link_path = os.path.join(links_folder, seq_r2_sample_id)
                    r2_link_path_ori = os.path.join("../../RAW", seq_r2_sample_id)
                    os.symlink(r2_link_path_ori, r2_link_path)
                except FileNotFoundError as e:
                    log.error("File not found %s", e)
                    if not samp_errors.get(sample_id):
                        samp_errors[sample_id] = []
                    samp_errors[sample_id].append(sample["r2_fastq_filepath"])
                    continue
        return samp_errors

    def create_samples_data(self, json_data):
        """Creates a copy of the json_data but only with relevant keys to copy files.
        Here 'r1_fastq_filepath' is created joining the original 'r1_fastq_filepath'
        and 'sequence_file_R1_fastq' fields. The same goes for 'r2_fastq_filepath'

        Args:
            json_data (list(dict)): Samples metadata in a list of dictionaries

        Returns:
            sample_data: list(dict)
                [
                  {
                    "sequencing_sample_id":XXXX,
                    "r1_fastq_filepath": XXXX,
                    "r2_fastq_filepath":XXXX
                  }
                ]
        """
        samples_data = []
        for item in json_data:
            sample = {}
            sample["sequencing_sample_id"] = item["sequencing_sample_id"]
            sample["r1_fastq_filepath"] = os.path.join(
                item["r1_fastq_filepath"], item["sequence_file_R1_fastq"]
            )
            if "r2_fastq_filepath" in item:
                sample["r2_fastq_filepath"] = os.path.join(
                    item["r2_fastq_filepath"], item["sequence_file_R2_fastq"]
                )
            samples_data.append(sample)
        return samples_data

    def split_data_by_key(self, json_data, keylist):
        """Split a given json data into different lists based on a given list of keys.
        From a single list of samples (dicts), the output will now be a list of lists
        where each new list is a subset of the original samples with the same values
        for the given list of keys

        Args:
            json_data (list(dict)): List of dictionaries, one for each sample
            keylist (list(str)): List of keys within the given dictionaries to
            split data.

        Returns:

        """
        if not keylist:
            return [json_data]

        json_split_by_key = {}
        new_key = keylist[0]
        next_keys = keylist[1:]

        json_uniq_vals = frozenset([x.get(new_key) for x in json_data])
        for val in json_uniq_vals:
            grouped_samples = [x for x in json_data if x.get(new_key) == val]
            json_split_by_key[val] = grouped_samples

        list_of_jsons_by_key = []
        for group in json_split_by_key.values():
            list_of_jsons_by_key.extend(self.split_data_by_key(group, next_keys))
        return list_of_jsons_by_key

    def pipeline_exc(self):
        """Prepare folder for analysis in HPC
        Copies template selected as input
        Copies RAW data with sequencing id as fastq file names
        Creates samples_id.txt

        Args:

        Returns:

        """
        # collect json with all validated samples
        join_validate, latest_date = self.join_valid_items()
        latest_date = str(latest_date).replace("-", "")
        if len(join_validate) == 0:
            stderr.print("[yellow]No samples were found. Aborting")
            sys.exit(0)
        keys_to_split = ["enrichment_panel", "enrichment_panel_version"]
        stderr.print(f"[blue]Splitting samples based on {keys_to_split}...")
        json_split_by_panel = self.split_data_by_key(join_validate, keys_to_split)
        stderr.print(f"[blue]Data splitted into {len(json_split_by_panel)} groups")
        # iterate over the sample_data to copy the fastq files in the output folder
        global_samp_errors = {}
        for idx, list_of_samples in enumerate(json_split_by_panel, start=1):
            group_tag = f"{latest_date}_PANEL{idx:02d}"
            log.info("Processing group %s", group_tag)
            stderr.print(f"[blue]Processing group {group_tag}...")
            group_outfolder = os.path.join(
                self.output_folder, self.out_folder_namevar % group_tag
            )
            if os.path.exists(group_outfolder):
                msg = f"Analysis folder {group_outfolder} already exists and it will be deleted. Do you want to continue? Y/N"
                confirmation = relecov_tools.utils.prompt_yn_question(msg)
                if confirmation is False:
                    continue
                shutil.rmtree(group_outfolder)
                log.info(f"Folder {group_outfolder} removed")
            samples_data = self.create_samples_data(list_of_samples)
            # Create a folder for the group of samples and copy the files there
            log.info("Creating folder for group %s", group_tag)
            stderr.print(f"[blue]Creating folder for group {group_tag}")
            # copy template folder and subfolders in output folder
            shutil.copytree(self.template, group_outfolder)
            # Check for possible duplicates
            log.info("Samples to copy %s", len(samples_data))
            # Extract the sequencing_sample_id from the list of dictionaries
            sample_ids = [item["sequencing_sample_id"] for item in samples_data]
            # Use Counter to count the occurrences of each sequencing_sample_id
            id_counts = Counter(sample_ids)
            # Find the sequencing_sample_id values that are duplicated (count > 1)
            duplicates = [
                sample_id for sample_id, count in id_counts.items() if count > 1
            ]
            if duplicates:
                log.error(
                    "There are duplicated samples in group %s: %s"
                    % ({group_tag}, {duplicates})
                )
                stderr.print(
                    f"[red] There are duplicated samples in group {group_tag}: {duplicates}. Please handle manually"
                )
                continue

            samp_errors = self.copy_process(samples_data, group_outfolder)
            if len(samp_errors) > 0:
                stderr.print(
                    f"[red]Unable to copy files from {len(samp_errors)} samples in group {group_tag}"
                )
                msg = f"Do you want to delete analysis folder {group_outfolder}? Y/N"
                confirmation = relecov_tools.utils.prompt_yn_question(msg)
                if confirmation:
                    shutil.rmtree(group_outfolder)
                    log.info(f"Folder {group_outfolder} removed")
                    continue
            global_samp_errors[group_tag] = samp_errors
            samples_copied = len(list_of_samples) - len(samp_errors)
            stderr.print(
                f"[green]Group {group_tag}: {samples_copied} samples copied out of {len(list_of_samples)}"
            )
            final_valid_samples = [
                x
                for x in list_of_samples
                if x.get("sequencing_sample_id") not in samp_errors
            ]
            sample_ids = [i for i in sample_ids if i not in samp_errors]
            group_analysis_folder = os.path.join(group_outfolder, self.analysis_folder)
            # print samples_id file
            stderr.print(
                f"[blue]Generating sample_id.txt file in {group_analysis_folder}"
            )
            with open(os.path.join(group_analysis_folder, "samples_id.txt"), "w") as f:
                for sample_id in sample_ids:
                    f.write(f"{sample_id}\n")
            json_filename = os.path.join(
                group_outfolder,
                self.doc_folder,
                f"{group_tag}_validate_batch.json",
            )
            relecov_tools.utils.write_json_fo_file(final_valid_samples, json_filename)
            log.info("[blue]Successfully created pipeline folder. Ready to launch")
            stderr.print(
                f"[blue]Successfully created folder for {group_tag}. Ready to launch"
            )
        for group, samples in global_samp_errors.items():
            if not samples:
                continue
            log.error("Group %s received error for samples: %s" % (group, samples))
        if not any(v for v in global_samp_errors.values()):
            stderr.print("[green]All samples were copied successfully!!")
        log.info("Finished execution")
        stderr.print("Finished execution")
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
