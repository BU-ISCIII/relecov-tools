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
        folder_list=None,
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
        required_conf = [
            "analysis_user",
            "analysis_group",
            "analysis_folder",
            "sample_stored_folder",
            "sample_link_folder",
            "doc_folder",
        ]
        missing_conf = [k for k in required_conf if k not in config_data]
        if missing_conf:
            log.error("Invalid pipeline config file. Missing %s", missing_conf)
            stderr.print(f"[red]Invalid pipeline config file. Missing {missing_conf}")
            sys.exit(1)
        if "group_by_fields" in config_data:
            logtxt = "Data will be grouped by the following fields: %s"
            log.info(logtxt % str(config_data["group_by_fields"]))
            self.keys_to_split = config_data["group_by_fields"]
        else:
            log.warning("No group_by_fields found in config, Data won't be grouped")
            self.keys_to_split = []
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
        self.folder_list = folder_list
        # Update the output folder with the current date and analysis name
        self.output_folder = output_folder
        self.out_folder_namevar = f"{self.current_date}_{config_data['analysis_group']}_%s_{config_data['analysis_user']}"
        self.analysis_folder = config_data["analysis_folder"]
        self.copied_sample_folder = config_data["sample_stored_folder"]
        self.linked_sample_folder = config_data["sample_link_folder"]
        self.doc_folder = config_data["doc_folder"]

    def get_latest_lab_folders(self, initial_date):
        """Get latest folder with the newest date
        Args:
            initial_date(datetime.date()): Starting date to search for
        Returns:
            lab_latest_folders: list of paths with the latest folders
            latest_date: latest date in the folders
        """
        lab_folders = [f.path for f in os.scandir(self.input_folder) if f.is_dir()]
        lab_latest_folders = {}
        latest_date = initial_date
        for lab_folder in lab_folders:
            existing_upload_folders = False
            last_folder_date = initial_date
            scan_folder = os.path.join(self.input_folder, lab_folder)
            lab_sub_folders = [f.path for f in os.scandir(scan_folder) if f.is_dir()]
            for lab_sub_folder in lab_sub_folders:
                f_name = os.path.basename(lab_sub_folder)
                sub_f_date = relecov_tools.utils.string_to_date(f_name)
                if not sub_f_date:
                    continue
                if sub_f_date.date() > last_folder_date:
                    last_folder_date = sub_f_date.date()
                    latest_folder_name = lab_sub_folder
                    existing_upload_folders = True
            if existing_upload_folders:
                lab_latest_folders[lab_folder] = {
                    "path": latest_folder_name,
                    "date": last_folder_date,
                }
                if last_folder_date > latest_date:
                    latest_date = last_folder_date
        # keep only folders with the latest date to process
        lab_latest_folders = [
            d["path"] for d in lab_latest_folders.values() if d["date"] == latest_date
        ]
        log.info("Latest date to process is %s", latest_date)
        stderr.print("[blue] Collecting samples from date ", latest_date)
        return lab_latest_folders, latest_date

    def join_valid_items(self, input_folder, folder_list=[], initial_date="20220101"):
        """Join validated metadata for the latest batches downloaded into a single one

        Args:
            input_folder (str): Folder to start the searching process.
            initial_date (str): Only search for folders newer than this date.
            folder_list (list(str)): Only retrieve folders with these basenames. Defaults to list()

        Returns:
            join_validate (list(dict)): List of dictionaries containing all the samples
            found in each validated_lab_metadata.json form the scanned folders
            latest_date (str): Latest batch date found in the scanned folder
        """
        if folder_list:
            folders_to_process = []
            # TODO: Change this for os.walk but with multithreading because its too slow
            lab_folders = [f.path for f in os.scandir(self.input_folder) if f.is_dir()]
            for lab_folder in lab_folders:
                full_path = os.path.join(input_folder, lab_folder)
                lab_subfolders = [
                    f.path for f in os.scandir(full_path) if f.path if f.is_dir()
                ]
                folders_to_process.extend(
                    [f for f in lab_subfolders if os.path.basename(f) in folder_list]
                )
            if not folders_to_process:
                raise FileNotFoundError("No folders found with the given names")
            last_folder = sorted(folder_list)[-1]
            try:
                latest_date = relecov_tools.utils.string_to_date(last_folder).date()
            except ValueError:
                log.error("Failed to get date from folder names. Using last mod date")
                latest_date = max(
                    [relecov_tools.utils.get_file_date(f) for f in folders_to_process]
                ).date()
        else:
            folders_to_process, latest_date = self.get_latest_lab_folders(initial_date)
        join_validate = list()
        for folder in folders_to_process:
            lab_code = folder.split("/")[-2]
            log.info("Collecting samples for %s", str(lab_code))
            stderr.print(f"[blue] Collecting samples for {lab_code}")
            # fetch the validate file and get sample id and r1 and r2 file path
            validate_files = [
                os.path.join(folder, f)
                for f in os.listdir(folder)
                if f.startswith("validated_lab_metadata") and f.endswith(".json")
            ]
            if not validate_files:
                log.error(f"No validated json file found for {folder}. Skipped")
                continue
            elif len(validate_files) > 1:
                log.error("Found multiple validated files in %s. Skipped" % folder)
                continue
            with open(validate_files[0]) as fh:
                data = json.load(fh)
            join_validate.extend(data)
        log.info("Found a total of %s samples", str(len(join_validate)))
        stderr.print(f"[blue]Found a total of {len(join_validate)} samples")
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
            list_of_jsons_by_key (list(list(dict))): List of JSONs. One for each group
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
        init_date = datetime.datetime.strptime("20220101", "%Y%m%d").date()
        join_validate, latest_date = self.join_valid_items(
            input_folder=self.input_folder,
            initial_date=init_date,
            folder_list=self.folder_list,
        )
        latest_date = str(latest_date).replace("-", "")
        if len(join_validate) == 0:
            stderr.print("[yellow]No samples were found. Aborting")
            sys.exit(0)
        if self.keys_to_split:
            stderr.print(f"[blue]Splitting samples based on {self.keys_to_split}...")
            splitted_json = self.split_data_by_key(join_validate, self.keys_to_split)
            stderr.print(f"[blue]Data splitted into {len(splitted_json)} groups")
        else:
            splitted_json = [join_validate]
        # iterate over the sample_data to copy the fastq files in the output folder
        global_samp_errors = {}
        for idx, list_of_samples in enumerate(splitted_json, start=1):
            group_tag = f"{latest_date}_GROUP{idx:02d}"
            log.info("Processing group %s", group_tag)
            fields = {
                k: v for k, v in list_of_samples[0].items() if k in self.keys_to_split
            }
            stderr.print(f"[blue]Processing group {group_tag} with fields: {fields}...")
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
                log.error("Duplicate samples in group %s: %s" % (group_tag, duplicates))
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
            copied_samps_log = f"Group {group_tag}: {samples_copied} samples copied out of {len(list_of_samples)}"
            log.info(copied_samps_log)
            stderr.print(copied_samps_log)
            final_valid_samples = [
                x
                for x in list_of_samples
                if x.get("sequencing_sample_id") not in samp_errors
            ]
            sample_ids = [i for i in sample_ids if i not in samp_errors]
            group_analysis_folder = os.path.join(group_outfolder, self.analysis_folder)
            group_doc_folder = os.path.join(group_outfolder, self.doc_folder)
            # print samples_id file
            stderr.print(
                f"[blue]Generating sample_id.txt file in {group_analysis_folder}..."
            )
            with open(os.path.join(group_analysis_folder, "samples_id.txt"), "w") as f:
                for sample_id in sample_ids:
                    f.write(f"{sample_id}\n")
            group_info = os.path.join(group_doc_folder, "group_fields.json")
            relecov_tools.utils.write_json_fo_file(fields, group_info)
            log.info(f"Group fields info saved in {group_info}")

            json_filename = os.path.join(
                group_doc_folder, f"{group_tag}_validate_batch.json"
            )
            relecov_tools.utils.write_json_fo_file(final_valid_samples, json_filename)
            log.info("Successfully created pipeline folder. Ready to launch")
            stderr.print(f"[blue]Folder {group_outfolder} finished. Ready to launch")
        error_ocurred = False
        for group, samples in global_samp_errors.items():
            if not samples:
                continue
            log.error("Group %s received error for samples: %s" % (group, samples))
            error_ocurred = True
        if not error_ocurred:
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
