import datetime
import json
import os
import re
import shutil
import sys
from collections import Counter, defaultdict

import rich.console
import relecov_tools.utils
from relecov_tools.base_module import BaseModule
from relecov_tools.config_json import ConfigJson

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class PipelineManager(BaseModule):
    def __init__(
        self,
        input: str | None = None,
        templates_root: str | None = None,
        output_dir: str | None = None,
        skip_db_upload: bool = False,
        folder_names=None,
    ):
        super().__init__(output_dir=output_dir, called_module=__name__)

        # Check CLI arguments

        self.log.info("Initiating pipeline-manager process")
        self.current_date = datetime.date.today().strftime("%Y%m%d")
        self.skip_db_upload = skip_db_upload

        if input is None:
            self.input_folder = relecov_tools.utils.prompt_path(
                msg="Select the folder which contains the fastq file of samples"
            )
        else:
            self.input_folder = input

        if not os.path.exists(self.input_folder):
            self.log.error("Input folder %s does not exist ", self.input_folder)
            stderr.print("[red] Input folder " + self.input_folder + " does not exist")
            raise FileNotFoundError(f"Input folder {self.input_folder} does not exist")

        if templates_root is None:
            self.templates_root = relecov_tools.utils.prompt_path(
                msg="Select the folder path which contains the templates"
            )
        else:
            self.templates_root = templates_root

        if not os.path.exists(self.templates_root):
            self.log.error("Template folder %s does not exist ", self.templates_root)
            stderr.print(
                "[red] Template folder " + self.templates_root + " does not exist"
            )
            raise FileNotFoundError(
                f"Template folder {self.templates_root} does not exist"
            )

        if output_dir is None:
            self.output_dir = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_dir = output_dir

        # Create the output folder if not exists
        try:
            os.makedirs(self.output_dir, exist_ok=True)
        except OSError or FileExistsError as e:
            self.log.error("Unable to create output folder %s ", e)
            stderr.print("[red] Unable to create output folder ", e)
            raise OSError(f"Unable to create output folder {self.output_dir}: {e}")

        self.folder_list = folder_names

        # Check and load config params
        self.config = ConfigJson(extra_config=True)
        config_data = self.config.get_configuration("pipeline_manager")

        if not config_data:
            self.log.error("Invalid pipeline config file")
            stderr.print("[red] Invalid pipeline config file")
            raise ValueError("Invalid pipeline config file")

        required_conf = [
            "analysis_user",
            "analysis_group",
            "analysis_folder",
            "sample_stored_folder",
            "sample_link_folder",
            "doc_folder",
            "organism_config",
        ]

        missing_conf = [k for k in required_conf if k not in config_data]

        if missing_conf:
            self.log.error("Invalid pipeline config file. Missing %s", missing_conf)
            stderr.print(f"[red]Invalid pipeline config file. Missing {missing_conf}")
            raise ValueError(f"Invalid pipeline config file. Missing {missing_conf}")

        # Update the output folder with the current date and analysis name
        self.out_folder_namevar = f"{self.current_date}_{config_data['analysis_group']}_%s_{config_data['analysis_user']}"
        self.analysis_folder = config_data["analysis_folder"]
        self.copied_sample_folder = config_data["sample_stored_folder"]
        self.linked_sample_folder = config_data["sample_link_folder"]
        self.doc_folder = config_data["doc_folder"]
        self.organism_config = config_data["organism_config"]

        req_conf = ["update_db"] * bool(self.skip_db_upload)
        missing = [
            conf for conf in req_conf if self.config.get_configuration(conf) is None
        ]
        if missing:
            self.log.error(
                "Extra config file () is missing required sections: %s"
                % ", ".join(missing)
            )
            self.log.error(
                "Please use add-extra-config to add them to the config file."
            )
            stderr.print(
                f"[red]Config file is missing required sections: {', '.join(missing)}"
            )
            stderr.print(
                "[red]Please use add-extra-config to add them to the config file."
            )
            raise ValueError(
                f"Config file is missing required sections: {', '.join(missing)}"
            )

    def get_latest_lab_folders(
        self, initial_date: datetime.date
    ) -> tuple[list[str], datetime.date]:
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
            latest_folder_name = None
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
            if existing_upload_folders and latest_folder_name is not None:
                lab_latest_folders[lab_folder] = {
                    "path": latest_folder_name,
                    "date": last_folder_date,
                }
                if last_folder_date > latest_date:
                    latest_date = last_folder_date
            else:
                self.log.warning(
                    "No valid subfolders found in %s. Skipping this lab folder",
                    lab_folder,
                )
                stderr.print(
                    f"[yellow] No valid subfolders found in {lab_folder}. Skipping this lab folder"
                )

        # keep only folders with the latest date to process
        lab_latest_folders = [
            d["path"] for d in lab_latest_folders.values() if d["date"] == latest_date
        ]
        self.log.info("Latest date to process is %s", latest_date)
        stderr.print("[blue] Collecting samples from date ", latest_date)

        return lab_latest_folders, latest_date

    def join_valid_items(
        self,
        input_folder: str | None = None,
        folder_list: list | None = [],
        initial_date: datetime.date = datetime.date(2022, 1, 1),
    ) -> tuple[list[dict], datetime.date]:
        """Join validated metadata for the latest batches downloaded into a single one

        Args:
            input_folder (str): Folder to start the searching process.
            initial_date (datetime): Only search for folders newer than this date.
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
                full_path = os.path.join(input_folder or "", lab_folder)
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
                last_folder_date_obj = relecov_tools.utils.string_to_date(last_folder)
                if last_folder_date_obj is not None:
                    latest_date = last_folder_date_obj.date()
                else:
                    raise ValueError("Could not parse date from folder name")
            except ValueError:
                self.log.error(
                    "Failed to get date from folder names. Using last mod date"
                )
                file_dates = [
                    relecov_tools.utils.get_file_date(f) for f in folders_to_process
                ]
                # Filter out None values
                file_dates = [d for d in file_dates if d is not None]
                if not file_dates:
                    raise ValueError("No valid dates found in folders_to_process")
                # Ensure file_dates are datetime objects before calling .date()
                if isinstance(file_dates[0], str):
                    file_dates = [
                        datetime.datetime.strptime(d, "%Y%m%d") for d in file_dates
                    ]
                latest_date = max(file_dates).date()  # type: ignore
        else:
            folders_to_process, latest_date = self.get_latest_lab_folders(initial_date)
        join_validate = list()
        for folder in folders_to_process:
            lab_code = folder.split("/")[-2]
            self.log.info("Collecting samples for %s", str(lab_code))
            stderr.print(f"[blue] Collecting samples for {lab_code}")
            # fetch the validate file and get sample id and r1 and r2 file path
            validate_files = [
                os.path.join(folder, f)
                for f in os.listdir(folder)
                if f.startswith("validated_read_lab_metadata") and f.endswith(".json")
            ]
            if not validate_files:
                self.log.error(f"No validated json file found for {folder}. Skipped")
                continue
            elif len(validate_files) > 1:
                self.log.error("Found multiple validated files in %s. Skipped" % folder)
                continue
            with open(validate_files[0]) as fh:
                data = json.load(fh)
            join_validate.extend(data)
        self.log.info("Found a total of %s samples", str(len(join_validate)))
        stderr.print(f"[blue]Found a total of {len(join_validate)} samples")
        return join_validate, latest_date

    def copy_process(self, samples_data: list[dict], output_dir: str) -> dict:
        """Copies all the necessary samples files in the given samples_data list
        to the output folder. Also creates symbolic links into the link folder
        given in config_file.

        Args:
            samples_data (list(dict)): samples_data from self.create_samples_data()
            output_dir (str): Destination folder to copy files

        Returns:
            samp_errors (dict): Dictionary where keys are sequencing_sample_id and values
            the files that received an error while trying to copy.
        """
        samp_errors = {}
        links_folder = os.path.join(
            output_dir, self.analysis_folder, self.linked_sample_folder
        )
        os.makedirs(links_folder, exist_ok=True)
        for sample in samples_data:
            sample_id = sample["sequencing_sample_id"]
            # fetch the file extension
            ext_found = re.match(r".*(fastq.*|bam)", sample["sequence_file_path_R1"])
            if not ext_found:
                self.log.error("No valid file extension found for %s", sample_id)
                samp_errors[sample_id].append(sample["sequence_file_path_R1"])
                continue
            ext = ext_found.group(1)
            seq_r1_sample_id = sample["sequencing_sample_id"] + "_R1." + ext
            # copy r1 sequencing file into the output folder self.analysis_folder
            sample_raw_r1 = os.path.join(
                output_dir, self.copied_sample_folder, seq_r1_sample_id
            )
            self.log.info("Copying sample %s", sample)
            stderr.print("[blue] Copying sample: ", sample["sequencing_sample_id"])
            try:
                shutil.copy(sample["sequence_file_path_R1"], sample_raw_r1)
                # create simlink for the r1
                r1_link_path = os.path.join(links_folder, seq_r1_sample_id)
                r1_link_path_ori = os.path.join("../../RAW", seq_r1_sample_id)
                os.symlink(r1_link_path_ori, r1_link_path)
            except FileNotFoundError as e:
                self.log.error("File not found %s", e)
                samp_errors[sample_id] = []
                samp_errors[sample_id].append(sample["sequence_file_path_R1"])
                if "sequence_file_path_R2" in sample:
                    samp_errors[sample_id].append(sample["sequence_file_path_R2"])
                continue
            # check if there is a r2 file
            if "sequence_file_path_R2" in sample:
                seq_r2_sample_id = sample["sequencing_sample_id"] + "_R2." + ext
                sample_raw_r2 = os.path.join(
                    output_dir,
                    self.copied_sample_folder,
                    seq_r2_sample_id,
                )
                try:
                    shutil.copy(sample["sequence_file_path_R2"], sample_raw_r2)
                    r2_link_path = os.path.join(links_folder, seq_r2_sample_id)
                    r2_link_path_ori = os.path.join("../../RAW", seq_r2_sample_id)
                    os.symlink(r2_link_path_ori, r2_link_path)
                except FileNotFoundError as e:
                    self.log.error("File not found %s", e)
                    if not samp_errors.get(sample_id):
                        samp_errors[sample_id] = []
                    samp_errors[sample_id].append(sample["sequence_file_path_R2"])
                    continue
        return samp_errors

    def create_samples_data(self, json_data: list[dict]) -> list[dict]:
        """Creates a copy of the json_data but only with relevant keys to copy files.
        Here 'sequence_file_path_R1' is created joining the original 'sequence_file_path_R1'
        and 'sequence_file_R1' fields. The same goes for 'sequence_file_path_R2'

        Args:
            json_data (list(dict)): Samples metadata in a list of dictionaries

        Returns:
            sample_data: list(dict)
                [
                  {
                    "sequencing_sample_id":XXXX,
                    "sequence_file_path_R1": XXXX,
                    "sequence_file_path_R2":XXXX
                  }
                ]
        """
        samples_data = []
        for item in json_data:
            sample = {}
            sample["sequencing_sample_id"] = item["sequencing_sample_id"]
            sample["sequence_file_path_R1"] = os.path.join(
                item["sequence_file_path_R1"], item["sequence_file_R1"]
            )
            if "sequence_file_path_R2" in item:
                sample["sequence_file_path_R2"] = os.path.join(
                    item["sequence_file_path_R2"], item["sequence_file_R2"]
                )
            samples_data.append(sample)
        return samples_data

    def split_data_by_key(
        self, json_data: list[dict], keylist: list[str]
    ) -> list[list[dict]]:
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

    def split_samples_by_organism(self, join_validate: list[dict]) -> dict:
        """Split the input JSON into groups for each organism, which will later
        be analysed differently depending on its configuration.

        Args:
            join_validate (list(dict)): Merged JSON from all the ones found

        Returns:
            jsons_by_organism_dict(dict): Dictionary with {organism1:splitted_json1}
            with the result from split_data_by_key() for each group of samples from
            a certain organism.
        """
        jsons_by_organism_dict = {}
        for organism, org_conf in self.organism_config.items():
            org_samples = [x for x in join_validate if x.get("organism") == organism]
            if not org_samples:
                self.log.info(f"No samples found for organism: {organism}")
                continue
            if "group_by_fields" in org_conf.keys():
                logtxt = "Data for %s will be grouped by the following fields: %s"
                self.log.info(logtxt % (organism, str(org_conf["group_by_fields"])))
                keys_to_split = org_conf["group_by_fields"]
            else:
                self.log.warning(
                    f"No group_by_fields found for {organism}. Data won't be grouped"
                )
                keys_to_split = []
            splitted_json = self.split_data_by_key(org_samples, keys_to_split)
            jsons_by_organism_dict[organism] = splitted_json
        return jsons_by_organism_dict

    def process_samples(self, splitted_json: list[dict], org_conf: dict, latest_date: datetime.date) -> dict:
        """Create the output folder, the required template for the pipeline and
        all the files necessary for each group of samples depending on the organism
        config given. Copy the samples and log the ones that could not be copied.

        Args:
            splitted_json (list(dict)): List of jsons, one for each group
            org_conf (dict): organism params from configuration.json
            latest_date (datetime): Latest date found. Used to tag the analysis folder

        Returns:
            global_samp_errors(dict): Dictionary holding the samples with errors for
            each group
        """
        global_samp_errors = {}
        keys_to_split = org_conf.get("group_by_fields", [])
        # iterate over the sample_data to copy the fastq files in the output folder
        for idx, list_of_samples in enumerate(splitted_json, start=1):
            group_tag = f"{latest_date}_{org_conf['service_tag']}{idx:02d}"
            self.log.info("Processing group %s", group_tag)
            fields = {k: list_of_samples[0].get(k, "") for k in keys_to_split}
            if keys_to_split:
                logtxt = f"[blue]Processing group {group_tag} with fields: {fields}..."
                stderr.print(logtxt)
            else:
                logtxt = f"[blue]Processing group {group_tag}"
                stderr.print(logtxt)
            group_outfolder = os.path.join(
                self.output_dir, self.out_folder_namevar % group_tag
            )
            if os.path.exists(group_outfolder):
                msg = f"Analysis folder {group_outfolder} already exists and it will be deleted. Do you want to continue? Y/N"
                confirmation = relecov_tools.utils.prompt_yn_question(msg)
                if confirmation is False:
                    continue
                shutil.rmtree(group_outfolder)
                self.log.info(f"Folder {group_outfolder} removed")
            samples_data = self.create_samples_data(list(list_of_samples))
            # Create a folder for the group of samples and copy the files there
            self.log.info("Creating folder for group %s", group_tag)
            stderr.print(f"[blue]Creating folder for group {group_tag}")

            pipeline_templates = org_conf.get("pipeline_templates")
            if pipeline_templates is not None:
                if not isinstance(pipeline_templates, list):
                    raise ValueError(
                        f"'pipeline_templates' must be a list, but got {type(pipeline_templates).__name__}"
                    )
                for template_name in pipeline_templates:
                    template_path = os.path.join(self.templates_root, template_name)
                    if not os.path.exists(template_path):
                        self.log.warning(
                            f"Template {template_path} does not exist. Skipping."
                        )
                        continue
                    for item in os.listdir(template_path):
                        s = os.path.join(template_path, item)
                        d = os.path.join(group_outfolder, item)
                        if os.path.isdir(s):
                            shutil.copytree(s, d, dirs_exist_ok=True)
                        else:
                            shutil.copy2(s, d)
            else:
                if "pipeline_template" not in org_conf:
                    raise ValueError(
                        "Organism config must include either 'pipeline_templates' (list) or 'pipeline_template' (string)."
                    )
                template_name = org_conf["pipeline_template"]
                if not isinstance(template_name, str):
                    raise ValueError(
                        f"'pipeline_template' must be a string, but got {type(template_name).__name__}"
                    )
                template_path = os.path.join(self.templates_root, template_name)
                if not os.path.exists(template_path):
                    raise FileNotFoundError(f"Template {template_path} does not exist.")
                shutil.copytree(template_path, group_outfolder)

            # Check for possible duplicates
            self.log.info("Samples to copy %s", len(samples_data))
            # Extract the sequencing_sample_id from the list of dictionaries
            sample_ids = [item["sequencing_sample_id"] for item in samples_data]
            # Use Counter to count the occurrences of each sequencing_sample_id
            id_counts = Counter(sample_ids)
            # Find the sequencing_sample_id values that are duplicated (count > 1)
            duplicates = [
                sample_id for sample_id, count in id_counts.items() if count > 1
            ]
            if duplicates:
                self.log.error(
                    "Duplicate samples in group %s: %s" % (group_tag, duplicates)
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
                    self.log.info(f"Folder {group_outfolder} removed")
                    continue
            global_samp_errors[group_tag] = samp_errors
            samples_copied = len(list_of_samples) - len(samp_errors)
            copied_samps_log = f"Group {group_tag}: {samples_copied} samples copied out of {len(list_of_samples)}"
            self.log.info(copied_samps_log)
            stderr.print(copied_samps_log)

            dest_folder = os.path.join(group_outfolder, self.copied_sample_folder)

            samples_by_path = defaultdict(list)
            for s in samples_data:
                institution_path = os.path.dirname(
                    s.get("sequence_file_path_R1", "Missing [LOINC:LA14698-7]")
                )
                samples_by_path[institution_path].append(s["sequencing_sample_id"])

            errors_by_path = defaultdict(set)
            for sample_id in samp_errors:
                matching_samples = [
                    s for s in samples_data if s["sequencing_sample_id"] == sample_id
                ]
                if matching_samples:
                    institution_path = os.path.dirname(
                        matching_samples[0].get(
                            "sequence_file_path_R1", "Missing [LOINC:LA14698-7]"
                        )
                    )
                else:
                    institution_path = "Missing [LOINC:LA14698-7]"
                errors_by_path[institution_path].add(sample_id)

            for institution_path, samples in samples_by_path.items():
                total = len(samples)
                copied = total - len(errors_by_path.get(institution_path, []))
                msg = f"{copied}/{total} samples from {institution_path} copied to {dest_folder}"
                self.log.info(msg)
                stderr.print(msg)

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
            relecov_tools.utils.write_json_to_file(fields, group_info)
            self.log.info(f"Group fields info saved in {group_info}")

            output_filename = self.tag_filename(f"{group_tag}_validate_batch.json")
            json_filename = os.path.join(group_doc_folder, output_filename)
            relecov_tools.utils.write_json_to_file(final_valid_samples, json_filename)
            self.log.info("Successfully created pipeline folder. Ready to launch")
            stderr.print(f"[blue]Folder {group_outfolder} finished. Ready to launch")
        return global_samp_errors

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
        batch_id = self.get_batch_id_from_data(join_validate)
        # If more than one batch is included, current date will be set as batch_id
        self.set_batch_id(batch_id)
        jsons_by_organism_dict = self.split_samples_by_organism(join_validate)
        if not jsons_by_organism_dict:
            self.log.error("No samples found for any of the organisms in config")
            raise ValueError("No samples found for any of the organisms in config")
        global_samp_errors = {}
        for organism, splitted_json in jsons_by_organism_dict.items():
            stderr.print(f"[blue]Processing samples for organism: {organism}")
            samp_errors = self.process_samples(
                splitted_json, self.organism_config[organism], latest_date
            )
            global_samp_errors[organism] = samp_errors

        for organism, org_samp_errors in global_samp_errors.items():
            if all(not v for v in org_samp_errors.values()):
                logtxt = f"All samples were copied successfully for {organism}!!"
                self.log.info(logtxt)
                stderr.print(f"[green]{logtxt}")
                continue
            else:
                self.log.error(f"Found errors during sample copying for {organism}: ")
            for group, samples in org_samp_errors.items():
                if not samples:
                    continue
                self.log.error(
                    "Group %s received error for samples: %s" % (group, samples)
                )
        self.log.info("Finished execution")
        stderr.print("Finished execution")
        return
