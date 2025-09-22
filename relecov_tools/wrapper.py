#!/usr/bin/env python
import re
import os
import inspect
import rich.console
from collections import defaultdict

from relecov_tools.config_json import ConfigJson
from relecov_tools.download import Download
from relecov_tools.read_lab_metadata import LabMetadata
from relecov_tools.validate import Validate
from relecov_tools.base_module import BaseModule
import relecov_tools.utils

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class Wrapper(BaseModule):
    """
    Always fill all the arguments for the class in the config file, leave its value
    if you dont want to use that argument e.g.(target_folders:  ) -> (target_folders = None)
    """

    def __init__(self, output_dir: str | None = None):
        config = ConfigJson(extra_config=True)
        wrapper_cfg = config.get_configuration("wrapper") or {}
        download_cfg = config.get_configuration("download") or {}

        final_output_dir = (
            output_dir
            or wrapper_cfg.get("output_dir")
            or download_cfg.get("output_dir")
        )

        if not final_output_dir:
            raise ValueError(
                "Output directory not supplied. "
                "Use --output_dir or configure it under wrapper/download sections."
            )
        if not os.path.isdir(final_output_dir):
            raise FileNotFoundError(f"Output folder '{final_output_dir}' is not valid")

        super().__init__(output_dir=final_output_dir, called_module="wrapper")
        self.output_dir = final_output_dir
        req_conf = ["download", "validate"]
        missing = [conf for conf in req_conf if config.get_configuration(conf) is None]
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

        self.download_params = self.clean_module_params("Download", download_cfg)
        self.download_params["output_dir"] = self.output_dir

        if (
            "subfolder" not in self.download_params
            or self.download_params["subfolder"] is None
        ):
            self.download_params["download"].update({"subfolder": "RELECOV"})
            self.log.warning("Subfolder not provided. Set to RELECOV by default")
            stderr.print("[yellow]Subfolder not provided. Set to RELECOV by default")

        self.readmeta_params = dict()

        self.validate_params = self.clean_module_params(
            "Validate", config.get_configuration("validate")
        )

        # intialize the log summary wrapper
        self.wrapper_logsum = self.parent_log_summary(
            output_dir=os.path.join(self.output_dir)
        )

    def clean_module_params(self, module, params):
        active_module = eval(module)
        module_args = inspect.getfullargspec(active_module.__init__)[0]
        module_args.remove("self")
        module_valid_params = {x: y for x, y in params.items() if x in module_args}
        if not module_valid_params:
            self.log.error(f"Invalid params for {module} in config file")
            stderr.print(f"[red]Invalid params for {module} in config file")
            raise ValueError(f"Invalid params for {module}. Use: {module_args}")
        return module_valid_params

    def exec_download(self, download_params):
        if "sftp_port" in download_params:
            sftp_port = download_params.pop("sftp_port", None)
        else:
            sftp_port = None
        download = Download(**download_params)
        if sftp_port is not None:
            download.relecov_sftp.sftp_port = int(sftp_port)
            print(f"SFTP port assigned: {download.relecov_sftp.sftp_port}")
        download.defer_cleanup = True
        download.execute_process()
        finished_folders = download.finished_folders
        download_logs = self.wrapper_logsum.prepare_final_logs(
            logs=download.logsum.logs
        )
        self.download = download
        return finished_folders, download_logs

    def exec_read_metadata(self, readmeta_params):
        read_metadata = LabMetadata(**readmeta_params)
        read_metadata.create_metadata_json()
        read_meta_logs = self.wrapper_logsum.prepare_final_logs(
            logs=read_metadata.logsum.logs
        )
        return read_meta_logs

    def exec_validation(self, validate_params):
        validate_proccess = Validate(**validate_params)
        validate_proccess.execute_validation_process()
        validate_logs = self.wrapper_logsum.prepare_final_logs(
            logs=validate_proccess.logsum.logs
        )
        return validate_logs

    def process_folder(self, key, folder_logs):
        """Executes read-lab-metadata and validation process for the given downloaded folder.
        Merges all the log summaries generated with the ones from download process, creates
        an excel file with custom format and uploads it back to its remote sftp folder.
        Also uploads the files that failed validation back to the remote sftp folder.
        Finally. It cleans all the remote remaining files if the process was successful.

        Args:
            key (str): Name of the folder to process in remote sftp, same name as the one
            included in the log_summary from download process.
            folder_logs (dict): Download log_summary corresponding to the processed folder

        Raises:
            ValueError: If folder_logs dont include a path to the local folder
            ValueError: If folder is not found in remote sftp or more than 1
            ValueError: If no samples/files are found for the folder after download
            ValueError: If no metadata json file is found after read-lab-metadata

        Returns:
            merged_logs (dict): Dictionary which includes the logs from all processes
        """
        local_folder = folder_logs.get("path")
        self.log.info(f"Working in downloaded local folder {local_folder}")
        if not local_folder:
            raise ValueError(f"Couldnt find local path for {key} in log after download")
        files = [os.path.join(local_folder, file) for file in os.listdir(local_folder)]
        try:
            metadata_file = [x for x in files if re.search("lab_metadata.*.xlsx", x)][0]
            samples_file = [x for x in files if re.search("samples_data.*.json", x)][0]
        except IndexError:
            raise ValueError("No metadata/samples files found after download")
        self.readmeta_params.update(
            {
                "metadata_file": metadata_file,
                "sample_list_file": samples_file,
                "output_dir": local_folder,
            }
        )
        read_meta_logs = self.exec_read_metadata(self.readmeta_params)
        # Replicating the way read-lab-metadata output file is named
        file_code = "_".join(["read_lab_metadata", key]) + ".json"
        metadata_json = self.tag_filename(filename=file_code)
        if metadata_json not in os.listdir(local_folder):
            raise ValueError("No metadata json found after read-lab-metadata")
        self.log.info("Merging logs from download and read-lab-metadata")
        previous_logs = self.wrapper_logsum.merge_logs(
            key_name=key, logs_list=[{key: folder_logs}, read_meta_logs]
        )
        temp_logsum = os.path.join(
            local_folder, self.tag_filename("temp") + "_log_summary.json"
        )
        stderr.print(
            "Creating temporary log_summary file with logs from download and read-lab-metadata..."
        )
        self.wrapper_logsum.create_error_summary(
            filepath=temp_logsum, logs=previous_logs
        )
        self.validate_params.update(
            {
                "json_file": os.path.join(local_folder, metadata_json),
                "metadata": metadata_file,
                "logsum_file": temp_logsum,
                "output_dir": local_folder,
                "upload_files": True,
                "check_db": True,
            }
        )
        validate_logs = self.exec_validation(self.validate_params)
        try:
            os.remove(temp_logsum)
        except OSError:
            self.log.warning(f"Could not remove {temp_logsum}")
        merged_logs = self.wrapper_logsum.merge_logs(
            key_name=key, logs_list=[previous_logs, validate_logs]
        )
        stderr.print(f"[green]Merged logs from all processes in {local_folder}")
        self.log.info(f"Merged logs from all processes in {local_folder}")
        return merged_logs

    def run_wrapper(self):
        """Execute each given process in config file sequentially, starting with download.
        Once the download has finished, each downloaded folder is processed using read-lab-metadata
        and validation modules. The logs from each module are merged into a single log-summary.
        These merged logs are then used to create an excel report of all the processes
        """
        self.log.info("Starting with wrapper")
        finished_folders, download_logs = self.exec_download(self.download_params)
        self.set_batch_id(self.download.batch_id)
        if not finished_folders:
            stderr.print("[red]No valid folders found to process")
            self.log.error("No valid folders found to process")
            raise FileNotFoundError("No valid folders found to process")
        stderr.print(f"[blue]Processing {len(finished_folders)} downloaded folders...")
        self.log.info(f"Processing {len(finished_folders)} downloaded folders.")
        counter = 0
        for key, folder_logs in download_logs.items():
            folder = folder_logs.get("path")
            if not folder:
                self.log.error(f"Skipped folder {key}. Logs do not include path field")
                continue
            counter += 1
            logtxt = "Processing folder %s/%s: %s with local path %s"
            self.log.info(logtxt % (counter, str(len(finished_folders)), key, folder))
            stderr.print(logtxt % (counter, str(len(finished_folders)), key, folder))
            try:
                merged_logs = self.process_folder(key, folder_logs)
            except (FileNotFoundError, ValueError) as e:
                self.log.error(f"Could not process folder {key}: {e}")
                folder_logs["errors"].append(f"Could not process folder {key}: {e}")
                log_filepath = os.path.join(
                    folder, self.tag_filename(f"wrapper_{key}.json")
                )
                log_filepath = log_filepath.replace(".json", "_log_summary.json")
                self.parent_create_error_summary(
                    called_module="metadata",
                    filepath=log_filepath,
                    logs={key: folder_logs},
                    to_excel=False,
                )
                continue
            self.wrapper_logsum.create_error_summary(
                called_module="metadata",
                filepath=os.path.join(folder, self.tag_filename(f"{key}.json")),
                logs={key: merged_logs[key]},
                to_excel=True,
            )
            self.wrapper_logsum.logs[key] = merged_logs[key]

        self.base_logsum.logs = self.wrapper_logsum.logs
        self.parent_create_error_summary(
            called_module="wrapper",
            to_excel=True,
        )

        # Logging wrapper stats
        labs = {folder.split("/")[0] for folder in finished_folders}
        num_labs = len(labs)
        samples_per_lab = defaultdict(int)
        for folder, files in finished_folders.items():
            lab = folder.split("/")[0]
            seq_files = [f for f in files if not f.endswith(".xlsx")]
            samples_per_lab[lab] += len(seq_files)
        total_count = sum(samples_per_lab.values())

        stderr.print("[blue] --------------------")
        stderr.print("[blue] WRAPPER SUMMARY")
        stderr.print("[blue] --------------------")
        self.log.info(f"Number of processed laboratories: {num_labs}")
        stderr.print(f"[blue]Number of processed laboratories: {num_labs}")
        self.log.info(f"Total number of processed files: {total_count}")
        stderr.print(f"[blue]Total number of processed files: {total_count}")
        stderr.print("[blue]Processed files for each laboratory")
        for lab, count in samples_per_lab.items():
            self.log.info(f"    {lab}: {count} files")
            stderr.print(f"[blue]    {lab}: {count} files")

        return
