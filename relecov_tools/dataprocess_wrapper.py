#!/usr/bin/env python
import logging
import re
import yaml
import os
import sys
from datetime import datetime
import inspect
import rich.console
from relecov_tools.download_manager import DownloadManager
from relecov_tools.read_lab_metadata import RelecovMetadata
from relecov_tools.json_validation import SchemaValidation
import relecov_tools.log_summary
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class ProcessWrapper:
    """
    Always fill all the arguments for the class in the config file, leave its value
    if you dont want to use that argument e.g.(target_folders:  ) -> (target_folders = None)
    """

    def __init__(self, config_file: str = None, output_folder: str = None):
        if not os.path.isdir(str(output_folder)):
            sys.exit(FileNotFoundError(f"Output folder {output_folder} is not valid"))
        else:
            self.output_folder = output_folder
        if not os.path.isfile(str(config_file)):
            sys.exit(FileNotFoundError(f"Config file {config_file} is not a file"))
        else:
            try:
                self.config_data = relecov_tools.utils.read_yml_file(config_file)
                # Config file should include a key
            except yaml.YAMLError as e:
                sys.exit(yaml.YAMLError(f"Invalid config file: {e}"))
        output_regex = ("out_folder", "output_folder", "output_location")
        for key, val in self.config_data.items():
            for arg in output_regex:
                if val == arg:
                    self.config_data[key] = self.output_folder
        self.wrapper_logsum = relecov_tools.log_summary.LogSum(
            output_location=os.path.join(self.output_folder, "logs")
        )
        self.config_data["download"].update({"output_location": output_folder})
        self.download_params = self.clean_module_params(
            "DownloadManager", self.config_data["download"]
        )
        self.readmeta_params = self.clean_module_params(
            "RelecovMetadata", self.config_data["read-lab-metadata"]
        )
        self.validate_params = self.clean_module_params(
            "SchemaValidation", self.config_data["validate"]
        )
        self.date = datetime.today().strftime("%Y%m%d%H%M%S")

    def clean_module_params(self, module, params):
        active_module = eval(module)
        module_args = inspect.getfullargspec(active_module.__init__)[0]
        module_args.remove("self")
        module_valid_params = {x: y for x, y in params.items() if x in module_args}
        if not module_valid_params:
            stderr.print(f"[red]Invalid params for {module} in config file")
            sys.exit(1)
        return module_valid_params

    def exec_download(self, download_params):
        download_manager = DownloadManager(**download_params)
        download_manager.execute_process()
        finished_folders = download_manager.finished_folders
        download_logs = self.wrapper_logsum.prepare_final_logs(
            logs=download_manager.logsum.logs
        )
        self.download_manager = download_manager
        return finished_folders, download_logs

    def exec_read_metadata(self, readmeta_params):
        read_metadata = RelecovMetadata(**readmeta_params)
        read_metadata.create_metadata_json()
        read_meta_logs = self.wrapper_logsum.prepare_final_logs(
            logs=read_metadata.logsum.logs
        )
        return read_meta_logs

    def exec_validation(self, validate_params):
        validate_proccess = SchemaValidation(**validate_params)
        valid_json_data, invalid_json = validate_proccess.validate()
        validate_logs = self.wrapper_logsum.prepare_final_logs(
            logs=validate_proccess.logsum.logs
        )
        return valid_json_data, invalid_json, validate_logs

    def process_folder(self, finished_folders, key, folder_logs):
        """Executes read-lab-metadata and validation process for the given downloaded folder.
        Merges all the log summaries generated with the ones from download process, creates
        an excel file with custom format and uploads it back to its remote sftp folder.
        Also uploads the files that failed validation back to the remote sftp folder.
        Finally. It cleans all the remote remaining files if the process was successful.

        Args:
            finished_folders (dict(str:list)): Dictionary which includes the names
            of the remote folders processed during download and the successfull files for each
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

        def upload_files_from_json(invalid_json, remote_dir):
            """Upload the files in a given json with samples metadata"""
            for sample in invalid_json:
                local_dir = sample.get("r1_fastq_filepath")
                # files_keys = [key for key in sample.keys() if "_file_" in key]
                sample_files = (
                    sample.get("sequence_file_R1_fastq"),
                    sample.get("sequence_file_R2_fastq"),
                )
                ftp_files = self.download_manager.relecov_sftp.get_file_list(remote_dir)
                uploaded_files = []
                for file in sample_files:
                    if not file or file in ftp_files:
                        continue
                    loc_path = os.path.join(local_dir, file)
                    sftp_path = os.path.join(remote_dir, file)
                    log.info("Uploading %s to remote %s" % (loc_path, remote_dir))
                    uploaded = self.download_manager.relecov_sftp.upload_file(
                        loc_path, sftp_path
                    )
                    if not uploaded:
                        err = f"Could not upload {loc_path} to {remote_dir}"
                        self.wrapper_logsum.add_error(sample=sample, entry=err)
                    else:
                        uploaded_files.append(file)
            return uploaded_files

        local_folder = folder_logs.get("path")
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
                "output_folder": local_folder,
            }
        )
        read_meta_logs = self.exec_read_metadata(self.readmeta_params)
        metadata_json = [
            x for x in os.listdir(local_folder) if re.search("lab_metadata.*.json", x)
        ]
        if not metadata_json:
            raise ValueError("No metadata json found after read-lab-metadata")
        self.validate_params.update(
            {
                "json_data_file": os.path.join(local_folder, metadata_json[0]),
                "metadata": metadata_file,
                "out_folder": local_folder,
            }
        )
        valid_json_data, invalid_json, validate_logs = self.exec_validation(
            self.validate_params
        )
        merged_logs = self.wrapper_logsum.merge_logs(
            key_name=key, logs_list=[{key: folder_logs}, read_meta_logs, validate_logs]
        )
        stderr.print(f"[green]Merged logs from all processes in {local_folder}")
        sftp_dirs = self.download_manager.relecov_sftp.list_remote_folders(key)
        sftp_dirs_paths = [os.path.join(key, d) for d in sftp_dirs]
        valid_dirs = [d for d in sftp_dirs_paths if d in finished_folders.keys()]
        # As all folders are merged into one during download, there should only be 1 folder
        if not valid_dirs or len(valid_dirs) >= 2:
            # If all samples were valid during download and download_clean is used, the original folder might have been deleted
            log.warning("Couldnt find %s folder in remote sftp. Creating new one", key)
            remote_dir = os.path.join(key, self.date + "_invalid_samples")
            self.download_manager.relecov_sftp.make_dir(remote_dir)
        else:
            remote_dir = valid_dirs[0]
            stderr.print(
                f"[blue]Cleaning successfully validated files from {remote_dir}"
            )
            log.info("Cleaning successfully validated files from remote dir")
            file_fields = ("sequence_file_R1_fastq", "sequence_file_R2_fastq")
            valid_sampfiles = [
                f.get(key) for key in file_fields for f in valid_json_data
            ]
            valid_files = [
                f for f in finished_folders[remote_dir] if f in valid_sampfiles
            ]
            self.download_manager.delete_remote_files(remote_dir, files=valid_files)
            self.download_manager.delete_remote_files(remote_dir, skip_seqs=True)
            self.download_manager.clean_remote_folder(remote_dir)
        if invalid_json:
            logtxt = f"Found {len(invalid_json)} invalid samples in {key}"
            self.wrapper_logsum.add_warning(key=key, entry=logtxt)
            assets = os.path.join(os.path.dirname(os.path.realpath(__file__)), "assets")
            metadata_template = [
                x for x in os.listdir(assets) if re.search("metadata_templat.*.xlsx", x)
            ][0]
            sftp_path = os.path.join(remote_dir, os.path.basename(metadata_template))
            log.info("Uploading invalid files and template to %s", remote_dir)
            stderr.print(f"[blue]Uploading invalid files and template to {remote_dir}")
            self.download_manager.relecov_sftp.upload_file(
                os.path.join(assets, metadata_template), sftp_path
            )
            # Upload all the files that failed validation process back to sftp
            upload_files_from_json(invalid_json, remote_dir)
        else:
            log.info("No invalid samples in %s", key)
            stderr.print(f"[green]No invalid samples were found for {key} !!!")
        log_filepath = os.path.join(local_folder, str(key) + "_metadata_report.json")
        self.wrapper_logsum.create_error_summary(
            called_module="metadata",
            filepath=log_filepath,
            logs=merged_logs,
            to_excel=True,
        )
        xlsx_report_files = [
            f for f in os.listdir(local_folder) if re.search("metadata_report.xlsx", f)
        ]
        if xlsx_report_files:
            log.info("Uploading %s xlsx report to remote %s" % (key, remote_dir))
            local_xlsx = os.path.join(local_folder, xlsx_report_files[0])
            remote_xlsx = os.path.join(remote_dir, xlsx_report_files[0])
            up = self.download_manager.relecov_sftp.upload_file(local_xlsx, remote_xlsx)
            if not up:
                log.error(
                    "Could not upload %s report to remote %s" % (key, local_folder)
                )
        else:
            log.error("Could not find xlsx report for %s in %s" % (key, local_folder))
        return merged_logs

    def run_wrapper(self):
        """Execute each given process in config file sequentially, starting with download.
        Once the download has finished, each downloaded folder is processed using read-lab-metadata
        and validation modules. The logs from each module are merged into a single log-summary.
        These merged logs are then used to create an excel report of all the processes
        """
        finished_folders, download_logs = self.exec_download(self.download_params)
        if not finished_folders:
            stderr.print("[red]No valid folders found to process")
            sys.exit(1)
        stderr.print(f"[blue]Processing {len(finished_folders)} downloaded folders...")
        counter = 0
        for key, folder_logs in download_logs.items():
            folder = folder_logs.get("path")
            if not folder:
                log.error(f"Skipped folder {key}. Logs do not include path field")
                continue
            if not folder_logs.get("valid"):
                log.error(f"Folder {key} is set as invalid in logs. Skipped.")
                continue
            counter += 1
            logtxt = "Processing folder %s/%s: %s with local path %s"
            log.info(logtxt % (counter, str(len(finished_folders)), key, folder))
            stderr.print(logtxt % (counter, str(len(finished_folders)), key, folder))
            try:
                merged_logs = self.process_folder(finished_folders, key, folder_logs)
            except (FileNotFoundError, ValueError) as e:
                log.error(f"Could not process folder {key}: {e}")
                folder_logs["errors"].append(f"Could not process folder {key}: {e}")
                log_filepath = os.path.join(
                    folder, self.date + "_" + str(key) + "_wrapper_summary.json"
                )
                self.wrapper_logsum.create_error_summary(
                    called_module="metadata",
                    filepath=log_filepath,
                    logs={key: folder_logs},
                    to_excel=False,
                )
                continue
            self.wrapper_logsum.logs[key] = merged_logs[key]
        self.wrapper_logsum.create_error_summary(
            called_module="wrapper",
            to_excel=True,
        )
        return
