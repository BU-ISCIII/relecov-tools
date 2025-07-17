#!/usr/bin/env python
import logging
import json
import os
import inspect
import copy
import re

import openpyxl
from rich.console import Console
from datetime import datetime
from collections import OrderedDict
from relecov_tools.utils import rich_force_colors
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson


log = logging.getLogger(__name__)
stderr = Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=rich_force_colors(),
)


class LogSum:
    def __init__(
        self,
        output_dir: str = None,
        lab_code: str = None,
        path: list = None,
    ):
        if output_dir is not None:
            if not os.path.isdir(str(output_dir)):
                try:
                    os.makedirs(output_dir, exist_ok=True)
                except IOError:
                    raise IOError(f"Logs output folder {output_dir} doesnt exist")
        else:
            log.info("No output_dir provided, selecting it from config...")
            config_json = ConfigJson(extra_config=True)
            logs_config = config_json.get_topic_data("general", "logs_config")
            output_dir = logs_config.get("default_outpath", "/tmp")

        log.info(f"Log summary outpath set to {output_dir}")
        self.output_dir = output_dir

        # Store new arguments for possible future use
        self.lab_code = lab_code
        self.path = path

        # Map legacy attributes to new arguments for compatibility
        self.lab_code = lab_code if lab_code else None
        self.path = path if path else None
        self.logs = {}
        return

    def feed_key(self, key=None, sample=None, path=None):
        """Run update_summary() with no entry nor log_type. Add a new empty key"""
        if self.lab_code:
            key = self.lab_code
        self.update_summary(
            log_type=None, key=key, entry=None, sample=sample, path=path
        )

    def add_error(self, entry, key=None, sample=None, path=None):
        """Run update_summary() with log_type as errors"""
        if self.lab_code:
            key = self.lab_code
        log.error(entry)
        self.update_summary(
            log_type="errors", key=key, entry=entry, sample=sample, path=path
        )
        return

    def add_warning(self, entry, key=None, sample=None, path=None):
        """Run update_summary() with log_type as warnings"""
        if self.lab_code:
            key = self.lab_code
        log.warning(entry)
        self.update_summary(
            log_type="warnings", key=key, entry=entry, sample=sample, path=path
        )
        return

    def update_summary(self, log_type, key, entry, sample=None, path=None):
        """Create a dictionary with a defined structure for each new key. Add the
        entry to the dictionary if it already exists. Add it to samples if its a sample

        Args:
            key (str): Name of the key holding the logs. A folder or a sample.
            log_type (str): Type of log being added. Either 'errors' or 'warnings'
            entry (str): Content message of the log.
            sample (str, optional): Name of a sample within key if the log is for it
            one sample instead of the whole key/folder. Defaults to None.
        """
        feed_dict = OrderedDict({"valid": True, "errors": [], "warnings": []})
        # Removing strange characters
        current_key = str(key).replace("./", "")
        entry, sample = (str(entry), str(sample))
        if current_key not in self.logs.keys():
            self.logs[current_key] = copy.deepcopy(feed_dict)
            self.logs[current_key]["samples"] = OrderedDict()
        if self.path:
            self.logs[current_key].update({"path": str(self.path)})
        if path is not None:
            self.logs[current_key].update({"path": str(path)})
        if log_type is None:
            if sample != "None" and sample not in self.logs[current_key]["samples"]:
                self.logs[current_key]["samples"][sample] = copy.deepcopy(feed_dict)
            return
        if sample == "None":
            self.logs[current_key][log_type].append(entry)
        else:
            if sample not in self.logs[current_key]["samples"].keys():
                self.logs[current_key]["samples"][sample] = copy.deepcopy(feed_dict)
            self.logs[current_key]["samples"][sample][log_type].append(entry)
        return

    def prepare_final_logs(self, logs):
        """Sets valid field to false if any errors were found for each key/sample

        Args:
            logs (dict): Custom dictionary of logs.

        Returns:
            logs: logs with updated valid field values
        """
        for key in logs.keys():
            if logs[key].get("errors"):
                logs[key]["valid"] = False
            if logs[key].get("samples") is not None:
                for sample in logs[key]["samples"].keys():
                    if logs[key]["samples"][sample]["errors"]:
                        logs[key]["samples"][sample]["valid"] = False
        return logs

    def merge_logs(self, logs_list, key_name=None):
        """Merge a multiple set of logs without losing information

        Args:
            logs_list (list(dict)): List of logs for different processes,
            logs should only include the actual records,
            key_name (str, Optional): Name of the final key holding the logs.
            If None, all the keys found in logs_list will be included.

        Returns:
            final_logs (dict): Merged list of logs into a single record
        """

        def add_new_logs(current_logs, new_logs):
            """Merge two logs including all its keys"""
            merged_logs = copy.deepcopy(current_logs)

            for key, values in new_logs.items():
                if key not in merged_logs:
                    merged_logs[key] = copy.deepcopy(values)
                else:
                    for field in ["errors", "warnings"]:
                        merged_logs[key].setdefault(field, [])
                        merged_logs[key][field].extend(values.get(field, []))
                        # Remove repeated elements
                        merged_logs[key][field] = list(
                            dict.fromkeys(merged_logs[key][field])
                        )
                    merged_logs[key].setdefault("samples", {})
                    for sample, vals in new_logs[key].get("samples", {}).items():
                        if sample not in merged_logs[key]["samples"].keys():
                            merged_logs[key]["samples"][sample] = vals
                        else:
                            merged_logs[key]["samples"][sample]["errors"].extend(
                                new_logs[key]["samples"][sample]["errors"]
                            )
                            merged_logs[key]["samples"][sample]["warnings"].extend(
                                new_logs[key]["samples"][sample]["warnings"]
                            )
                    merged_logs[key].setdefault("path", self.output_dir)
            return merged_logs

        if not logs_list:
            return {}
        if len(logs_list) == 1:
            if key_name is None:
                return logs_list[0]
            else:
                if key_name in logs_list[0]:
                    return logs_list[0]
                else:
                    return {}
        proc_logs_list = []
        for logs in logs_list:
            if key_name is None:
                proc_logs_list.append(logs)
            else:
                proc_logs_list.append({key_name: logs.get(key_name, {})})
        merged_logs = proc_logs_list[0]
        for idx, logs in enumerate(proc_logs_list[1:]):
            if not logs:
                err = f"Logs {idx} were empty. Check if key {key_name} is present"
                log.warning(err)
                continue
            try:
                merged_logs = add_new_logs(merged_logs, logs)
            except (TypeError, KeyError) as e:
                err = f"Could not add logs {idx} in list: {e}"
                stderr.print(f"[red]{err}")
                log.error(err)
        return merged_logs

    def create_logs_excel(self, logs, excel_outpath):
        """Create an excel file with logs information

        Args:
            logs (dict, optional): Custom dictionary of logs. Useful to create outputs
            excel_outpath (str): Path to output excel file
        """

        def reg_remover(string, pattern):
            """Remove annotation between brackets in logs message"""
            string = str(string)
            string = string.replace("['", "'").replace("']", "'").replace('"', "")
            string = re.sub(pattern, "", string)
            return string.strip()

        def feed_logs_to_excel(logs, excel_outpath):
            """Feed the data from logs into an excel file, creating different
            sheets depending on the provided list from configuration file"""

            workbook = openpyxl.Workbook()
            # TODO: Include these fields in configuration.json
            sheet_names_and_headers = {
                "Global Report": ["Lab_id", "Valid", "Errors", "Warnings"],
                "Samples Report": [
                    "Lab_id",
                    "Sample ID given for sequencing",
                    "Valid",
                    "Errors",
                ],
                "Other warnings": [
                    "Lab_id",
                    "Sample ID given for sequencing",
                    "Valid",
                    "Warnings",
                ],
            }
            for name, header in sheet_names_and_headers.items():
                new_sheet = workbook.create_sheet(name)
                new_sheet.append(header)
            regex = r"[\[\]]"  # Regex to remove lists brackets

            for key, logs in logs.items():
                if not logs.get("samples"):
                    log.warning(f"No samples found for key {key}")
                    try:
                        samples_logs = logs[key]["samples"]
                    except (KeyError, AttributeError) as e:
                        err = f"Could not convert log summary to excel for {key}: {e}"
                        stderr.print(f"[red]{err}")
                        log.error(err)
                        workbook.close()
                        return
                else:
                    samples_logs = logs.get("samples")
                if not samples_logs:
                    logs["Warnings"].append("No samples found to report")

                valid = logs.get("valid", False)
                warnings = logs.get("warnings", [])

                warnings_list = warnings if isinstance(warnings, list) else [warnings]
                truncated_warnings = []
                max_lenght = 250

                for warning in warnings_list:
                    warnings_str = str(warning)
                    if len(warnings_str) > max_lenght:
                        warnings_str = warnings_str[:max_lenght] + "..."
                    truncated_warnings.append(warnings_str)
                warnings_cleaned = "; ".join(truncated_warnings)

                errors_list = logs.get("errors", [])
                errors_list = (
                    errors_list if isinstance(errors_list, list) else [errors_list]
                )

                truncated_errors = []

                for err in errors_list:
                    err_str = reg_remover(str(err), regex)
                    if len(err_str) > max_lenght:
                        err_str = err_str[:max_lenght] + "..."
                    truncated_errors.append(err_str)

                errors_cleaned = "; ".join(truncated_errors)

                workbook["Global Report"].append(
                    [str(key), str(valid), errors_cleaned, warnings_cleaned]
                )

                regex = (
                    r"\[.*?\]"  # Regex to remove ontology annotations between brackets
                )

                for sample, slog in samples_logs.items():
                    clean_errors = []
                    for x in slog["errors"]:
                        err_str = reg_remover(str(x), regex)
                        if len(err_str) > max_lenght:
                            err_str = err_str[:max_lenght] + "..."
                        clean_errors.append(err_str)
                    error_row = [
                        str(key),
                        sample,
                        str(slog["valid"]),
                        "\n".join(clean_errors),
                    ]
                    workbook["Samples Report"].append(error_row)

                    clean_warngs = []
                    for x in slog["warnings"]:
                        war_str = reg_remover(str(x), regex)
                        if len(war_str) > max_lenght:
                            war_str = war_str[:max_lenght] + "..."
                        clean_warngs.append(war_str)
                    warning_row = [
                        str(key),
                        sample,
                        str(slog["valid"]),
                        "\n ".join(clean_warngs),
                    ]
                    workbook["Other warnings"].append(warning_row)

            # Adjusting the size of the columns in the excel file
            for name in sheet_names_and_headers.keys():
                relecov_tools.utils.adjust_sheet_size(workbook[name])
            del workbook["Sheet"]
            workbook.save(excel_outpath)
            stderr.print(f"[green]Successfully created logs excel in {excel_outpath}")
            return

        def translate_fields(samples_logs):
            # TODO Translate logs to spanish using a local translator model like deepl
            return

        if not os.path.exists(os.path.dirname(excel_outpath)):
            os.makedirs(os.path.dirname(excel_outpath), exist_ok=True)
            log.warning(
                "Given report outpath does not exist, created it automatically: %s",
                os.path.dirname(excel_outpath),
            )
        file_ext = os.path.splitext(excel_outpath)[-1]
        excel_outpath = excel_outpath.replace(file_ext, ".xlsx")

        feed_logs_to_excel(logs, excel_outpath)

        return

    def create_error_summary(
        self, called_module=None, filepath=None, logs=None, to_excel=False
    ):
        """Dump the log summary dictionary into a file with json format. If any of
        the 'errors' key is not empty, the parent key value 'valid' is set to false.

        Args:
            called_module (str, optional): Name of the module running this code.
            filename (str, optional): Name of the output file. Defaults to None.
            logs (dict, optional): Custom dictionary of logs. Useful to create outputs
            with selective information within all logs. Key names must remain the same.
            to_excel (bool, optional): Wether to output logs in excel format or not
        """
        if logs is None:
            logs = self.logs
        else:
            if not isinstance(logs, dict):
                log.error("Logs input must be a dict. No output file generated.")
                stderr.print("[red]Logs input must be a dict. No output file.")
                return
        if not called_module:
            traceback_functions = [
                f.function for f in inspect.stack() if "__main__.py" in f.filename
            ]
            if traceback_functions:
                called_module = traceback_functions[0]
            else:
                called_module = ""
        if not filepath:
            date = datetime.today().strftime("%Y%m%d%-H%M%S")
            filename = "_".join([date, called_module, "log_summary.json"])
            os.makedirs(self.output_dir, exist_ok=True)
            filepath = os.path.join(self.output_dir, filename)
        else:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
        if os.path.exists(filepath):
            log.info(f"{filepath} already exists, merging its content...")
            with open(filepath, "r", encoding="utf-8") as f:
                present_logs = json.load(f)
            if self.lab_code:
                merge_key = self.lab_code
            else:
                merge_key = None
            try:
                merged_logs = self.merge_logs(
                    key_name=merge_key, logs_list=[present_logs, logs]
                )
                logs = merged_logs
            except Exception as e:
                filepath = filepath.replace(".json", "_2.json")
                err = f"Could not merge logs: {e}. Saving logs in {filepath} instead"
                log.error(err)
        final_logs = self.prepare_final_logs(logs)
        with open(filepath, "w", encoding="utf-8") as f:
            try:
                f.write(
                    json.dumps(
                        final_logs, indent=4, sort_keys=False, ensure_ascii=False
                    )
                )
                stderr.print(f"Process log summary saved in {filepath}")
                if to_excel is True:
                    self.create_logs_excel(
                        final_logs, filepath.replace("log_summary", "report")
                    )
            except Exception as e:
                stderr.print(f"[red]Error exporting logs to file: {e}")
                log.error("Error exporting logs to file: %s", str(e))
                f.write(str(final_logs))
        return

    def rename_log_key(self, old_key, new_key):
        """Rename a key in the logs

        Args:
            old_key (str): Current key name
            new_key (str): New key name
        """
        if old_key in self.logs.keys():
            if new_key not in self.logs.keys():
                self.logs[new_key] = self.logs.pop(old_key)
            else:
                log.warning(
                    f"Could not rename logsum key {old_key}: {new_key} already in logs"
                )
        else:
            log.warning(f"Could not rename logsum key {old_key}: key not in logs")
        return

    @staticmethod
    def get_invalid_count(validation_logs):
        """
        Counts the number of invalid samples in the logs data by checking the `valid` field.

        Args:
            validation_logs (dict): Dictionary containing the validation logs.

        Returns:
            dict: Dictionary with entry_key as keys and counts of invalid samples as values.
        """
        invalid_counts = {}
        for entry_key, entry_value in validation_logs.items():
            if "samples" in entry_value:
                samples = entry_value["samples"]
                for sample_key, sample_value in samples.items():
                    if "valid" in sample_value and not sample_value["valid"]:
                        if invalid_counts.get(entry_key):
                            invalid_counts[entry_key] += 1
                        else:
                            invalid_counts[entry_key] = 1
        return invalid_counts
