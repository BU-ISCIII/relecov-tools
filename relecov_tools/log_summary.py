#!/usr/bin/env python
import logging
import json
import os
import inspect
import copy

import openpyxl
from rich.console import Console
from datetime import datetime
from collections import OrderedDict
from relecov_tools.utils import rich_force_colors


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
        output_location: str = None,
        unique_key: str = None,
        path: str = None,
    ):
        if not os.path.exists(str(output_location)):
            raise FileNotFoundError(f"Output folder {output_location} does not exist")
        else:
            self.output_location = output_location
        # if unique_key is given, all entries will be saved inside that key by default
        if unique_key:
            self.unique_key = unique_key
        else:
            self.unique_key = None
        # if path is given, all keys will include a field "path" with this value
        if path:
            self.path = path
        else:
            self.path = None
        self.logs = {}
        return

    def feed_key(self, key=None, sample=None, path=None):
        """Run update_summary() with no entry nor log_type. Add a new empty key"""
        if self.unique_key:
            key = self.unique_key
        self.update_summary(
            log_type=None, key=key, entry=None, sample=sample, path=path
        )

    def add_error(self, entry, key=None, sample=None, path=None):
        """Run update_summary() with log_type as errors"""
        if self.unique_key:
            key = self.unique_key
        log.error(entry)
        self.update_summary(
            log_type="errors", key=key, entry=entry, sample=sample, path=path
        )
        return

    def add_warning(self, entry, key=None, sample=None, path=None):
        """Run update_summary() with log_type as warnings"""
        if self.unique_key:
            key = self.unique_key
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
            if logs[key]["errors"]:
                logs[key]["valid"] = False
            if logs[key].get("samples") is not None:
                for sample in logs[key]["samples"].keys():
                    if logs[key]["samples"][sample]["errors"]:
                        logs[key]["samples"][sample]["valid"] = False
        return logs

    def merge_logs(self, key_name, logs_list):
        """Merge a multiple set of logs without losing information"""
        if not logs_list:
            return
        merged_logs = copy.deepcopy(logs_list[0])
        if not merged_logs["samples"]:
            merged_logs["samples"] = {}
        for logs in logs_list:
            merged_logs["errors"].extend(logs["errors"])
            merged_logs["warnings"].extend(logs["warnings"])
            if "samples" in logs.keys():
                for sample, vals in logs["samples"].items():
                    if sample not in merged_logs["samples"].keys():
                        merged_logs["samples"][sample] = vals
                    else:
                        merged_logs["samples"][sample]["errors"].extend(
                            logs["samples"][sample]["errors"]
                        )
                        merged_logs["samples"][sample]["warnings"].extend(
                            logs["samples"][sample]["warnings"]
                        )
        final_logs = {key_name: merged_logs}
        return final_logs

    def create_logs_excel(self, logs, called_module=""):
        """Create an excel file with logs information

        Args:
            logs (dict, optional): Custom dictionary of logs. Useful to create outputs
            called_module (str, optional): Name of the module running this code.
        """

        def translate_fields(samples_logs):
            # TODO Translate logs to spanish using a local translator model like deepl
            return

        date = datetime.today().strftime("%Y%m%d%-H%M%S")
        lab_code = list(logs.keys())[0]
        if self.unique_key:
            excel_filename = "_".join(
                [self.unique_key, called_module, date, "report.xlsx"]
            ).replace("__", "")
        else:
            excel_filename = "_".join([lab_code, date, "report.xlsx"])
        if not logs.get("samples"):
            try:
                samples_logs = logs[lab_code].get("samples")
            except (KeyError, AttributeError) as e:
                stderr.print(f"[red]Could not convert log summary to excel: {e}")
                return
        else:
            samples_logs = logs.get("samples")

        workbook = openpyxl.Workbook()
        main_worksheet = workbook.active
        main_worksheet.title = "Samples Report"
        main_headers = ["Sample ID given for sequencing", "Valid", "Errors"]
        main_worksheet.append(main_headers)
        warnings_sheet = workbook.create_sheet("Other warnings")
        warnings_headers = ["Sample ID given for sequencing", "Valid", "Warnings"]
        warnings_sheet.append(warnings_headers)
        for sample, logs in samples_logs.items():
            error_row = [sample, str(logs["valid"]), "; ".join(logs["errors"])]
            main_worksheet.append(error_row)
            warning_row = [sample, str(logs["valid"]), "; ".join(logs["warnings"])]
            warnings_sheet.append(warning_row)
        excel_outpath = os.path.join(self.output_location, excel_filename)
        workbook.save(excel_outpath)
        stderr.print(f"[green]Successfully created logs excel in {excel_outpath}")
        return

    def create_error_summary(
        self, called_module=None, filename=None, logs=None, to_excel=False
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
                stderr.print("[red]Logs input must be a dict. No output file.")
                return
        final_logs = self.prepare_final_logs(logs)
        if not called_module:
            try:
                called_module = [
                    f.function for f in inspect.stack() if "__main__.py" in f.filename
                ][0]
            except IndexError:
                called_module = ""
        if not filename:
            date = datetime.today().strftime("%Y%m%d%-H%M%S")
            filename = "_".join([date, called_module, "log_summary.json"])
        summary_path = os.path.join(self.output_location, filename)
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(
                json.dumps(final_logs, indent=4, sort_keys=False, ensure_ascii=False)
            )
        stderr.print(f"Process log summary saved in {summary_path}")
        if to_excel is True:
            self.create_logs_excel(final_logs, called_module)
        return
