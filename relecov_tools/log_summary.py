#!/usr/bin/env python
import logging
import json
import os
from rich.console import Console
from datetime import datetime
from collections import OrderedDict
from relecov_tools.utils import rich_force_colors


# from relecov_tools.rest_api import RestApi

log = logging.getLogger(__name__)
stderr = Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=rich_force_colors(),
)


class LogSum:
    def __init__(self, output_location: str = None, only_samples: bool = False):
        if not os.path.exists(output_location):
            raise FileNotFoundError("Output location does not exist")
        else:
            self.output_location = output_location
        # if only_samples is given, no "samples" key will be added to logs
        if only_samples:
            self.only_samples = True
        else:
            self.only_samples = False
        self.logs = {}
        return

    def feed_key(self, key, sample=None):
        """Run update_summary() with no entry nor log_type. Add a new empty key"""
        self.update_summary(log_type=None, key=key, entry=None, sample=sample)

    def add_error(self, key, entry, sample=None):
        """Run update_summary() with log_type as errors"""
        log.error(entry)
        self.update_summary(log_type="errors", key=key, entry=entry, sample=sample)
        return

    def add_warning(self, key, entry, sample=None):
        """Run update_summary() with log_type as warnings"""
        log.warning(entry)
        self.update_summary(log_type="warnings", key=key, entry=entry, sample=sample)
        return

    def update_summary(self, key, log_type, entry, sample=None):
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
        if self.only_samples and sample is not None:
            log.warning(
                "No samples record can be added if only_samples is set to True. "
                + f"Record will be added to {current_key}"
            )
            sample = None
        entry, sample = (str(entry), str(sample))
        if current_key not in self.logs.keys():
            self.logs[current_key] = feed_dict.copy()
            if not self.only_samples:
                self.logs[current_key]["samples"] = OrderedDict()
        if log_type is None:
            if sample != "None" and sample not in self.logs[current_key]["samples"]:
                self.logs[current_key]["samples"][sample] = feed_dict.copy()
            return
        if sample == "None":
            self.logs[current_key][log_type].append(entry)
        else:
            if sample not in self.logs[current_key]["samples"].keys():
                self.logs[current_key]["samples"][sample] = feed_dict.copy()
            self.logs[current_key]["samples"][sample][log_type].append(entry)
        return

    def create_error_summary(self, filename=None):
        """Dump the log summary dictionary into a file with json format. If any of
        the 'errors' key is not empty, the parent key value 'valid' is set to false.

        Args:
            filename (str, optional): Name of the output file. Defaults to None.
        """
        for key in self.logs.keys():
            if self.logs[key]["errors"]:
                self.logs[key]["valid"] = False
            if not self.only_samples:
                for sample in self.logs[key]["samples"].keys():
                    if self.logs[key]["samples"][sample]["errors"]:
                        self.logs[key]["samples"][sample]["valid"] = False
        if not filename:
            filename = datetime.today().strftime("%Y%m%d%-H%M%S") + "_log_summary.json"
        summary_path = os.path.join(self.output_location, filename)
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(self.logs, indent=4, sort_keys=True, ensure_ascii=False))
        stderr.print(f"Process log summary printed in {summary_path}")
        return
