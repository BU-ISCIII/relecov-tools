#!/usr/bin/env python
"""
Common utility function used for relecov_tools package.
"""
import os
import re
import sys
import yaml
import logging
import rich
import json

from pathlib import Path
from datetime import datetime

from relecov_tools.config_json import ConfigJson
from relecov_tools.read_bioinfo_metadata import BioinfoReportLog
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

def get_software_versions_yml(files_list, file_tag, pipeline_name, output_folder=None):
    """File handler to parse software versions from yaml.

    Args:
        files_list (list): A list with paths to software version files.

    Returns:
        version_dict: Dictionary of software versions in the format:
              { software_id: {"software_version": X, "software_name": Y} }
    """

    method_name = f"{get_software_versions_yml.__name__}"
    method_log_report = BioinfoReportLog()

    version_dict = {}

    for file in files_list:
        if not os.path.isfile(file):
            print(f"[WARNING] Version file {file} not found. Skipping.")
            method_log_report.update_log_report(
                method_name, "warning", f"Version file {file} not found. Skipping."
            )
            continue

        with open(file, "r") as f:
            try:
                yml_data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                print(f"[ERROR] Failed to parse YAML file {file}: {e}")
                method_log_report.update_log_report(
                    method_name, "warning", f"Failed to parse YAML file {file}: {e}"
                )
                continue

        for section in yml_data.values():
            for name, version in section.items():
                software_name = name.strip()
                software_version = version.strip() if isinstance(version, str) else str(version)
                software_id = software_name.lower()
                if "/" not in software_id:
                    software_id = software_id.split()[0]
                version_dict[software_id] = {
                    "software_version": software_version,
                    "software_name": software_name
                }

    return version_dict
