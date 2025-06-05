#!/usr/bin/env python
import os
import logging
import rich
import os.path
import pandas as pd

import relecov_tools.utils
from relecov_tools.read_bioinfo_metadata import BioinfoReportLog

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


def quality_control_evaluation(data):
    """Evaluate the quality of the samples and add the field 'qc_test' to each 'data' entry."""
    conditions = {
        "number_of_unambiguous_bases": lambda x: int(x) > 24000,
        "number_of_Ns": lambda x: int(x) < 5000,
        "pass_reads": lambda x: int(x) > 50000,
        "per_reads_host": lambda x: float(x) < 20,
    }
    for sample in data:
        try:
            qc_status = "pass"
            for param, condition in conditions.items():
                value = sample.get(param)
                if value is None or not condition(value):
                    qc_status = "fail"
                    break
            sample["qc_test"] = qc_status
        except ValueError as e:
            sample["qc_test"] = "fail"
            print(
                f"Error processing sample {sample.get('sequencing_sample_id', 'unknown')}: {e}"
            )
    return data


def get_software_versions(files_list, file_tag, pipeline_name, output_folder=None):
    """File handler to parse software versions from csv.

    Args:
        files_list (list): A list with paths to software version files.

    Returns:
        software_versions: A dictionary containing software versions.
    """
    method_name = f"{get_software_versions.__name__}"
    method_log_report = BioinfoReportLog()

    version_dict = {}
    for file in files_list:
        if os.path.isfile(file):
            df = pd.read_csv(file)
            for _, row in df.iterrows():
                name = row["software_name"]
                version = row["software_version"]
                id = row["software_name"].strip().lower()
                if "/" not in id:
                    id = id.split()[0]
                version_dict[id] = {"software_version": version, "software_name": name}
        else:
            method_log_report.update_log_report(
                method_name,
                "warning",
                f"Verions file {file} not found. Skipping.",
            )
    return version_dict
