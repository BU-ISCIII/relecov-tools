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
    """Evaluates QC status for each sample based on predefined thresholds.

    Parameters:
    -----------
    data : list of dict
        List of sample metadata dictionaries containing metrics such as coverage,
        ambiguity, number of Ns, %LDMutations, etc.

    Returns:
    --------
    list of dict
        The same list with an added 'qc_test' field per sample:
        - 'pass' if all evaluable conditions are met
        - 'fail' if any condition fails
        - Adds 'qc_failed' field with reasons for failure
    """
    log_report = BioinfoReportLog()
    method_name = f"{quality_control_evaluation.__name__}"

    thresholds = {
        "number_of_unambiguous_bases": (">", 11000),
        "number_of_Ns": ("<", 2300),
        "pass_reads": (">", 23000),
        "per_reads_host": ("<", 20.0),
        "per_hagene_coverage": (">", 98.0),
        "per_nagene_coverage": (">", 98.0)
    }

    def invert_operator(op):
        return {">": "<", "<": ">", ">=": "<=", "<=": ">=", "==": "!=", "!=": "=="}.get(
            op, f"NOT_{op}"
        )

    conditions = {
        k: (lambda x, op=op, th=th: eval(f"{float(x)} {op} {th}") if isinstance(x, (int, float)) else False)
        for k, (op, th) in thresholds.items()
    }

    for sample in data:
        try:
            qc_status = "pass"
            failed_reasons = []

            for param, condition in conditions.items():
                value = sample.get(param)
                try:
                    if value is None or not condition(value):
                        qc_status = "fail"
                        op, th = thresholds[param]
                        inverted_op = invert_operator(op)
                        failed_reasons.append(f"({param} {inverted_op} {th})")
                except (TypeError, ValueError):
                    qc_status = "fail"
                    failed_reasons.append(f"({param} = {value} invalid)")
                    log_report.update_log_report(
                        method_name,
                        "warning",
                        f"Sample {sample['sequencing_sample_id']} has unevaluable value for {param}: {value}",
                    )

            sample["qc_test"] = qc_status
            if qc_status == "fail" and failed_reasons:
                sample["qc_failed"] = " -- ".join(failed_reasons)

            log_report.update_log_report(
                method_name,
                "valid",
                f"{sample['sequencing_sample_id']} evaluated: {qc_status}",
            )

        except (TypeError, ValueError, AttributeError) as e:
            sample["qc_test"] = "fail"
            sample["qc_failed"] = f"(evaluation_error = {str(e)})"
            sample_id = sample.get("sequencing_sample_id", "unknown")
            log_report.update_log_report(
                method_name, "warning", f"Error evaluating sample {sample_id}: {e}"
            )

    return data


def get_software_versions(files_list, file_tag, pipeline_name, output_dir=None):
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
