#!/usr/bin/env python
import logging

import rich

import relecov_tools.assets.pipeline_utils.utils
import relecov_tools.utils
from relecov_tools.read_bioinfo_metadata import BioinfoReportLog

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


def handle_pangolin_data(files_list: list, **kwargs) -> dict:
    """File handler to parse pangolin data (csv) into JSON structured format.

    Args:
        files_list (list): A list with paths to pangolin files.

    Returns:
        pango_data_processed: A dictionary containing pangolin data handled.
    """

    method_name = f"{handle_pangolin_data.__name__}"
    method_log_report = BioinfoReportLog()
    pango_data_processed = {}
    valid_samples = []
    try:
        files_list_processed = relecov_tools.utils.select_most_recent_files_per_sample(
            files_list
        )
        for pango_file in files_list_processed:
            try:
                pango_data = relecov_tools.utils.read_csv_file_return_dict(
                    pango_file, sep=","
                )
                pango_data_key = next(iter(pango_data))
                pango_data_updated = {
                    str(key).split()[0]: value for key, value in pango_data.items()
                }
                pango_data_processed.update(pango_data_updated)
                valid_samples.append(str(pango_data_key).split()[0])
            except (FileNotFoundError, IndexError) as e:
                method_log_report.update_log_report(
                    method_name,
                    "warning",
                    f"Error occurred while processing file {pango_file}: {e}",
                )
                continue
    except Exception as e:
        method_log_report.update_log_report(
            method_name, "warning", f"Error occurred while processing files: {e}"
        )
    if valid_samples:
        method_log_report.update_log_report(
            method_name,
            "valid",
            f"Successfully handled data in samples: {', '.join(valid_samples)}",
        )
    method_log_report.print_log_report(method_name, ["valid", "warning"])
    return pango_data_processed


def quality_control_evaluation(data: list[dict], **kwargs) -> list[dict]:
    """Evaluates QC status for each sample based on predefined thresholds.

    Args:
    data : list of dict
        List of sample metadata dictionaries containing metrics such as coverage,
        ambiguity, number of Ns, %LDMutations, etc.

    Returns:
    list of dict
        The same list with an added 'qc_test' field per sample:
        - 'pass' if all evaluable conditions are met
        - 'fail' if any condition fails
        - Adds 'qc_failed' field with reasons for failure
    """
    log_report = BioinfoReportLog()
    method_name = f"{quality_control_evaluation.__name__}"

    thresholds = {
        "per_sgene_ambiguous": ("<", 10.0),
        "per_sgene_coverage": (">", 98.0),
        "per_ldmutations": (">", 60.0),
        "number_of_sgene_frameshifts": ("==", 0),
        "number_of_unambiguous_bases": (">", 24000),
        "number_of_Ns": ("<", 5000),
        "per_genome_greater_10x": (">", 90.0),
        "per_reads_host": ("<", 20.0),
    }

    def is_not_evaluable(value):
        return isinstance(value, str) and "Not Evaluable" in value

    def invert_operator(op):
        return {">": "<", "<": ">", ">=": "<=", "<=": ">=", "==": "!=", "!=": "=="}.get(
            op, f"NOT_{op}"
        )

    conditions = {
        k: (
            (
                lambda x, op=op, th=th: (
                    True
                    if is_not_evaluable(x) and k == "per_ldmutations"
                    else (
                        eval(f"{float(x)} {op} {th}")
                        if isinstance(x, (int, float))
                        else False
                    )
                )
            )
        )
        for k, (op, th) in thresholds.items()
    }

    data, warnings = relecov_tools.assets.pipeline_utils.utils.evaluate_qc_samples(
        data,
        thresholds,
        conditions,
        invert_operator,
        is_not_evaluable,
    )

    for warn in warnings:
        log_report.update_log_report(method_name, "warning", warn)

    return data
