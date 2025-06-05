#!/usr/bin/env python
import os
import sys
import logging
import rich
import os.path
import pandas as pd
from Bio import SeqIO


import relecov_tools.utils
from relecov_tools.read_bioinfo_metadata import BioinfoReportLog

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


# START util functions
def handle_consensus_fasta(files_list, file_tag, pipeline_name, output_folder=None):
    """File handler to parse consensus data (fasta) into JSON structured format.

    Args:
        files_list (list): A list with paths to condensus files.

    Returns:
        consensus_data_processed: A dictionary containing consensus data handled.
    """
    method_name = f"{handle_consensus_fasta.__name__}"
    method_log_report = BioinfoReportLog()

    consensus_data_processed = {}
    missing_consens = []
    for consensus_file in files_list:
        sequence_names = []
        genome_length = 0
        try:
            record_fasta = SeqIO.parse(consensus_file, "fasta")
            for record in record_fasta:
                sequence_names.append(record.description)
                genome_length += len(record.seq)
        except FileNotFoundError as e:
            missing_consens.append(e.filename)
            continue
        sample_key = os.path.basename(consensus_file).split(".")[0]
        # Update consensus data for the sample key
        consensus_data_processed[sample_key] = {
            "sequence_name": ", ".join(sequence_names),
            "genome_length": genome_length,
            "sequence_filepath": os.path.dirname(consensus_file),
            "sequence_filename": sample_key,
            "sequence_md5": relecov_tools.utils.calculate_md5(consensus_file),
        }
    # Report missing consensus
    conserrs = len(missing_consens)
    if conserrs >= 1:
        method_log_report.update_log_report(
            method_name,
            "warning",
            f"{conserrs} samples missing in consensus file: {missing_consens}",
        )
        method_log_report.print_log_report(method_name, ["valid", "warning"])
    return consensus_data_processed


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
