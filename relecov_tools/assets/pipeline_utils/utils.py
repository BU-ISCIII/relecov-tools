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
from Bio import SeqIO

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


# INIT Class
class LongTableParse:
    """
    - parse_a_list_of_dictionaries() : returns generated_JSON
    - saving_file(generated_JSON)
    - parsing_csv() : It manages all this proccess:
        - calling first to parse_a_list_of_dictionaries() and then calling to saving_file()
    """

    def __init__(self, file_path=None, pipeline_name=None, output_folder=None):
        if file_path is None:
            self.file_path = relecov_tools.utils.prompt_path(
                msg="Select the csv file which contains variant long table information"
            )
        else:
            self.file_path = file_path

        if not os.path.exists(self.file_path):
            log.error("Variant long table file %s does not exist ", self.file_path)
            stderr.print(
                f"[red] Variant long table file {self.file_path} does not exist"
            )
            sys.exit(1)

        if not self.file_path.endswith(".csv"):
            log.error("Variant long table file %s is not a csv file ", self.file_path)
            stderr.print(
                f"[red] Variant long table file {self.file_path} must be a csv file"
            )
            sys.exit(1)

        if output_folder is None:
            use_default = relecov_tools.utils.prompt_yn_question("Use default path?: ")
            if use_default:
                self.output_folder = os.getcwd()
            else:
                self.output_folder = relecov_tools.utils.prompt_path(
                    msg="Select the output folder:"
                )
        else:
            self.output_folder = output_folder
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)

        json_file = os.path.join(
            os.path.dirname(__file__), "..", "..", "conf", "bioinfo_config.json"
        )
        config_json = ConfigJson(json_file)
        self.software_config = config_json.get_configuration(pipeline_name)
        self.long_table_heading = self.software_config["variants_long_table"]["content"]

    def validate_file(self, heading):
        """Check if long table file has all mandatory fields defined in
        configuration file
        """
        for field in self.long_table_heading:
            if field not in heading:
                log.error("Incorrect format file. %s is missing", field)
                stderr.print(f"[red]Incorrect Format. {field} is missing in file")
                sys.exit(1)
        return True

    def parse_file(self):
        """This function generates a json file from the csv file entered by
        the user (long_table.csv).
        Validate the file by checking the header line
        """
        with open(self.file_path, encoding="utf-8-sig") as fh:
            lines = fh.readlines()

        heading_index = {}
        headings_from_csv = lines[0].strip().split(",")
        for heading in self.long_table_heading.values():
            heading_index[heading] = headings_from_csv.index(heading)

        stderr.print("[green]\tSuccessful checking heading fields")
        log.info("Successful checking heading fields")

        samp_dict = {}
        for line in lines[1:]:
            line_s = line.strip().split(",")

            sample = line_s[heading_index["SAMPLE"]]
            if sample not in samp_dict:
                samp_dict[sample] = []

            variant_dict = {
                key: (
                    {key2: line_s[heading_index[val2]] for key2, val2 in value.items()}
                    if isinstance(value, dict)
                    else line_s[heading_index[value]]
                )
                for key, value in self.long_table_heading.items()
            }

            if re.search("&", line_s[heading_index["GENE"]]):
                # Example
                # 215184,NC_045512.2,27886,AAACGAACATGAAATT,A,PASS,1789,1756,1552,0.87,ORF7b&ORF8,gene_fusion,n.27887_27901delAACGAACATGAAATT,.,.,ivar,B.1.1.318
                # This only occurs (for now) as gene fusion, so we just duplicate lines with same values
                genes = re.split("&", line_s[heading_index["GENE"]])
                for gene in genes:
                    variant_dict_copy = variant_dict.copy()
                    variant_dict_copy["Gene"] = gene
                    samp_dict[sample].append(variant_dict_copy)
            else:
                samp_dict[sample].append(variant_dict)
        stderr.print("[green]\tSuccessful parsing data")
        log.info("Successful parsing long table data")
        return samp_dict

    def convert_to_json(self, samp_dict):
        j_list = []
        # Grab date from filename
        result_regex = re.search(
            "variants_long_table(?:_\d{14})?\.csv", os.path.basename(self.file_path)
        )
        if result_regex is None:
            stderr.print(
                "[red]\tWARN: Couldn't find variants long table file. Expected file name is:"
            )
            stderr.print(
                "[red]\t\t- variants_long_table.csv or variants_long_table_YYYYMMDD.csv. Aborting..."
            )
            sys.exit(1)
        else:
            analysis_date = relecov_tools.utils.get_file_date(self.file_path)
            if len(analysis_date) == 8:
                analysis_date = datetime.strptime(analysis_date, "%Y%m%d").strftime(
                    "%Y-%m-%d"
                )
        for key, values in samp_dict.items():
            j_dict = {"sample_name": key, "bioinformatics_analysis_date": analysis_date}
            j_dict["variants"] = values
            j_list.append(j_dict)
        return j_list

    def save_to_file(self, j_list, file_tag):
        """Transform the parsed data into a json file"""
        file_name = "long_table_" + file_tag + ".json"
        file_path = os.path.join(self.output_folder, file_name)
        if os.path.exists(file_path):
            stderr.print(
                f"[blue]Long table {file_path} file already exists. Merging new data if possible."
            )
            log.info(
                "Long table %s file already exists. Merging new data if possible."
                % file_path
            )
            original_table = relecov_tools.utils.read_json_file(file_path)
            samples_indict = {item["sample_name"]: item for item in original_table}
            for item in j_list:
                sample_name = item["sample_name"]
                if sample_name in samples_indict:
                    if samples_indict[sample_name] != item:
                        stderr.print(
                            f"[red]Same sample {sample_name} has different data in both long tables."
                        )
                        log.error(
                            "Sample %s has different data in %s and new long table. Can't merge."
                            % (sample_name, file_path)
                        )
                        return None
                else:
                    original_table.append(item)
            try:
                with open(file_path, "w") as fh:
                    fh.write(json.dumps(original_table, indent=4))
                stderr.print(
                    "[green]\tParsed data successfully saved to file:", file_path
                )
                log.info("Parsed data successfully saved to file: %s", file_path)
            except Exception as e:
                stderr.print("[red]\tError saving parsed data to file:", str(e))
                log.error("Error saving parsed data to file: %s", e)
        else:
            try:
                with open(file_path, "w") as fh:
                    fh.write(json.dumps(j_list, indent=4))
                stderr.print(
                    "[green]\tParsed data successfully saved to file:", file_path
                )
                log.info("Parsed data successfully saved to file: %s", file_path)
            except Exception as e:
                stderr.print("[red]\tError saving parsed data to file:", str(e))
                log.error("Error saving parsed data to file: %s", e)

    def parsing_csv(self):
        """
        Function called when using the relecov-tools long-table-parse function.
        """
        # Parsing longtable file
        parsed_data = self.parse_file()
        j_list = self.convert_to_json(parsed_data)
        return j_list


# END of Class


def parse_long_table(files_list, file_tag, pipeline_name, output_folder=None):
    """File handler to retrieve data from long table files and convert it into a JSON structured format.
    This function utilizes the LongTableParse class to parse the long table data.
    Since this utility handles and maps data using a custom way, it returns None to be avoid being  transferred to method read_bioinfo_metadata.BioinfoMetadata.mapping_over_table().

    Args:
        files_list (list): A list of paths to long table files.

    Returns:
        None: Indicates that the function does not return any meaningful value.
    """
    method_name = f"{parse_long_table.__name__}"
    method_log_report = BioinfoReportLog()

    # Handling long table data
    if len(files_list) == 1:
        files_list_processed = files_list[0]
        if not os.path.isfile(files_list_processed):
            method_log_report.update_log_report(
                method_name, "error", f"{files_list_processed} given file is not a file"
            )
            sys.exit(method_log_report.print_log_report(method_name, ["error"]))

        long_table = LongTableParse(
            file_path=files_list_processed,
            pipeline_name=pipeline_name,
            output_folder=output_folder,
        )
        # Parsing long table data and saving it
        long_table_data = long_table.parsing_csv()
        # Saving long table data into a file
        long_table.save_to_file(long_table_data, file_tag)
        stderr.print("[green]\tProcess completed")
        log.info("Long table process completed")
    elif len(files_list) > 1:
        method_log_report.update_log_report(
            method_name,
            "warning",
            f"Found {len(files_list)} variants_long_table files. This version is unable to process more than one variants long table each time.",
        )
    # This needs to return none to avoid being parsed by method mapping-over-table
    return None


def extract_consensus_stats(files_list, file_tag, pipeline_name, output_folder=None):
    """File handler to parse consensus data (fasta) into JSON structured format.

    Args:
        files_list (list): A list with paths to condensus files.

    Returns:
        consensus_data_processed: A dictionary containing consensus data handled.
    """
    method_name = f"{extract_consensus_stats.__name__}"
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
                software_version = (
                    version.strip() if isinstance(version, str) else str(version)
                )
                software_id = software_name.lower()
                if "/" not in software_id:
                    software_id = software_id.split()[0]
                version_dict[software_id] = {
                    "software_version": software_version,
                    "software_name": software_name,
                }

    return version_dict


def evaluate_qc_samples(
    data, thresholds, conditions, invert_operator, is_not_evaluable
):
    """
    Perform QC evaluation on a list of sample data dictionaries using provided threshold logic.

    Parameters:
    -----------
    data : list of dict
        Each dict represents a sample and contains metrics to be evaluated.
    thresholds : dict
        A dictionary where keys are metric names and values are tuples (operator, threshold).
    conditions : dict
        A dictionary where keys are metric names and values are condition functions.
    invert_operator : function
        Function that returns the inverse of a comparison operator.
    is_not_evaluable : function
        Function to detect if a metric value should be skipped.
    log_report : BioinfoReportLog
        Logger to record validation and warning messages.
    log : Logger
        Standard Python logger for logging events.
    method_name : str
        Name of the method for logging purposes.

    Returns:
    --------
    Tuple of:
    - data : List[Dict[str, Any]]
        The input list with added 'qc_test' and optional 'qc_failed' keys.
    - warning_messages : List[str]
        List of warning messages that occurred during evaluation.
    """
    warning_messages = []

    for sample in data:
        try:
            qc_status = "pass"
            failed_reasons = []

            for param, condition in conditions.items():
                value = sample.get(param)
                try:
                    if value is None or not condition(value):
                        if is_not_evaluable(value):
                            log.info(
                                "%s is not evaluable for %s in sample %s",
                                value,
                                param,
                                sample.get("sequencing_sample_id", "unknown"),
                            )
                            continue
                        qc_status = "fail"
                        op, th = thresholds[param]
                        inverted_op = invert_operator(op)
                        failed_reasons.append(f"({param} {inverted_op} {th})")
                except (TypeError, ValueError):
                    if is_not_evaluable(value):
                        continue
                    qc_status = "fail"
                    failed_reasons.append(f"({param} = {value} invalid)")
                    msg = (
                        f"Sample {sample.get('sequencing_sample_id', 'unknown')} "
                        f"has unevaluable value for {param}: {value}"
                    )
                    warning_messages.append(msg)

            sample["qc_test"] = qc_status
            if qc_status == "fail" and failed_reasons:
                sample["qc_failed"] = " -- ".join(failed_reasons)

            msg = (
                "valid",
                f"{sample.get('sequencing_sample_id', 'unknown')} evaluated: {qc_status}",
            )

        except (TypeError, ValueError, AttributeError) as e:
            sample["qc_test"] = "fail"
            sample["qc_failed"] = f"(evaluation_error = {str(e)})"
            sample_id = sample.get("sequencing_sample_id", "unknown")
            msg = f"Error evaluating sample {sample_id}: {e}"
            warning_messages.append(msg)

    return data, warning_messages
