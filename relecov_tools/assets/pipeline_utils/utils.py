#!/usr/bin/env python
"""
Common utility function used for relecov_tools package.
"""
import json
import logging
import os
import re
import sys
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import rich
import yaml
from Bio import SeqIO  # type: ignore

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.read_bioinfo_metadata import BioinfoReportLog

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

    def __init__(
        self,
        file_path: str | None = None,
        pipeline_name: str | None = None,
        output_folder: str | None = None,
        metadata: list[dict] | None = None,
    ):
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
            raise FileNotFoundError(
                f"Variant long table file {self.file_path} does not exist"
            )

        if not self.file_path.endswith(".csv"):
            log.error("Variant long table file %s is not a csv file ", self.file_path)
            stderr.print(
                f"[red] Variant long table file {self.file_path} must be a csv file"
            )
            raise ValueError(
                f"Variant long table file {self.file_path} must be a csv file"
            )

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

        self.metadata = metadata or []
        self.seq_to_unique: dict[str, str] = {}
        self.unique_to_seq: dict[str, str] = {}
        self._raw_to_unique: dict[str, str] = {}
        self._metadata_missing_unique: set[str] = set()
        self._warned_missing_in_metadata: set[str] = set()
        self._warned_inferred_from_csv: set[str] = set()

        json_file = os.path.join(
            os.path.dirname(__file__), "..", "..", "conf", "bioinfo_config.json"
        )

        if not pipeline_name:
            pipeline_name = relecov_tools.utils.prompt_text(
                msg="Enter the pipeline name for long table parsing"
            )

        config_json = ConfigJson(json_file)
        if pipeline_name:
            self.software_config = config_json.get_configuration(pipeline_name)
        else:
            log.error("Pipeline name is required for long table parsing")
            stderr.print("[red]Pipeline name is required for long table parsing")
            raise ValueError("Pipeline name is required for long table parsing")

        if self.software_config:
            self.long_table_heading = self.software_config["variants_long_table"][
                "content"
            ]
            self._initialize_sample_lookup()
        else:
            log.error("No configuration found for pipeline %s", pipeline_name)
            stderr.print(f"[red]No configuration found for pipeline {pipeline_name}")
            raise ValueError(f"No configuration found for pipeline {pipeline_name}")

    @staticmethod
    def _clean_identifier(value: str | int | None) -> str:
        """Normalize identifiers into clean strings."""
        if value is None:
            return ""
        return str(value).strip()

    def _register_mapping(self, seq_id: str, unique_id: str, context: str) -> None:
        """Register validated mapping between sequencing and unique identifiers."""
        existing_unique = self.seq_to_unique.get(seq_id)
        if existing_unique and existing_unique != unique_id:
            msg = (
                f"Conflicting unique_sample_id for sequencing_sample_id '{seq_id}': "
                f"'{existing_unique}' vs '{unique_id}' (source: {context})."
            )
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        existing_seq = self.unique_to_seq.get(unique_id)
        if existing_seq and existing_seq != seq_id:
            msg = (
                f"unique_sample_id '{unique_id}' is already linked to sequencing_sample_id "
                f"'{existing_seq}' (source: {context})."
            )
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        self.seq_to_unique[seq_id] = unique_id
        self.unique_to_seq[unique_id] = seq_id

    def _initialize_sample_lookup(self) -> None:
        """Build initial mapping between sequencing_sample_id and unique_sample_id."""
        for entry in self.metadata:
            seq_id = self._clean_identifier(entry.get("sequencing_sample_id"))
            unique_id = self._clean_identifier(entry.get("unique_sample_id"))
            if not seq_id:
                continue
            if not unique_id:
                self._metadata_missing_unique.add(seq_id)
                continue
            self._register_mapping(seq_id, unique_id, context="metadata")

    def _normalize_sample_name(self, raw_sample: str) -> str:
        """Validate and convert raw sample identifier into unique_sample_id."""
        if raw_sample is None:
            msg = "Found empty sample value in variants_long_table."
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        raw_sample = raw_sample.strip()
        if not raw_sample:
            msg = "Found empty sample value in variants_long_table."
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        cached = self._raw_to_unique.get(raw_sample)
        if cached:
            return cached

        if "_" in raw_sample:
            seq_id, candidate_unique = raw_sample.split("_", 1)
        else:
            seq_id, candidate_unique = raw_sample, ""

        seq_id = self._clean_identifier(seq_id)
        candidate_unique = self._clean_identifier(candidate_unique)

        # Handle case where sample name already equals a known unique_sample_id
        if not candidate_unique:
            seq_from_unique = self.unique_to_seq.get(seq_id)
            if seq_from_unique:
                unique_id = seq_id
                self._register_mapping(
                    seq_from_unique, unique_id, context="variants_long_table"
                )
                self._raw_to_unique[raw_sample] = unique_id
                return unique_id

        if not seq_id:
            msg = (
                f"Unable to determine sequencing_sample_id from variants_long_table sample "
                f"value '{raw_sample}'."
            )
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        metadata_unique = self.seq_to_unique.get(seq_id)

        if metadata_unique and candidate_unique and metadata_unique != candidate_unique:
            msg = (
                f"Mismatch between metadata unique_sample_id '{metadata_unique}' and "
                f"variants_long_table sample '{candidate_unique}' for sequencing_sample_id "
                f"'{seq_id}'."
            )
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        unique_id = metadata_unique or candidate_unique

        if not unique_id:
            msg = (
                f"No unique_sample_id found for sequencing_sample_id '{seq_id}'. "
                "Ensure it is present in the bioinfo metadata or encoded in the variants_long_table."
            )
            stderr.print(f"[red]{msg}")
            log.error(msg)
            raise ValueError(msg)

        context = "metadata"
        if not metadata_unique:
            context = "variants_long_table"
            if (
                seq_id in self._metadata_missing_unique
                and seq_id not in self._warned_missing_in_metadata
            ):
                stderr.print(
                    f"[yellow]unique_sample_id for sequencing_sample_id '{seq_id}' missing in metadata. "
                    "Using value from variants_long_table."
                )
                log.warning(
                    "unique_sample_id for sequencing_sample_id '%s' missing in metadata. "
                    "Using value from variants_long_table.",
                    seq_id,
                )
                self._warned_missing_in_metadata.add(seq_id)
            elif (
                seq_id not in self._metadata_missing_unique
                and seq_id not in self._warned_inferred_from_csv
            ):
                stderr.print(
                    f"[yellow]Sequencing_sample_id '{seq_id}' not present in metadata. "
                    "Using mapping from variants_long_table."
                )
                log.warning(
                    "Sequencing_sample_id '%s' not present in metadata. Using mapping from variants_long_table.",
                    seq_id,
                )
                self._warned_inferred_from_csv.add(seq_id)

        self._register_mapping(seq_id, unique_id, context=context)
        self._raw_to_unique[raw_sample] = unique_id
        return unique_id

    def validate_file(self, heading: list[str]) -> bool:
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

        headings_from_csv = lines[0].strip().split(",")
        heading_index = {
            heading: headings_from_csv.index(heading)
            for heading in self.long_table_heading.values()
        }
        stderr.print("[green]\tSuccessful checking heading fields")
        log.info("Successful checking heading fields")

        samp_dict = {}
        for line in lines[1:]:
            line_s = line.strip().split(",")

            raw_sample = line_s[heading_index["SAMPLE"]]
            unique_sample = self._normalize_sample_name(raw_sample)
            if unique_sample not in samp_dict:
                samp_dict[unique_sample] = []

            variant_dict = {
                key: (
                    {key2: line_s[heading_index[val2]] for key2, val2 in value.items()}
                    if isinstance(value, dict)
                    else line_s[heading_index[value]]
                )
                for key, value in self.long_table_heading.items()
            }
            variant_dict["sample"] = unique_sample

            if re.search("&", line_s[heading_index["GENE"]]):
                # Example
                # 215184,NC_045512.2,27886,AAACGAACATGAAATT,A,PASS,1789,1756,1552,0.87,ORF7b&ORF8,gene_fusion,n.27887_27901delAACGAACATGAAATT,.,.,ivar,B.1.1.318
                # This only occurs (for now) as gene fusion, so we just duplicate lines with same values
                genes = re.split("&", line_s[heading_index["GENE"]])
                for gene in genes:
                    variant_dict_copy = variant_dict.copy()
                    variant_dict_copy["Gene"] = gene
                    samp_dict[unique_sample].append(variant_dict_copy)
            else:
                samp_dict[unique_sample].append(variant_dict)
        stderr.print("[green]\tSuccessful parsing data")
        log.info("Successful parsing long table data")
        return samp_dict

    def convert_to_json(self, samp_dict):
        j_list = []
        # Grab date from filename
        result_regex = re.search(
            r"variants_long_table(?:_\d{14})?\.csv", os.path.basename(self.file_path)
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
            if analysis_date:
                if len(analysis_date) == 8:
                    analysis_date = datetime.strptime(analysis_date, "%Y%m%d").strftime(
                        "%Y-%m-%d"
                    )
                else:
                    log.error(
                        "Analysis date could not be parsed from file name %s",
                        self.file_path,
                    )

        for key, values in samp_dict.items():
            j_dict = {
                "sample_name": key,
                "bioinformatics_analysis_date": analysis_date,
                "file_name": "long_table",
                "variants": values,
            }
            j_list.append(j_dict)
        return j_list

    def save_to_file(self, j_list, file_tag):
        """Transform the parsed data into a json file"""
        file_name = f"long_table_{file_tag}.json"
        file_path = os.path.join(self.output_folder, file_name)
        if os.path.exists(file_path):
            stderr.print(
                f"[blue]Long table {file_path} file already exists. Merging new data if possible."
            )
            log.info(
                f"Long table {file_path} file already exists. Merging new data if possible."
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
                            f"Sample {sample_name} has different data in {file_path} and new long table. Can't merge."
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
        return self.convert_to_json(parsed_data)


# END of Class


def parse_long_table(
    files_list: list,
    file_tag: str,
    pipeline_name: str,
    output_folder: str | None = None,
    metadata: list[dict] | None = None,
) -> None | list[dict]:
    """File handler to retrieve data from long table files and convert it into a JSON structured format.
    This function utilizes the LongTableParse class to parse the long table data.
    Since this utility handles and maps data using a custom way, it returns None to be avoid being  transferred to method read_bioinfo_metadata.BioinfoMetadata.mapping_over_table().

    Args:
        files_list (list): A list of paths to long table files.
        file_tag (str): A tag to be used in the output file name.
        pipeline_name (str): The name of the pipeline for which the long table is being parsed.
        output_folder (str | None): The folder where the output file will be saved.

    Returns:
        None: Indicates that the function does not return any meaningful value.
    """
    method_name = f"{parse_long_table.__name__}"
    method_log_report = BioinfoReportLog()

    # Handling long table data
    if len(files_list) == 1:
        csv_path = files_list[0]
        if not os.path.isfile(csv_path):
            method_log_report.update_log_report(
                method_name, "error", f"{csv_path} is not a valid file"
            )
            sys.exit(method_log_report.print_log_report(method_name, ["error"]))

        parser = LongTableParse(
            file_path=csv_path,
            pipeline_name=pipeline_name,
            output_folder=output_folder,
            metadata=metadata,
        )
        # Parsing long table data and saving it
        long_table_json = parser.parsing_csv()

        # Saving long table data into a file
        parser.save_to_file(long_table_json, file_tag)

        stderr.print("[green]\tProcess completed")
        log.info("Long table process completed")

        return long_table_json
    elif len(files_list) > 1:
        method_log_report.update_log_report(
            method_name,
            "warning",
            f"Found {len(files_list)} variants_long_table files. This version is unable to process more than one variants long table each time.",
        )
        return None
    else:
        method_log_report.update_log_report(
            method_name, "error", "No valid variants_long_table file found."
        )
        return None


def extract_consensus_stats(files_list: list, **kwargs) -> dict:
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


def get_software_versions_yml(files_list: list, **kwargs) -> dict:
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
    data: list[dict],
    thresholds: dict,
    conditions: dict,
    invert_operator: Callable,
    is_not_evaluable: Callable,
) -> tuple[list[dict], list[str]]:
    """
    Perform QC evaluation on a list of sample data dictionaries using provided threshold logic.

    Args:
    data : list of dict
        Each dict represents a sample and contains metrics to be evaluated.
    thresholds : dict
        A dictionary where keys are metric names and values are tuples (operator, threshold).
    conditions : dict
        A dictionary where keys are metric names and values are condition functions.
    invert_operator : function
        Function that returns the inverse of a comparison operator.
    is_not_evaluable : function
        Function to detect if a metric value should be skipped

    Returns:
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
                value = (
                    float(sample.get(param))
                    if isinstance(sample.get(param), str)
                    else sample.get(param)
                )
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
