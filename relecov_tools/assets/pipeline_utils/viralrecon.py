#!/usr/bin/env python
import json
import os
import sys
import re
import logging
import rich
import os.path

from pathlib import Path

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

    def __init__(self, file_path=None, output_directory=None):
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

        if output_directory is None:
            use_default = relecov_tools.utils.prompt_yn_question("Use default path?: ")
            if use_default:
                self.output_directory = os.getcwd()
            else:
                self.output_directory = relecov_tools.utils.prompt_path(
                    msg="Select the output folder:"
                )
        else:
            self.output_directory = output_directory
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        json_file = os.path.join(
            os.path.dirname(__file__), "..", "..", "conf", "bioinfo_config.json"
        )
        config_json = ConfigJson(json_file)
        self.software_config = config_json.get_configuration("viralrecon")
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

        stderr.print("[green]\tSuccessful checking heading fields")
        log.info("Successful checking heading fields")
        heading_index = {}
        headings_from_csv = lines[0].strip().split(",")
        for heading in self.long_table_heading.values():
            heading_index[heading] = headings_from_csv.index(heading)

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
        for key, values in samp_dict.items():
            j_dict = {"sample_name": key, "analysis_date": analysis_date}
            j_dict["variants"] = values
            j_list.append(j_dict)
        return j_list

    def save_to_file(self, j_list, file_tag):
        """Transform the parsed data into a json file"""
        file_name = "long_table_" + file_tag + ".json"
        file_path = os.path.join(self.output_directory, file_name)
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


# START util functions
def handle_pangolin_data(files_list, file_tag, output_folder=None):
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
                    key.split()[0]: value for key, value in pango_data.items()
                }
                pango_data_processed.update(pango_data_updated)
                valid_samples.append(pango_data_key.split()[0])
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
    if len(valid_samples) > 0:
        method_log_report.update_log_report(
            method_name,
            "valid",
            f"Successfully handled data in samples: {', '.join(valid_samples)}",
        )
    method_log_report.print_log_report(method_name, ["valid", "warning"])
    return pango_data_processed


def parse_long_table(files_list, file_tag, output_folder=None):
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
            file_path=files_list_processed, output_directory=output_folder
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


def handle_consensus_fasta(files_list, file_tag, output_folder=None):
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
        try:
            record_fasta = relecov_tools.utils.read_fasta_return_SeqIO_instance(
                consensus_file
            )
        except FileNotFoundError as e:
            missing_consens.append(e.filename)
            continue
        sample_key = os.path.basename(consensus_file).split(".")[0]

        # Update consensus data for the sample key
        consensus_data_processed[sample_key] = {
            "sequence_name": record_fasta.description,
            "genome_length": str(len(record_fasta)),
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
        "per_sgene_ambiguous": ("<", 10.0),
        "per_sgene_coverage": (">", 98.0),
        "per_ldmutations": (">", 60.0),  # except if 'Not Evaluable'
        "number_of_sgene_frameshifts": ("==", 0),
        "number_of_unambiguous_bases": (">", 24000),
        "number_of_Ns": ("<", 5000),
        "pass_reads": (">", 50000),
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
                                sample["sequencing_sample_id"],
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
