#!/usr/bin/env python
import os
import sys
import re
import logging

import relecov_tools.utils
from relecov_tools.read_bioinfo_metadata import BioinfoReportLog
from relecov_tools.read_bioinfo_metadata import BioinfoMetadata
from relecov_tools.long_table_parse import LongTableParse

log = logging.getLogger(__name__)

def handle_pangolin_data(files_list):
    """File handler to parse pangolin data (csv) into JSON structured format.
    """
    method_name = f"{handle_pangolin_data.__name__}"
    method_log_report = BioinfoReportLog()

    # Handling pangolin data
    pango_data_processed = {}
    try:
        files_list_processed = relecov_tools.utils.select_most_recent_files_per_sample(files_list)
        for pango_file in files_list_processed:
            try:
                pango_data = relecov_tools.utils.read_csv_file_return_dict(
                    pango_file, sep=","
                )
                # Add custom content in pangolin
                pango_data_key = next(iter(pango_data))
                pango_data[pango_data_key]['lineage_analysis_date'] = relecov_tools.utils.get_file_date(
                    pango_file
                )

                # Rename key in f_data
                pango_data_updated = {key.split()[0]: value for key, value in pango_data.items()}
                pango_data_processed.update(pango_data_updated)
                method_log_report.update_log_report(
                    method_name,
                    'valid', 
                    f"Successfully handled data in {pango_file}."
                )
            except (FileNotFoundError, IndexError) as e:
                method_log_report.update_log_report(
                    method_name,
                    'error', 
                    f"Error processing file {pango_file}: {e}"
                )
                sys.exit(
                    method_log_report.print_log_report(method_name, ['error'])
                )
    except Exception as e:
        method_log_report.update_log_report(
            method_name,
            'error',
            f"Error occurred while processing files: {e}"
        )
        sys.exit(
            method_log_report.print_log_report(method_name, ['error'])
        )
    return pango_data_processed

def parse_long_table(files_list):
    method_name = f"{parse_long_table.__name__}"
    method_log_report = BioinfoReportLog()

    # Hanfling long table data
    if len(files_list) == 1:
        files_list_processed = files_list[0]
        if not os.path.isfile(files_list_processed):
            method_log_report.update_log_report(
                method_name,
                'error',
                f"{files_list_processed} given file is not a file"
            )
            sys.exit(method_log_report.print_log_report(method_name,["error"]))
        long_table = LongTableParse(files_list_processed)
        # Parsing long table data and saving it
        long_table.parsing_csv()
        # FIXME: cannot write over j_data when this function is invoked from a differnt file.
        # Adding custom long_table data to j_data
        # self.j_data = long_table.add_custom_longtable_data(self.j_data)
    elif len(files_list) > 1:
        method_log_report.update_log_report(
            method_name, 
            'warning',
            f"Found {len(files_list)} variants_long_table files. This version is unable to process more than one variants long table each time."
        )
    # This needs to return none to avoid being parsed by method mapping-over-table  
    return None

def handle_consensus_fasta(files_list):
    """File handler to parse consensus fasta data (*.consensus.fa) into JSON structured format"""
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
        sample_key = os.path.splitext(os.path.basename(consensus_file))[0]

        # Update consensus data for the sample key
        consensus_data_processed[sample_key] = {
            'sequence_name': record_fasta.description,
            'genome_length': str(len(record_fasta)),
            'sequence_filepath': os.path.dirname(consensus_file),
            'sequence_filename': sample_key,
            'sequence_md5': relecov_tools.utils.calculate_md5(consensus_file),
            # TODO: Not sure this is correct. If not, recover previous version: https://github.com/BU-ISCIII/relecov-tools/blob/09c00c1ddd11f7489de7757841aff506ef4b7e1d/relecov_tools/read_bioinfo_metadata.py#L211-L218
            'number_of_base_pairs_sequenced': len(record_fasta.seq)
        }         

    # Report missing consensus
    conserrs = len(missing_consens)
    if conserrs >= 1:
        method_log_report.update_log_report(
            method_name,
            'warning', 
            f"{conserrs} samples missing in consensus file: {missing_consens}"
        )
    method_log_report.print_log_report(method_name, ['valid','warning'])
    return consensus_data_processed
