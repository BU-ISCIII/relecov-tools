#!/usr/bin/env python
import sys

import logging
import rich.console
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


def replace_originating_lab(metadata, f_data, mapped_fields, heading):
    """Replace the format text in the originating lab and replace by the ones
    defined in the System
    """
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            meta_idx = heading.index(key)
            try:
                row[meta_idx] = f_data[row[meta_idx].strip()][val].strip()
            except KeyError as e:
                log.error("Value  %s does not exist ", e)
                stderr.print(f"[red] Value {e} does not exist")
                sys.exit(1)
    return metadata


def added_seq_inst_model(metadata, f_data, mapped_fields, heading):
    """Set the type of sequencer instrument based on the run name"""
    s_idx = heading.index("Sample ID given for sequencing")
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            m_idx = heading.index(key)

            try:
                run_name = f_data[str(row[s_idx])][val].lower()
            except KeyError as e:
                log.error("Value  %s does not exist ", e)
                stderr.print(f"[red] Value {e} does not exist")
                sys.exit(1)
            if "nextseq" in run_name:
                row[m_idx] = "Illumina NextSeq 500"
            elif "next_seq" in run_name:
                row[m_idx] = "Illumina NextSeq 500"
            elif "miseq" in run_name:
                row[m_idx] = "Illumina MiSeq"
            elif "miseaq" in run_name:
                row[m_idx] = "Illumina MiSeq"
            elif "novaseq" in run_name:
                row[m_idx] = "Illumina NovaSeq 6000"
            else:
                log.error("Value  %s is not defined in the mapping ", run_name)
                stderr.print(f"[red] Value {run_name} is not defined in the mapping")
                sys.exit(1)
    return metadata


def translate_gender_to_english(metadata, f_data, mapped_fields, heading):
    """Translate into english the host gender that is written in spanish"""
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            m_idx = heading.index(key)
            if row[m_idx] is None:
                row[m_idx] = "not provided"
            elif "hombre" in row[m_idx].lower():
                row[m_idx] = "Male"
            elif "mujer" in row[m_idx].lower():
                row[m_idx] = "Female"
            elif "desconocido" in row[m_idx].lower():
                row[m_idx] = "not provided"
            elif "Unknown" in row[m_idx].lower():
                row[m_idx] = "not provided"
            elif "unknown" in row[m_idx].lower():
                row[m_idx] = "not provided"
            else:
                row[m_idx] = "not applicable"
    return metadata
