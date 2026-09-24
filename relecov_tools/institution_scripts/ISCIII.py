#!/usr/bin/env python
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
                continue
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
    return metadata


def translate_gender_to_english(metadata, f_data, mapped_fields, heading):
    """Translate into english the host gender that is written in spanish"""
    map_dict = {
        "hombre": "Male",
        "mujer": "Female",
        "genero no-binario": "Non-binary Gender",
        "genero no-binario": "Non-binary Gender",
        "desconocido": "Not Provided",
        "unknown": "Not Provided",
    }
    for row in metadata[1:]:
        for key, _ in mapped_fields.items():
            m_idx = heading.index(key)
            if row[m_idx] is None or row[m_idx] == "":
                row[m_idx] = "Not Provided"
                continue
            item = str(row[m_idx]).lower()
            if item in map_dict:
                row[m_idx] = map_dict[item]
            else:
                log.error("The %s is not a valid data for translation", row[m_idx])
                stderr.print(
                    f"[red] The '{row[m_idx]}' is not a valid data for translation"
                )
    return metadata


def translate_specimen_source(metadata, f_data, mapped_fields, heading):
    """Translate into english the "muestra" that is written in spanish"""
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            m_idx = heading.index(key)
            if row[m_idx] is None:
                row[m_idx] = "Not Provided"
            elif "ASPIRADO NASOFARÍNGEO" in row[m_idx].upper():
                row[m_idx] = "Nasopharynx Aspiration"
            elif "ASPIRADO BRONQUIAL" in row[m_idx].upper():
                row[m_idx] = "Bronchus Aspiration"
            elif "ESPUTO" in row[m_idx].upper():
                row[m_idx] = "Sputum"
            elif "EXTRACTO" in row[m_idx].upper():
                row[m_idx] = "Scraping"
            elif "EXUDADO FARÍNGEO" in row[m_idx].upper():
                row[m_idx] = "Pharynx Swab"
            elif "EXUDADO NASOFARÍNGEO" in row[m_idx].upper():
                row[m_idx] = "Nasopharynx swab"
            elif "EXUDADO OROFARINGEO" in row[m_idx].upper():
                row[m_idx] = "Oropharynx Swab"
            elif "PLACENTA" in row[m_idx].upper():
                row[m_idx] = "Placenta"
            elif "SALIVA" in row[m_idx].upper():
                row[m_idx] = "Saliva"
            else:
                log.error("The field is not correctly written or is not filled")
                stderr.print("The field is not correctly written or not filled")
    return metadata


def translate_purpose_seq_to_english(metadata, f_data, mapped_fields, heading):
    """Fetch the first words of the option to group them according the
    schema
    """
    map_dict = {
        "estudio variante": "Targeted surveillance (non-random sampling)",
        "trabajador/a granja visones": "Targeted surveillance (non-random sampling)",
        "sospecha reinfección": "Re-infection surveillance",
        "i-move-covid": "Research",
        "irag": "Research",
        "muestreo aleatorio": "Baseline surveillance (random sampling)",
        "paciente vacunado": "Vaccine escape surveillance",
        "posible variante": "Sample has epidemiological link to Variant of Concern (VoC)",
        "no consta": "Not Collected",
        "brote": "Cluster/Outbreak investigation",
        "viaje": "Surveillance of international border crossing by air travel or ground transport",
        "posible variante": "Sample has epidemiological link to Variant of Concern (VoC)",
    }
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            m_idx = heading.index(key)
            if row[m_idx] is None or row[m_idx] == "":
                row[m_idx] = "Not Provided"
                continue
            item = row[m_idx].lower()
            if item in map_dict:
                row[m_idx] = map_dict[item]
            elif "brote" in item:
                row[m_idx] = map_dict["brote"]
            elif "viaje" in item:
                row[m_idx] = map_dict["viaje"]
            elif "posible variante" in item:
                row[m_idx] = map_dict["posible variante"]
            else:
                log.error("The '%s' is not a valid data for translation", row[m_idx])
                stderr.print(
                    "f[red] The {row[m_idx]} is not a valid data for translation"
                )
    return metadata


def translate_nucleic_acid_extract_prot(metadata, f_data, mapped_fields, heading):
    """Fetch the short name given in the input laboratory file and change for
    the one is allow according to schema
    """
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            m_idx = heading.index(key)
            if "NA" in row[m_idx]:
                row[m_idx] = "Not Applicable"
            elif "opentrons" in row[m_idx].lower():
                row[m_idx] = "Opentrons custom rna extraction protocol"
            else:
                # allow from now on until more options are available
                continue
    return metadata


def findout_library_layout(metadata, f_data, mapped_fields, heading):
    """Read the file and by checking if read2_cycles is 0 set to Single otherwise
    to paired"""
    s_idx = heading.index("Sample ID given for sequencing")
    for row in metadata[1:]:
        for key, val in mapped_fields.items():
            m_idx = heading.index(key)
            try:
                if f_data[str(row[s_idx])][val] == "0":
                    row[m_idx] = "Single"
                else:
                    row[m_idx] = "Paired"
            except KeyError as e:
                log.error("The %s is not defined in function findout_library_layout", e)
                stderr.print(
                    f"[red] {e} is not defined in function findout_library_layout"
                )
    return metadata
