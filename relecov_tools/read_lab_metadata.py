#!/usr/bin/env python
import json
import logging
import rich.console
import os
import sys
import re
from datetime import datetime as dtime
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
import relecov_tools.json_schema
from relecov_tools.log_summary import LogSum

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class RelecovMetadata:
    def __init__(self, metadata_file=None, sample_list_file=None, output_folder=None):
        if metadata_file is None:
            self.metadata_file = relecov_tools.utils.prompt_path(
                msg="Select the excel file which contains metadata"
            )
        else:
            self.metadata_file = metadata_file

        if not os.path.exists(self.metadata_file):
            log.error("Metadata file %s does not exist ", self.metadata_file)
            stderr.print(
                "[red] Metadata file " + self.metadata_file + " does not exist"
            )
            sys.exit(1)

        if sample_list_file is None:
            stderr.print("[yellow]No samples_data.json file provided")
        self.sample_list_file = sample_list_file

        if sample_list_file is not None and not os.path.exists(sample_list_file):
            log.error("Sample information file %s does not exist ", sample_list_file)
            stderr.print("[red] Samples file " + sample_list_file + " does not exist")
            sys.exit(1)

        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = output_folder
        out_path = os.path.realpath(self.output_folder)
        self.lab_code = out_path.split("/")[-2]
        self.logsum = LogSum(
            output_location=self.output_folder, unique_key=self.lab_code, path=out_path
        )
        config_json = ConfigJson()
        # TODO: remove hardcoded schema selection
        relecov_schema = config_json.get_topic_data("json_schemas", "relecov_schema")
        relecov_sch_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", relecov_schema
        )
        self.configuration = config_json

        with open(relecov_sch_path, "r") as fh:
            self.relecov_sch_json = json.load(fh)
        self.label_prop_dict = {}

        for prop, values in self.relecov_sch_json["properties"].items():
            try:
                self.label_prop_dict[values["label"]] = prop
            except KeyError:
                log.warning("Property %s does not have 'label' attribute", prop)
                stderr.print(
                    "[orange]Property " + prop + " does not have 'label' attribute"
                )
                continue
        self.date = dtime.now().strftime("%Y%m%d%H%M%S")
        self.json_req_files = config_json.get_topic_data(
            "lab_metadata", "lab_metadata_req_json"
        )
        self.schema_name = self.relecov_sch_json["title"]
        self.schema_version = self.relecov_sch_json["version"]
        self.metadata_processing = config_json.get_topic_data(
            "sftp_handle", "metadata_processing"
        )
        self.samples_json_fields = config_json.get_topic_data(
            "lab_metadata", "samples_json_fields"
        )

    def get_samples_files_data(self, clean_metadata_rows):
        """Include the fields that would be included in samples_data.json

        Args:
            clean_metadata_rows (list(dict)): Cleaned list of rows from metadata_lab.xlsx file

        Returns:
            j_data (dict(dict)): Dictionary where each key is the sample ID and the values are
            its file names, locations and md5
        """

        def safely_calculate_md5(file):
            """Check file md5, but return Not Provided if file does not exist"""
            try:
                return relecov_tools.utils.calculate_md5(file)
            except IOError:
                return "Not Provided [GENEPIO:0001668]"

        dir_path = os.path.dirname(os.path.realpath(self.metadata_file))
        md5_checksum_files = [f for f in os.listdir(dir_path) if "md5" in f]
        if md5_checksum_files:
            skip_list = self.configuration.get_topic_data(
                "sftp_handle", "skip_when_found"
            )
            md5_dict = relecov_tools.utils.read_md5_checksum(
                file_name=md5_checksum_files[0], avoid_chars=skip_list
            )
        else:
            md5_dict = {}
            log.warning("No md5sum file found.")
            log.warning("Generating new md5 hashes. This might take a while...")
        j_data = {}
        no_fastq_error = "No R1 fastq was given for sample %s"
        for sample in clean_metadata_rows:
            files_dict = {}
            r1_file = sample.get("sequence_file_R1_fastq")
            r2_file = sample.get("sequence_file_R2_fastq")
            if not r1_file:
                self.logsum.add_error(
                    sample=sample.get("sequencing_sample_id"),
                    entry=no_fastq_error % sample.get("sequencing_sample_id"),
                )
                j_data[str(sample.get("sequencing_sample_id"))] = files_dict
                continue
            r1_md5 = md5_dict.get(r1_file)
            r2_md5 = md5_dict.get(r2_file)
            files_dict["sequence_file_R1_fastq"] = r1_file
            files_dict["r1_fastq_filepath"] = dir_path
            if r1_md5:
                files_dict["fastq_r1_md5"] = r1_md5
            else:
                files_dict["fastq_r1_md5"] = safely_calculate_md5(
                    os.path.join(dir_path, r1_file)
                )
            if r2_file:
                files_dict["sequence_file_R2_fastq"] = r2_file
                files_dict["r2_fastq_filepath"] = dir_path
                if r2_md5:
                    files_dict["fastq_r2_md5"] = r2_md5
                else:
                    files_dict["fastq_r2_md5"] = safely_calculate_md5(
                        os.path.join(dir_path, r2_file)
                    )
            j_data[str(sample.get("sequencing_sample_id"))] = files_dict
        try:
            filename = "_".join(["samples_data", self.lab_code, self.date + ".json"])
            file_path = os.path.join(self.output_folder, filename)
            relecov_tools.utils.write_json_fo_file(j_data, file_path)
        except Exception:
            log.error("Could not output samples_data.json file to output folder")
        return j_data

    def match_to_json(self, valid_metadata_rows):
        """Keep only the rows from samples present in the input file samples.json

        Args:
            valid_metadata_rows (list(dict)): List of rows from metadata_lab.xlsx file

        Returns:
            clean_metadata_rows(list(dict)): List of rows matching the samples in samples_data.json
            missing_samples(list(str)): List of samples not found in samples_data.json
        """
        missing_samples = []
        if not self.sample_list_file:
            logtxt = "samples_data.json not provided, all samples will be included"
            self.logsum.add_warning(entry=logtxt)
            return valid_metadata_rows, missing_samples
        else:
            samples_json = relecov_tools.utils.read_json_file(self.sample_list_file)
        clean_metadata_rows = []
        for row in valid_metadata_rows:
            sample_id = str(row["sequencing_sample_id"]).strip()
            self.logsum.feed_key(sample=sample_id)
            if sample_id in samples_json.keys():
                clean_metadata_rows.append(row)
            else:
                log_text = "Sample in metadata but missing in downloaded samples file"
                self.logsum.add_warning(sample=sample_id, entry=log_text)
                missing_samples.append(sample_id)
        return clean_metadata_rows, missing_samples

    def adding_fixed_fields(self, m_data):
        """Include fixed data that are always the same for every sample"""
        p_data = self.configuration.get_topic_data("lab_metadata", "fixed_fields")
        for idx in range(len(m_data)):
            for key, value in p_data.items():
                m_data[idx][key] = value
            m_data[idx]["schema_name"] = self.schema_name
            m_data[idx]["schema_version"] = self.schema_version
            m_data[idx]["submitting_institution_id"] = self.lab_code
        return m_data

    def adding_copy_from_other_field(self, m_data):
        """Add a new field with information based in another field."""
        p_data = self.configuration.get_topic_data(
            "lab_metadata", "required_copy_from_other_field"
        )
        for idx in range(len(m_data)):
            for key, value in p_data.items():
                m_data[idx][key] = m_data[idx][value]
        return m_data

    def adding_post_processing(self, m_data):
        """Add fields which values require post processing"""
        p_data = self.configuration.get_topic_data(
            "lab_metadata", "required_post_processing"
        )
        for idx in range(len(m_data)):
            for key, p_values in p_data.items():
                value = m_data[idx].get(key)
                if not value:
                    continue
                if value in p_values:
                    p_field, p_set = p_values[value].split("::")
                    m_data[idx][p_field] = p_set
                else:
                    # Check if key p_values should match only part of the value
                    for reg_key, reg_value in p_values.items():
                        if reg_key in value:
                            p_field, p_set = reg_value.split("::")
                            m_data[idx][p_field] = p_set

        return m_data

    def adding_ontology_to_enum(self, m_data):
        """Read the schema to get the properties enum and, for those fields
        which have an enum property value, replace the value for the one
        that is defined in the schema.
        """
        enum_dict = {}
        for prop, values in self.relecov_sch_json["properties"].items():
            if "enum" in values:
                enum_dict[prop] = {}
                for enum in values["enum"]:
                    go_match = re.search(r"(.+) \[\w+:.*", enum)
                    if go_match:
                        enum_dict[prop][go_match.group(1)] = enum
                    else:
                        enum_dict[prop][enum] = enum
        ontology_errors = {}
        for idx in range(len(m_data)):
            for key, e_values in enum_dict.items():
                if key in m_data[idx]:
                    if m_data[idx][key] in e_values:
                        m_data[idx][key] = e_values[m_data[idx][key]]
                    else:
                        sample_id = m_data[idx]["sequencing_sample_id"]
                        log_text = f"No ontology found for {m_data[idx][key]} in {key}"
                        self.logsum.add_warning(sample=sample_id, entry=log_text)
                        try:
                            ontology_errors[key] += 1
                        except KeyError:
                            ontology_errors[key] = 1
                        continue
        if len(ontology_errors) >= 1:
            stderr.print(
                "[red] No ontology could be added in:\n",
                "\n".join({f"{x} - {y} samples" for x, y in ontology_errors.items()}),
            )
        return m_data

    def process_from_json(self, m_data, json_fields):
        """Find the labels that are missing in the file to match the given schema."""
        map_field = json_fields["map_field"]
        col_name = self.relecov_sch_json["properties"].get(map_field).get("label")
        json_data = json_fields["j_data"]
        for idx in range(len(m_data)):
            sample_id = str(m_data[idx].get("sequencing_sample_id"))
            if m_data[idx].get(map_field):
                try:
                    m_data[idx].update(json_data[m_data[idx][map_field]])
                except KeyError as error:
                    clean_error = re.sub("[\[].*?[\]]", "", str(error.args[0]))
                    if str(clean_error).lower().strip() == "not provided":
                        log_text = (
                            f"Label {col_name} was not provided in sample "
                            + f"{sample_id}, auto-completing with Not Provided"
                        )
                        self.logsum.add_warning(sample=sample_id, entry=log_text)
                    else:
                        log_text = (
                            f"Unknown field value {error} for json data: "
                            + f"{str(col_name)} in sample {sample_id}. Skipped"
                        )
                        self.logsum.add_warning(sample=sample_id, entry=log_text)
                        continue
                    # TODO: Include Not Provided as a configuration field
                    fields_to_add = {
                        x: "Not Provided [GENEPIO:0001668]"
                        for x in json_fields["adding_fields"]
                    }
                    m_data[idx].update(fields_to_add)
        return m_data

    def adding_fields(self, metadata):
        """Add information located inside various json file as fields"""

        for key, values in self.json_req_files.items():
            stderr.print(f"[blue]Processing {key}")
            f_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "conf", values["file"]
            )
            values["j_data"] = relecov_tools.utils.read_json_file(f_path)
            metadata = self.process_from_json(metadata, values)
            stderr.print(f"[green]Processed {key}")

        # Include Sample information data from sample json file
        stderr.print("[blue]Processing sample data file")
        s_json = {}
        # TODO: Change sequencing_sample_id for some unique ID used in RELECOV database
        s_json["map_field"] = "sequencing_sample_id"
        s_json["adding_fields"] = self.samples_json_fields
        if self.sample_list_file:
            s_json["j_data"] = relecov_tools.utils.read_json_file(self.sample_list_file)
        else:
            s_json["j_data"] = self.get_samples_files_data(metadata)
        metadata = self.process_from_json(metadata, s_json)
        stderr.print("[green]Processed sample data file.")
        return metadata

    def read_configuration_json_files(self):
        """Read json files defined in configuration lab_metadata_req_json
        property
        """
        c_files = {}
        for item, value in self.json_files.items():
            f_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "conf", value
            )
            c_files[item] = relecov_tools.utils.read_json_file(f_path)
        return c_files

    def read_metadata_file(self):
        """Reads the input metadata file from header row, changes the metadata heading
        with their property name values defined in schema. Convert the date columns
        value to the yyyy/mm/dd format. Return list of dicts with data
        """
        meta_sheet = self.metadata_processing.get("excel_sheet")
        header_flag = self.metadata_processing.get("header_flag")
        sample_id_col = self.metadata_processing.get("sample_id_col")
        self.alternative_heading = False
        try:
            ws_metadata_lab, heading_row_number = relecov_tools.utils.read_excel_file(
                self.metadata_file, meta_sheet, header_flag, leave_empty=False
            )
        except KeyError:
            self.alternative_heading = True
            alt_sheet = self.metadata_processing.get("alternative_sheet")
            header_flag = self.metadata_processing.get("alternative_flag")
            sample_id_col = self.metadata_processing.get("alternative_sample_id_col")
            logtxt = f"No excel sheet named {meta_sheet}. Using {alt_sheet}"
            stderr.print(f"[yellow]{logtxt}")
            ws_metadata_lab, heading_row_number = relecov_tools.utils.read_excel_file(
                self.metadata_file, alt_sheet, header_flag, leave_empty=False
            )
        alt_header_dict = self.configuration.get_topic_data(
            "lab_metadata", "alt_heading_equivalences"
        )
        valid_metadata_rows = []
        row_number = heading_row_number
        for row in ws_metadata_lab:
            row_number += 1
            property_row = {}
            try:
                sample_id = str(row[sample_id_col]).strip()
            except KeyError:
                self.logsum.add_error(entry=f"No {sample_id_col} found in excel file")
                continue
            if not row[sample_id_col] or "Not Provided" in sample_id:
                log_text = f"{sample_id_col} not provided in row {row_number}. Skipped"
                self.logsum.add_warning(entry=log_text)
                stderr.print(f"[red]{log_text}")
                continue
            for key in row.keys():
                # skip the first column of the Metadata lab file
                if header_flag in key:
                    continue
                if row[key] is None or "not provided" in str(row[key]).lower():
                    log_text = f"{key} not provided for sample {sample_id}"
                    self.logsum.add_warning(sample=sample_id, entry=log_text)
                    continue
                if "date" in key.lower():
                    # Check if date is a string. Format YYYY/MM/DD to YYYY-MM-DD
                    pattern = r"^\d{4}[-/.]\d{2}[-/.]\d{2}"
                    if isinstance(row[key], dtime):
                        row[key] = str(row[key].date())
                    elif re.match(pattern, str(row[key])):
                        row[key] = str(row[key]).replace("/", "-").replace(".", "-")
                        row[key] = re.match(pattern, row[key]).group(0)
                    else:
                        try:
                            row[key] = str(int(float(str(row[key]))))
                            log.info("Date given as an integer. Understood as a year")
                        except (ValueError, TypeError):
                            log_text = f"Invalid date format in {key}: {row[key]}"
                            self.logsum.add_error(sample=sample_id, entry=log_text)
                            stderr.print(f"[red]{log_text} for sample {sample_id}")
                            continue
                elif "sample id" in key.lower():
                    if isinstance(row[key], float) or isinstance(row[key], int):
                        row[key] = str(int(row[key]))
                else:
                    if isinstance(row[key], float) or isinstance(row[key], int):
                        row[key] = str(row[key])
                if "date" not in key.lower() and isinstance(row[key], dtime):
                    logtxt = f"Non-date field {key} provided as date. Parsed as int"
                    self.logsum.add_warning(sample=sample_id, entry=logtxt)
                    row[key] = str(relecov_tools.utils.excel_date_to_num(row[key]))
                if self.alternative_heading:
                    alt_key = alt_header_dict.get(key)
                if row[key] is not None or "not provided" not in str(row[key]).lower():
                    try:
                        property_row[self.label_prop_dict[key]] = str(row[key]).strip()
                    except KeyError as e:
                        if self.alternative_heading:
                            try:
                                property_row[self.label_prop_dict[alt_key]] = str(
                                    row[key]
                                ).strip()
                                continue
                            except KeyError:
                                pass
                        log_text = f"Error when mapping the label {str(e)}"
                        self.logsum.add_error(sample=sample_id, entry=log_text)
                        stderr.print(f"[red]{log_text}")
                        continue
            valid_metadata_rows.append(property_row)

        return valid_metadata_rows

    def create_metadata_json(self):
        stderr.print("[blue]Reading Lab Metadata Excel File")
        valid_metadata_rows = self.read_metadata_file()
        clean_metadata_rows, missing_samples = self.match_to_json(valid_metadata_rows)
        if missing_samples:
            num_miss = len(missing_samples)
            logtx = "%s samples not found in metadata: %s" % (num_miss, missing_samples)
            self.logsum.add_warning(entry=logtx)
            stderr.print(f"[yellow]{num_miss} samples missing:\n{missing_samples}")
        # Continue by adding extra information
        stderr.print("[blue]Including additional information")

        extended_metadata = self.adding_fields(clean_metadata_rows)
        stderr.print("[blue]Including post processing information")
        extended_metadata = self.adding_post_processing(extended_metadata)
        extended_metadata = self.adding_copy_from_other_field(extended_metadata)
        extended_metadata = self.adding_fixed_fields(extended_metadata)
        completed_metadata = self.adding_ontology_to_enum(extended_metadata)
        if not completed_metadata:
            stderr.print("Metadata was completely empty. No output file generated")
            sys.exit(1)
        file_code = "lab_metadata_" + self.lab_code + "_"
        file_name = file_code + self.date + ".json"
        stderr.print("[blue]Writting output json file")
        os.makedirs(self.output_folder, exist_ok=True)
        self.logsum.create_error_summary(called_module="read-lab-metadata")
        file_path = os.path.join(self.output_folder, file_name)
        relecov_tools.utils.write_json_fo_file(completed_metadata, file_path)
        return True
