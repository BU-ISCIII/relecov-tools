#!/usr/bin/env python
import json
import rich.console
import os
import re
from datetime import datetime as dtime
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
from relecov_tools.base_module import BaseModule

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class LabMetadata(BaseModule):
    def __init__(
        self,
        metadata_file=None,
        sample_list_file=None,
        output_dir=None,
        files_folder=None,
        **kwargs,
    ):
        super().__init__(output_dir=output_dir, called_module=__name__)
        self.log.info("Initiating read-lab-metadata process")
        self.sample_list_file = sample_list_file
        self.files_folder = files_folder

        if metadata_file is None:
            self.metadata_file = relecov_tools.utils.prompt_path(
                msg="Select the excel file which contains metadata"
            )
        else:
            self.metadata_file = metadata_file

        if not os.path.exists(self.metadata_file):
            self.log.error("Metadata file %s does not exist ", self.metadata_file)
            stderr.print(
                "[red] Metadata file " + self.metadata_file + " does not exist"
            )
            raise FileNotFoundError(f"Metadata file {self.metadata_file} not found")

        if sample_list_file is None:
            stderr.print("[yellow]No samples_data.json file provided")
            self.log.warning("No samples_data.json file provided")
            if not os.path.isdir(str(files_folder)):
                stderr.print("[red]No samples file nor valid files folder provided")
                self.log.error("No samples file nor valid files folder provided")
                raise FileNotFoundError(
                    "No samples file nor valid files folder provided"
                )
            self.files_folder = os.path.abspath(files_folder)

        if sample_list_file is not None and not os.path.exists(sample_list_file):
            self.log.error(
                "Sample information file %s does not exist ", sample_list_file
            )
            stderr.print("[red] Samples file " + sample_list_file + " does not exist")
            raise FileNotFoundError(
                "Sample information file %s does not exist ", sample_list_file
            )
        else:
            self.sample_list_file = sample_list_file

        if output_dir is None:
            self.output_dir = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_dir = output_dir

        config_json = ConfigJson(extra_config=True)

        # TODO: remove hardcoded schema selection
        relecov_schema = config_json.get_topic_data("generic", "json_schemas")[
            "relecov_schema"
        ]
        relecov_sch_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", relecov_schema
        )
        self.configuration = config_json
        self.institution_config = config_json.get_configuration("institutions_config")

        out_path = os.path.realpath(self.output_dir)
        self.lab_code = out_path.split("/")[-2]
        self.logsum = self.parent_log_summary(
            output_dir=self.output_dir, lab_code=self.lab_code, path=out_path
        )

        with open(relecov_sch_path, "r") as fh:
            self.relecov_sch_json = json.load(fh)

        try:
            relecov_tools.assets.schema_utils.jsonschema_draft.check_schema_draft(
                self.relecov_sch_json, "2020-12"
            )
        except Exception as e:
            self.log.error("JSON schema is not valid: %s", str(e))
            stderr.print(f"[red]Error: JSON schema is not valid.\n{str(e)}")
            raise

        self.label_prop_dict = {}

        for prop, values in self.relecov_sch_json["properties"].items():
            try:
                self.label_prop_dict[values["label"]] = prop
            except KeyError:
                self.log.warning("Property %s does not have 'label' attribute", prop)
                stderr.print(
                    "[orange]Property " + prop + " does not have 'label' attribute"
                )
                continue
        self.date = dtime.now().strftime("%Y%m%d%H%M%S")
        self.json_req_files = config_json.get_topic_data(
            "read_lab_metadata", "lab_metadata_req_json"
        )
        self.schema_name = self.relecov_sch_json["title"]
        self.schema_version = self.relecov_sch_json["version"]
        self.metadata_processing = config_json.get_topic_data(
            "sftp_handle", "metadata_processing"
        )
        self.samples_json_fields = config_json.get_topic_data(
            "read_lab_metadata", "samples_json_fields"
        )
        self.unique_sample_id = "sequencing_sample_id"

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
            self.log.info("Generating md5 hash for %s...", str(file))
            try:
                return relecov_tools.utils.calculate_md5(file)
            except IOError:
                return "Not Provided [SNOMED:434941000124101]"

        # The files are and md5file are supposed to be located together
        dir_path = self.files_folder
        md5_checksum_files = [
            os.path.join(dir_path, f) for f in os.listdir(dir_path) if "md5" in f
        ]
        if md5_checksum_files:
            skip_list = self.configuration.get_topic_data(
                "sftp_handle", "skip_when_found"
            )
            md5_dict = relecov_tools.utils.read_md5_checksum(
                file_name=md5_checksum_files[0], avoid_chars=skip_list
            )
        else:
            md5_dict = {}
            self.log.warning("No md5sum file found.")
            self.log.warning("Generating new md5 hashes. This might take a while...")
        j_data = {}
        no_fastq_error = "No R1 fastq file was given for sample %s in metadata"
        n = 0
        for sample in clean_metadata_rows:
            n += 1
            sample_id = str(sample.get(self.unique_sample_id))
            if not sample_id:
                if sample.get("collecting_lab_sample_id"):
                    sample_id = sample["collecting_lab_sample_id"]
                    self.unique_sample_id = "collecting_lab_sample_id"
                else:
                    sample_id = sample.get("sequence_file_R1", "").split(".")[0]
                    self.unique_sample_id = "sequence_file_R1"
            files_dict = {}
            r1_file = sample.get("sequence_file_R1")
            r2_file = sample.get("sequence_file_R2")
            if not r1_file:
                self.logsum.add_error(
                    sample=sample_id,
                    entry=no_fastq_error % sample_id,
                )
                j_data[sample_id] = files_dict
                continue
            r1_md5 = md5_dict.get(r1_file)
            r2_md5 = md5_dict.get(r2_file)
            files_dict["sequence_file_R1"] = r1_file
            files_dict["sequence_file_path_R1"] = dir_path
            batch_id = dir_path.split("/")[-1]
            logtxt = f"Setting batch_id to {batch_id} based on download dir: {dir_path}"
            stderr.print(f"[yellow]{logtxt}")
            self.log.info(logtxt)
            files_dict["batch_id"] = dir_path.split("/")[-1]
            if not os.path.exists(os.path.join(dir_path, r1_file)):
                self.logsum.add_error(
                    sample=sample_id, entry="Provided R1 file not found after download"
                )
                continue
            if r1_md5:
                files_dict["sequence_file_R1_md5"] = r1_md5
            else:
                files_dict["sequence_file_R1_md5"] = safely_calculate_md5(
                    os.path.join(dir_path, r1_file)
                )
            if r2_file:
                files_dict["sequence_file_R2"] = r2_file
                files_dict["sequence_file_path_R2"] = dir_path
                if not os.path.exists(os.path.join(dir_path, r2_file)):
                    self.logsum.add_error(
                        sample=sample_id,
                        entry="Provided R2 file not found after download",
                    )
                    continue
                if r2_md5:
                    files_dict["sequence_file_R2_md5"] = r2_md5
                else:
                    files_dict["sequence_file_R2_md5"] = safely_calculate_md5(
                        os.path.join(dir_path, r1_file)
                    )
            if sample_id in j_data:
                sample_id = "_".join([sample_id, str(n)])
            j_data[sample_id] = files_dict
        if not any(val for val in j_data.values()):
            errtxt = f"No files found for the samples in {dir_path}"
            self.logsum.add_error(entry=errtxt, sample=sample_id)
            stderr.print(f"[red]{errtxt}")
        try:
            samples_filename = "_".join(["samples_data", self.lab_code + ".json"])
            samples_filename = self.tag_filename(filename=samples_filename)
            file_path = os.path.join(self.output_dir, samples_filename)
            relecov_tools.utils.write_json_to_file(j_data, file_path)
        except Exception:
            self.log.error("Could not output samples_data.json file to output folder")
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
            sample_id = str(row[self.unique_sample_id]).strip()
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
        p_data = self.configuration.get_topic_data("read_lab_metadata", "fixed_fields")
        organism_mapping = self.configuration.get_topic_data(
            "read_lab_metadata", "organism_mapping"
        )

        for idx in range(len(m_data)):
            organism = m_data[idx].get("organism", "")
            if organism in organism_mapping:
                m_data[idx]["tax_id"] = organism_mapping[organism]["tax_id"]
                m_data[idx]["host_disease"] = organism_mapping[organism]["host_disease"]
            else:
                m_data[idx]["tax_id"] = "Missing [LOINC:LA14698-7]"
                m_data[idx]["host_disease"] = "Missing [LOINC:LA14698-7]"
            for key, value in p_data.items():
                m_data[idx][key] = value
            m_data[idx]["schema_name"] = self.schema_name
            m_data[idx]["schema_version"] = self.schema_version
            m_data[idx]["submitting_institution_id"] = self.lab_code
        return m_data

    def adding_copy_from_other_field(self, m_data):
        """Add a new field with information based in another field."""
        p_data = self.configuration.get_topic_data(
            "read_lab_metadata", "required_copy_from_other_field"
        )
        for idx in range(len(m_data)):
            for key, value in p_data.items():
                m_data[idx][key] = m_data[idx][value]
        return m_data

    def adding_post_processing(self, m_data):
        """Add fields which values require post processing"""
        p_data = self.configuration.get_topic_data(
            "read_lab_metadata", "required_post_processing"
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
            enum_values = values.get("enum", [])
            ontologies_present = any(
                isinstance(enum, str) and re.search(r" \[\w+:.*\]$", enum)
                for enum in enum_values
            )
            if not ontologies_present:
                continue
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
                    current_value = m_data[idx][key]
                    if re.search(r" \[\w+:.*\]$", current_value):
                        continue  # If already has ontology, do nothing.
                    if current_value in e_values:
                        m_data[idx][key] = e_values[current_value]
                    else:
                        sample_id = m_data[idx][self.unique_sample_id]
                        log_text = f"No ontology found for {current_value} in {key}"
                        self.logsum.add_warning(sample=sample_id, entry=log_text)
                        ontology_errors[key] = ontology_errors.get(key, 0) + 1
                        continue
        if len(ontology_errors) >= 1:
            stderr.print(
                "[red] No ontology could be added in:\n",
                "\n".join({f"{x} - {y} samples" for x, y in ontology_errors.items()}),
            )
            self.log.warning(
                "No ontology could be added in:\n%s",
                "\n".join(f"{x} - {y} samples" for x, y in ontology_errors.items()),
            )

        return m_data

    def process_from_json(self, m_data, json_fields):
        """Find the labels that are missing in the file to match the given schema."""
        map_field = json_fields["map_field"]
        col_name = self.relecov_sch_json["properties"].get(map_field).get("label")
        json_data = json_fields["j_data"]
        for idx in range(len(m_data)):
            sample_id = str(m_data[idx].get(self.unique_sample_id))
            if m_data[idx].get(map_field):
                # Remove potential ontology tags from value like [SNOMED:258500001]
                cleaned_key = re.sub(" [\[].*?[\]]", "", m_data[idx][map_field])
                try:
                    adding_data = {
                        k: v
                        for k, v in json_data[cleaned_key].items()
                        if k in json_fields["adding_fields"]
                    }
                    m_data[idx].update(adding_data)
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
                        x: "Not Provided [SNOMED:434941000124101]"
                        for x in json_fields["adding_fields"]
                    }
                    m_data[idx].update(fields_to_add)
        return m_data

    def infer_file_format_from_schema(self, metadata):
        """Infer the file_format field based on the extension in sequence_file_R1,
        using enum values (with ontology) directly from the schema."""

        extension_map = {
            ".fastq": "FASTQ",
            ".fastq.gz": "FASTQ",
            ".fq": "FASTQ",
            ".fq.gz": "FASTQ",
            ".bam": "BAM",
            ".cram": "CRAM",
            ".fasta": "FASTA",
            ".fa": "FASTA",
        }

        file_format_enum = (
            self.relecov_sch_json["properties"].get("file_format", {}).get("enum", [])
        )
        keyword_to_enum = {}

        for item in file_format_enum:
            match = re.match(r"^([A-Z]+)", item)
            if match:
                keyword = match.group(1)
                keyword_to_enum[keyword] = item

        for row in metadata:
            r1_file = row.get("sequence_file_R1", "").lower()
            file_format_val = None
            for ext, keyword in extension_map.items():
                if r1_file.endswith(ext.lower()):
                    file_format_val = keyword_to_enum.get(keyword)
                    break

            if file_format_val:
                row["file_format"] = file_format_val
            else:
                row["file_format"] = "Not Provided [SNOMED:434941000124101]"

        return metadata

    def adding_fields(self, metadata):
        """Add information located inside various json file as fields"""

        for key, values in self.json_req_files.items():
            stderr.print(f"[blue]Processing {key}")
            self.log.info(f"Processing {key}")
            f_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "conf", values["file"]
            )
            values["j_data"] = relecov_tools.utils.read_json_file(f_path)
            metadata = self.process_from_json(metadata, values)
            stderr.print(f"[green]Processed {key}")
            self.log.info(f"Processed {key}")

        if self.institution_config:
            self.log.info("Updating laboratory code from institutions_config...")
        # Include Sample information data from sample json file
        stderr.print("[blue]Processing sample data file")
        self.log.info("Processing sample data file")
        s_json = {}
        # TODO: Change sequencing_sample_id for some unique ID used in RELECOV database
        s_json["map_field"] = self.unique_sample_id
        s_json["adding_fields"] = self.samples_json_fields
        if self.sample_list_file:
            s_json["j_data"] = relecov_tools.utils.read_json_file(self.sample_list_file)
        else:
            s_json["j_data"] = self.get_samples_files_data(metadata)
        if not s_json["j_data"]:
            self.log.warning(
                f"Samples file {self.sample_list_file} is empty. All samples will be included"
            )
            s_json["j_data"] = self.get_samples_files_data(metadata)
        batch_id = self.get_batch_id_from_data(list(s_json["j_data"].values()))
        # This will declare self.batch_id in BaseModule() which will be used later
        self.set_batch_id(batch_id)
        metadata = self.process_from_json(metadata, s_json)
        metadata = self.infer_file_format_from_schema(metadata)
        stderr.print("[green]Processed sample data file.")
        self.log.info("Processed sample data file.")
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
            self.log.error(f"{logtxt}")
            ws_metadata_lab, heading_row_number = relecov_tools.utils.read_excel_file(
                self.metadata_file, alt_sheet, header_flag, leave_empty=False
            )
        valid_metadata_rows = []
        included_sample_ids = []
        row_number = heading_row_number
        for row in ws_metadata_lab:
            row_number += 1
            property_row = {}
            try:
                sample_id = str(row[sample_id_col]).strip()
            except KeyError:
                self.logsum.add_error(entry=f"No {sample_id_col} found in excel file")
                continue
            # Validations on the sample_id
            if sample_id in included_sample_ids:
                log_text = f"Skipped duplicated sample {sample_id} in row {row_number}. Sequencing sample id must be unique"
                self.logsum.add_warning(entry=log_text)
                continue
            if not row[sample_id_col] or "Not Provided" in sample_id:
                if row.get("collecting_lab_sample_id"):
                    sample_id = row["collecting_lab_sample_id"]
                else:
                    sample_id = row.get("sequence_file_R1", "").split(".")[0]
                if not sample_id:
                    log_text = (
                        f"{sample_id_col} not provided in row {row_number}. Skipped"
                    )
                    self.logsum.add_error(entry=log_text)
                    stderr.print(f"[red]{log_text}")
                    continue
                else:
                    log_text = f"{sample_id_col} not provided for {sample_id}"
                    self.logsum.add_error(entry=log_text, sample=sample_id)
                    stderr.print(f"[red]{log_text}")
            included_sample_ids.append(sample_id)
            for key in row.keys():
                if header_flag in key:
                    continue
                value = row[key]
                # Omitting empty or not provided values
                if value is None or value == "" or "not provided" in str(value).lower():
                    log_text = f"{key} not provided for sample {sample_id}"
                    self.logsum.add_warning(sample=sample_id, entry=log_text)
                    continue
                # Get JSON schema type
                schema_key = self.label_prop_dict.get(key, key)
                schema_type = (
                    self.relecov_sch_json["properties"]
                    .get(schema_key, {})
                    .get("type", "string")
                )
                # Conversion of values according to expected type
                try:
                    value = relecov_tools.utils.cast_value_to_schema_type(
                        value, schema_type
                    )
                except (ValueError, TypeError) as e:
                    log_text = f"Type conversion error for {key} (expected {schema_type}): {value}. {str(e)}"
                    self.logsum.add_error(sample=sample_id, entry=log_text)
                    stderr.print(f"[red]{log_text}")
                    continue
                if "date" in key.lower():
                    # Check if date is a string. Format YYYY/MM/DD to YYYY-MM-DD
                    pattern = r"^\d{4}[-/.]\d{2}[-/.]\d{2}"
                    if isinstance(row[key], dtime):
                        row[key] = str(row[key].date())
                    elif re.match(pattern, str(row[key])):
                        row[key] = str(row[key]).replace("/", "-").replace(".", "-")
                        row[key] = re.match(pattern, row[key]).group(0)
                        value = row[key]
                    else:
                        try:
                            row[key] = str(int(float(str(row[key]))))
                            self.log.info(
                                "Date given as an integer. Understood as a year"
                            )
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
                property_row[schema_key] = value
            valid_metadata_rows.append(property_row)
        return valid_metadata_rows

    def create_metadata_json(self):
        stderr.print("[blue]Reading Lab Metadata Excel File")
        valid_metadata_rows = self.read_metadata_file()
        stderr.print(f"[green]Processed {len(valid_metadata_rows)} valid metadata rows")
        self.log.info(f"Processed {len(valid_metadata_rows)} valid metadata rows")
        clean_metadata_rows, missing_samples = self.match_to_json(valid_metadata_rows)
        if missing_samples:
            num_miss = len(missing_samples)
            logtx = "%s samples not found in metadata: %s" % (num_miss, missing_samples)
            self.logsum.add_warning(entry=logtx)
            stderr.print(f"[yellow]{num_miss} samples missing:\n{missing_samples}")
        # Continue by adding extra information
        stderr.print("[blue]Including additional information")
        self.log.info("Including additional information")

        extended_metadata = self.adding_fields(clean_metadata_rows)
        stderr.print("[blue]Including post processing information")
        self.log.info("Including post processing information")
        extended_metadata = self.adding_post_processing(extended_metadata)
        extended_metadata = self.adding_copy_from_other_field(extended_metadata)
        extended_metadata = self.adding_fixed_fields(extended_metadata)
        completed_metadata = self.adding_ontology_to_enum(extended_metadata)
        if not completed_metadata:
            self.log.warning("Metadata was completely empty. No output file generated")
            stderr.print("Metadata was completely empty. No output file generated")
            return
        file_code = "_".join(["read_lab_metadata", self.lab_code]) + ".json"
        file_name = self.tag_filename(filename=file_code)
        stderr.print("[blue]Writting output json file")
        os.makedirs(self.output_dir, exist_ok=True)
        # Creating log summary
        self.parent_create_error_summary()
        file_path = os.path.join(self.output_dir, file_name)
        self.log.info("Writting output json file %s", file_path)
        relecov_tools.utils.write_json_to_file(completed_metadata, file_path)
        return True
