#!/usr/bin/env python
import copy
import json
import os
import re
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime as dtime

import rich.console

import relecov_tools.assets.schema_utils.jsonschema_draft

import relecov_tools.utils
from relecov_tools.base_module import BaseModule
from relecov_tools.config_json import ConfigJson

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


@dataclass
class SchemaField:
    path: tuple[str, ...]
    schema_type: str = "string"
    is_array_item: bool = False

    @property
    def top_level(self) -> str | None:
        return self.path[0] if self.path else None

    @property
    def field_name(self) -> str | None:
        return self.path[-1] if self.path else None


class LabMetadata(BaseModule):
    def __init__(
        self,
        metadata_file=None,
        sample_list_file=None,
        output_dir=None,
        files_folder=None,
        project=None,
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

        self.config_json = ConfigJson(extra_config=True)
        self.configuration = self.config_json
        self.institution_config = self.config_json.get_configuration(
            "institutions_config"
        )

        self.readmeta_config = (
            self.configuration.get_configuration("read_lab_metadata") or {}
        )
        default_project = self.readmeta_config.get("default_project") or "relecov"
        self.project = (project or default_project).lower()
        self.project_config = self._load_project_config(
            self.readmeta_config, self.project, default_project
        )
        self.log.info("Using project configuration '%s'", self.project)
        schema_file = self.project_config.get(
            "schema_file"
        ) or self.config_json.get_topic_data("generic", "relecov_schema")
        relecov_sch_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", schema_file
        )

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

        self.schema_properties = self.relecov_sch_json.get("properties", {})
        self.schema_field_map = self._build_schema_field_map()
        self.schema_property_names = set(self.schema_properties.keys())
        self.not_provided_field = self.config_json.get_topic_data(
            "generic", "not_provided_field"
        )
        self.fixed_fields = self.project_config.get("fixed_fields", {}) or {}
        self.organism_mapping = self.project_config.get("organism_mapping", {}) or {}
        self.required_copy_fields = (
            self.project_config.get("required_copy_from_other_field", {}) or {}
        )
        self.required_post_processing = (
            self.project_config.get("required_post_processing", {}) or {}
        )
        self.json_req_files = self.project_config.get("lab_metadata_req_json", {}) or {}
        self.schema_name = self.relecov_sch_json["title"]
        self.schema_version = self.relecov_sch_json["version"]
        base_metadata_processing = (
            self.config_json.get_topic_data("sftp_handle", "metadata_processing") or {}
        )
        project_metadata_processing = (
            self.project_config.get("metadata_processing", {}) or {}
        )
        self.metadata_processing = self._deep_merge_dicts(
            base_metadata_processing, project_metadata_processing
        )
        self.samples_json_fields = self.project_config.get("samples_json_fields", [])
        if not self.samples_json_fields:
            self.samples_json_fields = self.config_json.get_topic_data(
                "read_lab_metadata", "samples_json_fields"
            )
        self.unique_sample_id = self.project_config.get(
            "unique_sample_id", "sequencing_sample_id"
        )
        self.alt_heading_equivalences = (
            self.project_config.get("alt_heading_equivalences", {}) or {}
        )
        self.header_alias_lookup = self._build_header_alias_index(
            self.alt_heading_equivalences
        )
        self.date = dtime.now().strftime("%Y%m%d%H%M%S")

    def _split_institution(self, raw: str) -> tuple[str, str]:
        """
        Split a free-text institution string into:

            1. **name** – the visible name without any bracketed tags.
            2. **code** – the last bracketed element, assumed to be the CCN
            (e.g. `[1328000027]`). If no second bracket exists, returns "".

        Example
        -------
        _split_institution("Hospital X [Madrid] [1328000027]")
        ("Hospital X", "1328000027")
        """
        name = re.split(r"\s*\[", raw, maxsplit=1)[0].strip()
        brackets = re.findall(r"\[([^\]]+)\]", raw)
        code = brackets[-1].strip() if len(brackets) >= 2 else ""
        return name, code

    INSTITUTION_FIELDS = {
        "collecting_institution",
        "submitting_institution",
        "sequencing_institution",
    }

    def _deep_merge_dicts(self, base: dict | None, override: dict | None) -> dict:
        """Return a copy of `base` with any override keys merged recursively."""
        base = copy.deepcopy(base) if isinstance(base, dict) else {}
        if not override:
            return base
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(base.get(key), dict):
                base[key] = self._deep_merge_dicts(base.get(key), value)
            else:
                base[key] = value
        return base

    def _load_project_config(
        self, base_config: dict, project: str, default_project: str | None = None
    ) -> dict:
        """Merge base read_lab_metadata configuration with the requested project."""
        if not isinstance(base_config, dict):
            return {}
        project_map = {
            (key.lower() if isinstance(key, str) else key): value
            for key, value in (base_config.get("projects") or {}).items()
        }
        overrides = project_map.get(project)
        if (
            overrides is None
            and project_map
            and (default_project or "relecov").lower() != project
        ):
            self.log.warning(
                "Project '%s' not found in configuration. Using base settings", project
            )
        merged = self._deep_merge_dicts(base_config, overrides or {})
        merged.pop("projects", None)
        merged.pop("default_project", None)
        return merged

    def _build_schema_field_map(self) -> dict[str, SchemaField]:
        """Create a lookup for every schema property label/path, including arrays."""
        mapping: dict[str, SchemaField] = {}

        def _walk(properties: dict, parent_path: tuple[str, ...] = ()):
            """Walk through the schema properties recursively to build the mapping (it includes complex fields)"""
            for prop, conf in properties.items():
                current_path = parent_path + (prop,)
                prop_type = conf.get("type", "string")
                label = conf.get("label")
                if (
                    prop_type == "array"
                    and conf.get("items", {}).get("type") == "object"
                ):
                    for child, child_conf in (
                        conf["items"].get("properties", {}).items()
                    ):
                        child_label = child_conf.get("label")
                        descriptor = SchemaField(
                            path=current_path + (child,),
                            schema_type=child_conf.get("type", "string"),
                            is_array_item=True,
                        )
                        if child_label:
                            mapping[child_label] = descriptor
                        mapping[".".join((prop, child))] = descriptor
                else:
                    descriptor = SchemaField(
                        path=current_path,
                        schema_type=prop_type,
                        is_array_item=False,
                    )
                    if label:
                        mapping[label] = descriptor
                    mapping[prop] = descriptor

        _walk(self.relecov_sch_json.get("properties", {}))
        return mapping

    def _build_header_alias_index(self, alias_map: dict) -> dict[str, set[str]]:
        """Index canonical headers to all aliases (plus themselves) for fast lookup."""
        lookup: dict[str, set[str]] = {}
        for alias, canonical in alias_map.items():
            if not isinstance(alias, str) or not isinstance(canonical, str):
                continue
            lookup.setdefault(canonical, set()).add(alias)
        for canonical in list(lookup.keys()):
            lookup[canonical].add(canonical)
        return lookup

    def _normalize_header(self, header):
        """Return alias-normalised header or original if no mapping exists."""
        if not isinstance(header, str):
            return header
        header = header.strip()
        return self.alt_heading_equivalences.get(header, header)

    def _get_row_value(self, row: dict, label: str):
        """Fetch row value checking canonical header name first, then aliases."""
        if label in row:
            return row[label]
        for candidate in self.header_alias_lookup.get(label, []):
            if candidate in row:
                return row[candidate]
        return row.get(label)

    @staticmethod
    def _finalize_array_items(array_data: dict[str, dict]) -> dict[str, list[dict]]:
        """Convert temporary dict values into schema-compliant array entries."""
        finalized = {}
        for array_name, values in array_data.items():
            cleaned = {k: v for k, v in values.items() if v not in (None, "")}
            if cleaned:
                finalized[array_name] = [cleaned]
        return finalized

    def _set_if_allowed(self, row: dict, key: str, value):
        """Assign metadata value only if the schema exposes that property."""
        if key in self.schema_property_names:
            row[key] = value

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
                return self.config_json.get_topic_data("generic", "not_provided_field")

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
        for idx in range(len(m_data)):
            organism = m_data[idx].get("organism", "")
            if (
                isinstance(organism, str)
                and organism in self.organism_mapping
                and "tax_id" in self.schema_property_names
            ):
                m_data[idx]["tax_id"] = self.organism_mapping[organism]["tax_id"]
                if "host_disease" in self.schema_property_names:
                    m_data[idx]["host_disease"] = self.organism_mapping[organism][
                        "host_disease"
                    ]
            else:
                if "tax_id" in self.schema_property_names:
                    m_data[idx]["tax_id"] = "Missing [LOINC:LA14698-7]"
                if "host_disease" in self.schema_property_names:
                    m_data[idx]["host_disease"] = "Missing [LOINC:LA14698-7]"
            for key, value in self.fixed_fields.items():
                self._set_if_allowed(m_data[idx], key, value)
            if "schema_name" in self.schema_property_names:
                m_data[idx]["schema_name"] = self.schema_name
            if "schema_version" in self.schema_property_names:
                m_data[idx]["schema_version"] = self.schema_version
            if "submitting_institution_id" in self.schema_property_names:
                m_data[idx]["submitting_institution_id"] = self.lab_code
        return m_data

    def adding_copy_from_other_field(self, m_data):
        """Add a new field with information based in another field."""
        for idx in range(len(m_data)):
            for key, value in self.required_copy_fields.items():
                if key not in self.schema_property_names:
                    continue
                if value not in m_data[idx]:
                    continue
                m_data[idx][key] = m_data[idx][value]
        return m_data

    def adding_post_processing(self, m_data):
        """Add fields which values require post processing"""
        for idx in range(len(m_data)):
            for key, p_values in self.required_post_processing.items():
                if key not in self.schema_property_names:
                    continue
                value = m_data[idx].get(key)
                if not value:
                    continue
                if value in p_values:
                    p_field, p_set = p_values[value].split("::")
                    if p_field in self.schema_property_names:
                        m_data[idx][p_field] = p_set
                else:
                    # Check if key p_values should match only part of the value
                    for reg_key, reg_value in p_values.items():
                        if reg_key in value:
                            p_field, p_set = reg_value.split("::")
                            if p_field in self.schema_property_names:
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
        """
        Fill in the fields defined in *json_fields* for each sample.

        This is done in two phases:

        1.  **Per-sample
            - If *map_field* is empty → warning in `log_summary.json`.
            - If the code exists but is not found in the auxiliary JSON → warning in `log_summary.json`.
            - If the code exists and is found → add the fields specified in *adding_fields*.
            - If any of these fields are `Not Provided`, they are filled with `Not Provided [SNOMED:434941000124101]`.

        2.  **console summary** (`stderr`)
            - On completion, a **single line** per type of problem
              (empty codes or unknown codes) is displayed indicating how many samples
              are affected and the affected property.
            Example:
                `13 samples without CCN; check log`.

        Parameters
        ----------
            m_data : list[dict]
            Metadata already read from Excel.
            json_fields : dict
            Dict taken from *configuration.json* with:
                        - map_field
                        - adding_fields
                        - file
                        - j_data (previously loaded in adding_fields())

        Returns
        -------
            list[dict]
            Metadata with the new fields added.
        """
        map_field = json_fields["map_field"]
        prop_conf = self.relecov_sch_json.get("properties", {}).get(map_field, {})
        if not prop_conf:
            self.log.warning(
                "Map field '%s' not present in schema. Using fallback label.",
                map_field,
            )
        col_label = prop_conf.get("label", map_field)
        json_data = json_fields["j_data"]
        allowed_fields = [
            field
            for field in json_fields["adding_fields"]
            if field in self.schema_property_names
        ]

        # ─── counters for the summary ─────────────────────────────────────────
        empty_codes = []  # samples without value in map_field
        unknown_codes = []  # value present but not found in auxiliary json

        for row in m_data:
            sample_id = str(row.get(self.unique_sample_id))
            code = (row.get(map_field) or "").strip()

            # ╭─ 1. Empty field ───────────────────────────────────────────────╮
            if not code:
                msg = (
                    f"{col_label} not provided; cannot map "
                    f"{json_fields['file']} data"
                )
                self.logsum.add_warning(sample=sample_id, entry=msg)
                empty_codes.append(sample_id)
                continue
            # ╰────────────────────────────────────────────────────────────────╯

            # ─── 2. Attempt Mapping ────────────────────────────────────────────
            try:
                for k, v in json_data[code].items():
                    if k not in allowed_fields:
                        continue
                    row[k] = v

            except KeyError:
                # Code present but does not exist in the auxiliary JSON
                msg = (
                    f"Unknown {col_label} '{code}' in {json_fields['file']} "
                    f"for sample {sample_id}"
                )
                self.logsum.add_warning(sample=sample_id, entry=msg)
                unknown_codes.append(sample_id)
                continue

            # ─── 3. Fill Not Provided in the added fields ─────────────
            for field in allowed_fields:
                if (
                    field not in row
                    or not row[field]
                    or str(row[field]).lower().startswith("not provided")
                ):
                    row[field] = self.config_json.get_topic_data(
                        "generic", "not_provided_field"
                    )

        # ─── 4. Summary in stderr ──────────────────────────────────────────
        if empty_codes:
            stderr.print(
                f"[yellow]{len(empty_codes)} samples without {col_label}; "
                "check the log"
            )
        if unknown_codes:
            stderr.print(
                f"[yellow]{len(unknown_codes)} values of {col_label} not found "
                f"en {json_fields['file']}; check the log"
            )

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
                row["file_format"] = self.config_json.get_topic_data(
                    "generic", "not_provided_field"
                )

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
        """
        Reads the input metadata file, converts headings to schema properties
        and returns a list[dict] with the cleaned rows.
        """
        meta_sheet = self.metadata_processing.get("excel_sheet")
        header_flag = self.metadata_processing.get("header_flag")
        sample_id_col = self.metadata_processing.get("sample_id_col")
        self.alternative_heading = False

        # ────────── 0. Open sheet ──────────
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
            self.log.error(logtxt)
            ws_metadata_lab, heading_row_number = relecov_tools.utils.read_excel_file(
                self.metadata_file, alt_sheet, header_flag, leave_empty=False
            )

        valid_metadata_rows, included_sample_ids = [], []
        row_number = heading_row_number

        for row in ws_metadata_lab:
            row_number += 1
            property_row = {}
            array_values = defaultdict(dict)

            sample_cell = self._get_row_value(row, sample_id_col)
            if sample_cell is None:
                self.logsum.add_error(entry=f"No {sample_id_col} found in excel file")
                continue

            sample_id = str(sample_cell).strip()
            if sample_id in included_sample_ids:
                log_text = (
                    f"Skipped duplicated sample {sample_id} in row {row_number}. "
                    f"Sequencing sample id must be unique"
                )
                self.logsum.add_warning(entry=log_text)
                continue

            if not sample_cell or "Not Provided" in sample_id:
                fallback_id = row.get("collecting_lab_sample_id") or row.get(
                    "sequence_file_R1", ""
                )
                if isinstance(fallback_id, str):
                    sample_id = fallback_id.split(".")[0]
                else:
                    sample_id = str(fallback_id).split(".")[0]
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

            for raw_key, raw_value in row.items():
                if raw_key is None:
                    continue
                if header_flag and isinstance(raw_key, str) and header_flag in raw_key:
                    continue

                canonical_key = self._normalize_header(raw_key)
                descriptor = None
                if isinstance(canonical_key, str):
                    descriptor = self.schema_field_map.get(canonical_key)
                    if descriptor is None:
                        descriptor = self.schema_field_map.get(canonical_key.strip())

                value = raw_value

                if (
                    value is None
                    or value == ""
                    or (isinstance(value, str) and "not provided" in value.lower())
                ):
                    log_text = f"{raw_key} not provided for sample {sample_id}"
                    self.logsum.add_warning(sample=sample_id, entry=log_text)
                    continue

                schema_key = (
                    descriptor.top_level
                    if descriptor and descriptor.top_level
                    else canonical_key
                )
                schema_type = (
                    descriptor.schema_type
                    if descriptor
                    else self.schema_properties.get(schema_key, {}).get(
                        "type", "string"
                    )
                )
                try:
                    value = relecov_tools.utils.cast_value_to_schema_type(
                        value, schema_type
                    )
                except (ValueError, TypeError) as e:
                    log_text = (
                        f"Type conversion error for {raw_key} (expected {schema_type}): "
                        f"{raw_value}. {e}"
                    )
                    self.logsum.add_error(sample=sample_id, entry=log_text)
                    stderr.print(f"[red]{log_text}")
                    continue

                key_for_checks = (
                    canonical_key if isinstance(canonical_key, str) else str(raw_key)
                )
                if isinstance(key_for_checks, str) and "date" in key_for_checks.lower():
                    pattern = r"^\d{4}[-/.]\d{2}[-/.]\d{2}"
                    if isinstance(raw_value, dtime):
                        value = str(raw_value.date())
                    elif re.match(pattern, str(raw_value)):
                        value = re.match(
                            pattern,
                            str(raw_value).replace("/", "-").replace(".", "-"),
                        ).group(0)
                    else:
                        try:
                            value = str(int(float(str(raw_value))))
                            self.log.info(
                                "Date given as an integer. Understood as a year"
                            )
                        except (ValueError, TypeError):
                            log_text = f"Invalid date format in {raw_key}: {raw_value}"
                            self.logsum.add_error(sample=sample_id, entry=log_text)
                            stderr.print(f"[red]{log_text} for sample {sample_id}")
                            continue
                elif (
                    isinstance(key_for_checks, str)
                    and "sample id" in key_for_checks.lower()
                ):
                    if isinstance(raw_value, (float, int)):
                        value = str(int(raw_value))
                elif isinstance(raw_value, (float, int)) and not isinstance(value, str):
                    value = str(raw_value)

                if (
                    isinstance(key_for_checks, str)
                    and "date" not in key_for_checks.lower()
                    and isinstance(raw_value, dtime)
                ):
                    logtxt = f"Non-date field {raw_key} provided as date. Parsed as int"
                    self.logsum.add_warning(sample=sample_id, entry=logtxt)
                    value = str(relecov_tools.utils.excel_date_to_num(raw_value))

                if (
                    isinstance(schema_key, str)
                    and schema_key in self.INSTITUTION_FIELDS
                    and value
                    and not (descriptor and descriptor.is_array_item)
                ):
                    name, code = self._split_institution(str(value))
                    value = name
                    if schema_key == "collecting_institution":
                        if code:
                            property_row["collecting_institution_code_1"] = code
                        else:
                            self.logsum.add_warning(
                                sample=sample_id,
                                entry="CCN not provided for collecting_institution",
                            )

                if descriptor and descriptor.is_array_item:
                    array_values[descriptor.top_level][descriptor.field_name] = value
                    continue

                property_row[schema_key] = value

            property_row.update(self._finalize_array_items(array_values))
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
