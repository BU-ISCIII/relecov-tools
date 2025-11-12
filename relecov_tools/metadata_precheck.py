#!/usr/bin/env python
from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from datetime import datetime
from typing import Any, Iterable

import rich.console
from rich.table import Table
from jsonschema import Draft202012Validator
import re

import relecov_tools.utils
from relecov_tools.base_module import BaseModule
from relecov_tools.config_json import ConfigJson
from relecov_tools.sftp_client import SftpClient
import relecov_tools.assets.schema_utils.custom_validators

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SchemaMapper:
    """Handle Excel header normalisation and schema-aware casting."""

    SAMPLE_FALLBACKS = [
        "Sample ID given by the submitting laboratory",
        "Sample ID given by originating laboratory",
        "Sample ID given in the microbiology lab",
        "Sample ID given if multiple rna-extraction or passages",
        "Sequence file R1",
        "Sequence file R1 fastq",
    ]

    def __init__(
        self,
        schema_properties: dict[str, Any],
        label_to_prop: dict[str, str],
        heading_aliases: dict[str, str] | None,
        not_provided_field: str | None,
    ) -> None:
        self.schema_properties = schema_properties
        self.label_to_prop = label_to_prop
        self.heading_aliases = heading_aliases or {}
        self.not_provided_field = (not_provided_field or "").lower()
        self.sample_fallbacks = [
            self.canonical_label(label) for label in self.SAMPLE_FALLBACKS
        ]

    def canonical_label(self, label: Any) -> str:
        if label is None:
            return ""
        label_str = str(label).strip()
        return self.heading_aliases.get(label_str, label_str)

    def canonicalize_row(
        self, excel_row: dict[str, Any], header_flag: str
    ) -> dict[str, Any]:
        """Normalise header names and drop the metadata flag column."""
        canon_row = {}
        for key, value in excel_row.items():
            if key == header_flag:
                continue
            canon_row[self.canonical_label(key)] = value
        return canon_row

    def normalise_sample_id(
        self, canonical_row: dict[str, Any], sample_column: str
    ) -> str | None:
        """Try to extract the sequencing sample id with sensible fallbacks."""
        lookup_order = [sample_column] + self.sample_fallbacks
        for key in lookup_order:
            candidate = canonical_row.get(key)
            cleaned = self._clean_cell(candidate)
            if cleaned:
                return str(cleaned).strip()
        return None

    def row_to_payload(self, canonical_row: dict[str, Any]) -> dict[str, Any]:
        """Convert a canonical Excel row into a schema-ready dict."""
        payload: dict[str, Any] = {}
        for label, value in canonical_row.items():
            schema_key = self.label_to_prop.get(label, label)
            if schema_key not in self.schema_properties:
                continue
            prepared = self._prepare_value(schema_key, value)
            if prepared is not None:
                payload[schema_key] = prepared
        return payload

    # ── Helpers ──────────────────────────────────────────────────────────
    def _prepare_value(self, schema_key: str, value: Any) -> Any:
        cleaned = self._clean_cell(value)
        if cleaned is None:
            return None
        if isinstance(cleaned, str) and not cleaned.strip():
            return None

        schema_def = self.schema_properties.get(schema_key, {})
        schema_type = schema_def.get("type")
        if isinstance(schema_type, list):
            schema_type = [s for s in schema_type if s != "null"]
            schema_type = schema_type[0] if schema_type else None

        if schema_type == "string":
            string_value = self._normalise_string(schema_key, cleaned)
            return self._align_to_enum(schema_key, string_value)

        if schema_type in {"integer", "number", "boolean"}:
            return relecov_tools.utils.cast_value_to_schema_type(cleaned, schema_type)

        if schema_type == "array":
            return self._prepare_array(schema_key, cleaned)

        return cleaned

    def _prepare_array(self, schema_key: str, value: Any) -> list[Any] | Any:
        if isinstance(value, list):
            raw_items = value
        elif isinstance(value, str):
            delimiter = self.schema_properties.get(schema_key, {}).get("delimiter", ";")
            raw_items = [
                part.strip() for part in value.split(delimiter) if part.strip()
            ]
        else:
            return value

        normalised = []
        for item in raw_items:
            normalised_item = self._normalise_string(schema_key, item)
            aligned_item = self._align_to_enum(
                schema_key, normalised_item, is_array_item=True
            )
            normalised.append(aligned_item)
        return normalised

    def _normalise_string(self, schema_key: str, value: Any) -> str:
        if isinstance(value, datetime):
            return (
                value.date().isoformat()
                if "date" in schema_key.lower()
                else value.isoformat()
            )
        if isinstance(value, (int, float)):
            text = str(int(value)) if float(value).is_integer() else str(value)
        else:
            text = str(value)
        text = text.strip()
        if "date" in schema_key.lower():
            return self._normalise_date(text)
        return text

    def _normalise_date(self, value: str) -> str:
        if not value:
            return value
        if value.lower() == self.not_provided_field:
            return value
        clean = value.replace("/", "-").replace(".", "-")
        match = re.match(r"^\d{4}-\d{2}-\d{2}", clean)
        if match:
            return match.group(0)
        try:
            parsed = datetime.strptime(clean, "%Y%m%d").date()
            return parsed.isoformat()
        except ValueError:
            return value

    def _align_to_enum(
        self, schema_key: str, value: Any, *, is_array_item: bool = False
    ) -> Any:
        schema_def = self.schema_properties.get(schema_key, {})
        enum_values = schema_def.get("enum")
        if is_array_item and not enum_values:
            enum_values = schema_def.get("items", {}).get("enum", [])
        if not enum_values or value is None:
            return value
        if value in enum_values:
            return value
        stripped_map = {
            self._strip_ontology(enum_val).lower(): enum_val for enum_val in enum_values
        }
        candidate = self._strip_ontology(value).lower()
        if candidate in stripped_map:
            return stripped_map[candidate]
        for enum_val in enum_values:
            if enum_val.lower() == str(value).lower():
                return enum_val
        return value

    @staticmethod
    def _strip_ontology(value: Any) -> str:
        text = str(value)
        if "[" in text and text.strip().endswith("]"):
            return text.split("[", 1)[0].strip()
        return text.strip()

    def _clean_cell(self, value: Any) -> Any:
        if isinstance(value, float) and value != value:  # NaN
            return None
        if isinstance(value, str):
            return value.strip()
        return value


class MetadataPrecheck(BaseModule):
    def __init__(
        self,
        user: str | None = None,
        password: str | None = None,
        conf_file: str | None = None,
        output_dir: str | None = None,
        target_folders: Iterable[str] | str | None = None,
        export_excel: bool = False,
    ) -> None:
        super().__init__(output_dir=output_dir, called_module=__name__)
        self.log.info("Initiating metadata precheck process")

        self.config = ConfigJson(extra_config=True)
        self.core_config = ConfigJson()

        generic_topic = self.core_config.get_topic_data("generic", "relecov_schema")
        schema_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "schema", generic_topic
        )
        self.schema = relecov_tools.utils.read_json_file(schema_path)

        self.metadata_processing = self.core_config.get_topic_data(
            "sftp_handle", "metadata_processing"
        )
        self.metadata_lab_heading = self.config.get_topic_data(
            "read_lab_metadata", "metadata_lab_heading"
        )
        self.heading_aliases = (
            self.config.get_topic_data("read_lab_metadata", "alt_heading_equivalences")
            or {}
        )

        self.required_properties = self.schema.get("required", [])
        self.schema_properties = self.schema.get("properties", {})
        self.label_to_prop = {}
        self.prop_to_label = {}
        for prop, definition in self.schema_properties.items():
            label = definition.get("label")
            if label:
                self.label_to_prop[label] = prop
                self.prop_to_label[prop] = label

        default_sample_label = self.metadata_processing.get("sample_id_col")
        self.sample_id_property = self.label_to_prop.get(
            default_sample_label, default_sample_label
        )
        self.not_provided_field = self.core_config.get_topic_data(
            "generic", "not_provided_field"
        )
        starting_date = self.core_config.get_topic_data("generic", "starting_date")
        try:
            start_date = datetime.strptime(starting_date, "%Y-%m-%d").date()
        except (TypeError, ValueError):
            start_date = datetime(2020, 1, 1).date()
        end_date = datetime.now().date()
        self._date_checker = (
            relecov_tools.assets.schema_utils.custom_validators.make_date_checker(
                start_date, end_date
            )
        )
        self.validator = Draft202012Validator(
            self.schema, format_checker=self._date_checker
        )

        self.mapper = SchemaMapper(
            self.schema_properties,
            self.label_to_prop,
            self.heading_aliases,
            self.not_provided_field,
        )

        self.sheet_options = self._build_sheet_options()

        self.export_excel = export_excel

        parsed_targets, prompt_flag = self._parse_target_folders(target_folders)
        self.target_folders = parsed_targets
        self.prompt_for_targets = prompt_flag

        if user is None:
            user = relecov_tools.utils.prompt_text(msg="Enter the user id")
        if password is None:
            password = relecov_tools.utils.prompt_password(msg="Enter your password")

        self.sftp_client = SftpClient(conf_file, user, password)

        self.logsum = self.parent_log_summary(output_dir=self.basemod_outdir)
        self.folder_reports: dict[str, list[dict[str, Any]]] = defaultdict(list)
        self.lab_summary: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "total_samples": 0,
                "valid_files": [],
                "invalid_files": [],
                "files": [],
                "invalid_sample_count": 0,
            }
        )
        self.generated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

        self.set_batch_id(datetime.utcnow().strftime("%Y%m%d%H%M%S"))

    def _build_sheet_options(self) -> list[dict[str, str]]:
        opts = []
        primary = {
            "sheet": self.metadata_processing.get("excel_sheet"),
            "header_flag": self.metadata_processing.get("header_flag"),
            "sample_col": self.metadata_processing.get("sample_id_col"),
        }
        alternative = {
            "sheet": self.metadata_processing.get("alternative_sheet"),
            "header_flag": self.metadata_processing.get("alternative_flag"),
            "sample_col": self.metadata_processing.get("alternative_sample_id_col"),
        }
        for option in (primary, alternative):
            if option["sheet"] and option["header_flag"]:
                opts.append(option)
        return opts

    def _parse_target_folders(
        self, target_folders: Iterable[str] | str | None
    ) -> tuple[list[str] | None, bool]:
        if target_folders is None:
            return None, False
        if isinstance(target_folders, str):
            if target_folders == "ALL":
                return None, True
            clean = [
                f.strip() for f in target_folders.strip("[]").split(",") if f.strip()
            ]
            return clean or None, False
        clean_list = [f.strip() for f in target_folders if str(f).strip()]
        if clean_list and clean_list[0] == "ALL":
            return None, True
        return clean_list or None, False

    def execute_process(self) -> None:
        if not self.sftp_client.open_connection():
            msg = "Unable to establish sftp connection"
            self.log.error(msg)
            stderr.print(f"[red]{msg}")
            raise ConnectionError(msg)
        try:
            metadata_targets = self._discover_metadata_targets()
            if not metadata_targets:
                stderr.print("[yellow]No metadata Excel files found in remote folders")
                self.log.warning("No metadata Excel files discovered")
            for folder, meta_files in metadata_targets.items():
                self._process_folder(folder, meta_files)
        finally:
            self.sftp_client.close_connection()

        if self.logsum.logs:
            self.parent_create_error_summary(to_excel=self.export_excel)

        self._export_report()
        self._print_summary()

    def _discover_metadata_targets(self) -> dict[str, list[str]]:
        directory_list = self.sftp_client.list_remote_folders(".", recursive=True)
        clean_dirs = sorted(
            {d.replace("./", "", 1) for d in directory_list if d and d != "."}
        )

        selected_dirs = self._select_target_directories(clean_dirs)
        metadata_targets: dict[str, list[str]] = {}
        for directory in selected_dirs:
            if not directory or directory.endswith("_tmp_processing"):
                continue
            path_parts = [part for part in directory.split("/") if part]
            if any(
                part.lower() == "invalid_samples"
                or part.lower().endswith("_invalid_samples")
                for part in path_parts
            ):
                self.log.info(
                    "Skipping %s because it points to an *_invalid_samples folder",
                    directory,
                )
                continue
            try:
                file_list = self.sftp_client.get_file_list(directory)
            except FileNotFoundError:
                self.log.warning("Folder %s not found during listing", directory)
                continue
            meta_files = [
                f
                for f in file_list
                if f.lower().endswith(".xlsx")
                and not os.path.basename(f).startswith((".~lock", "~$"))
            ]
            if meta_files:
                metadata_targets[directory] = sorted(meta_files)
        return metadata_targets

    def _select_target_directories(self, clean_dirs: list[str]) -> list[str]:
        if self.prompt_for_targets:
            choices = sorted(clean_dirs)
            if not choices:
                return []
            selected = relecov_tools.utils.prompt_checkbox(
                msg="Select the folders to validate", choices=choices
            )
            return selected
        if self.target_folders is None:
            return clean_dirs
        missing = sorted(set(self.target_folders) - set(clean_dirs))
        for folder in missing:
            self.log.warning("Target folder %s not present in remote tree", folder)
            stderr.print(f"[yellow]Target folder {folder} not present in remote tree")
        return [folder for folder in self.target_folders if folder in clean_dirs]

    def _process_folder(self, folder: str, meta_files: list[str]) -> None:
        stderr.print(f"[blue]Processing folder {folder}")
        self.log.info("Processing folder %s", folder)
        self.logsum.feed_key(key=folder)
        for remote_file in meta_files:
            file_summary = self._validate_remote_metadata(folder, remote_file)
            lab_code = folder.split("/")[0] if folder else "root"
            self._update_lab_summary(lab_code, folder, file_summary)
            self.folder_reports[folder].append(file_summary)

    def _validate_remote_metadata(
        self, folder: str, remote_file: str
    ) -> dict[str, Any]:
        file_errors: list[dict[str, Any]] = []
        file_warnings: list[dict[str, Any]] = []
        sample_count = 0
        seen_samples: set[str] = set()

        with tempfile.TemporaryDirectory() as tmp_dir:
            local_target = os.path.join(tmp_dir, os.path.basename(remote_file))
            try:
                self.sftp_client.get_from_sftp(remote_file, local_target, exist_ok=True)
            except Exception as exc:  # pragma: no cover - passthrough for runtime
                message = f"Could not download metadata file {remote_file}: {exc}"
                self._record_error(folder, file_errors, message)
                self.log.error(message)
                return {
                    "remote_file": remote_file,
                    "samples": 0,
                    "valid": False,
                    "errors": file_errors,
                    "warnings": file_warnings,
                }

            try:
                sheet_data = self._read_metadata_sheet(local_target)
            except RuntimeError as exc:
                message = f"Unable to read metadata sheet {remote_file}: {exc}"
                self._record_error(folder, file_errors, message)
                self.log.error(message)
                return {
                    "remote_file": remote_file,
                    "samples": 0,
                    "valid": False,
                    "errors": file_errors,
                    "warnings": file_warnings,
                }

        rows = sheet_data["rows"]
        header_flag = sheet_data["header_flag"]
        raw_header = sheet_data["header"]
        sample_column_raw = sheet_data["sample_column"]
        header_row_index = sheet_data["heading_row"]

        if not rows:
            message = f"Metadata sheet {remote_file} contains no data rows"
            self._record_error(folder, file_errors, message)
            self.log.error(message)
            return {
                "remote_file": remote_file,
                "samples": 0,
                "valid": False,
                "errors": file_errors,
                "warnings": file_warnings,
            }

        canonical_header = [self.mapper.canonical_label(col) for col in raw_header]
        if canonical_header and canonical_header[0] == header_flag:
            data_columns = canonical_header[1:]
        else:
            data_columns = canonical_header

        missing_columns = [
            column for column in self.metadata_lab_heading if column not in data_columns
        ]
        if missing_columns:
            message = "Missing columns in metadata header: " + ", ".join(
                sorted(missing_columns)
            )
            self._record_warning(folder, file_warnings, message)
            self.log.warning(message)

        extra_columns = [
            column for column in data_columns if column not in self.metadata_lab_heading
        ]
        if extra_columns:
            message = "Unexpected columns found: " + ", ".join(
                sorted(set(extra_columns))
            )
            self._record_warning(folder, file_warnings, message)
            self.log.warning(message)

        sample_column = self.mapper.canonical_label(sample_column_raw)

        available_schema_keys = {
            self.label_to_prop.get(column, column)
            for column in data_columns
            if self.label_to_prop.get(column, column) in self.schema_properties
        }

        for idx, row in enumerate(rows):
            row_number = header_row_index + 1 + idx
            canonical_row = self.mapper.canonicalize_row(row, header_flag)
            sample_value_raw = canonical_row.get(sample_column)
            sample_value = self.mapper.normalise_sample_id(canonical_row, sample_column)

            if not sample_value:
                message = f"Missing sequencing sample identifier at row {row_number}"
                self._record_error(
                    folder,
                    file_errors,
                    message,
                    sample=None,
                    row=row_number,
                )
            else:
                if sample_value in seen_samples:
                    message = (
                        f"Duplicated sequencing sample identifier {sample_value} "
                        f"at row {row_number}"
                    )
                    self._record_warning(
                        folder,
                        file_warnings,
                        message,
                        sample=sample_value,
                        row=row_number,
                    )
                    continue
                seen_samples.add(sample_value)
                sample_count += 1

            schema_payload = self.mapper.row_to_payload(canonical_row)
            self._validate_payload(
                payload=schema_payload,
                sample_label=sample_value if sample_value else None,
                row_number=row_number,
                available_props=available_schema_keys,
                folder=folder,
                file_errors=file_errors,
            )

        invalid_samples_details: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for err in file_errors:
            sample_label = err.get("sample")
            row = err.get("row")
            if not sample_label and row is not None:
                sample_label = f"row {row}"
            if not sample_label:
                continue
            invalid_samples_details[sample_label].append(
                {"message": err["message"], "row": row}
            )

        invalid_samples = []
        for sample_label, details in invalid_samples_details.items():
            unique_rows = sorted({d["row"] for d in details if d["row"] is not None})
            unique_messages = []
            seen_messages = set()
            for detail in details:
                msg = detail["message"]
                if msg not in seen_messages:
                    unique_messages.append(msg)
                    seen_messages.add(msg)
            invalid_samples.append(
                {
                    "sample": sample_label,
                    "rows": unique_rows,
                    "messages": unique_messages,
                    "summary": "; ".join(unique_messages),
                }
            )

        file_valid = not file_errors
        return {
            "remote_file": remote_file,
            "samples": sample_count,
            "valid": file_valid,
            "errors": file_errors,
            "warnings": file_warnings,
            "invalid_samples": invalid_samples,
        }

    def _read_metadata_sheet(self, local_path: str) -> dict[str, Any]:
        errors: list[str] = []
        for option in self.sheet_options:
            try:
                rows, heading_row = relecov_tools.utils.read_excel_file(
                    local_path,
                    option["sheet"],
                    option["header_flag"],
                    leave_empty=True,
                )
                header = list(rows[0].keys()) if rows else []
                return {
                    "rows": rows,
                    "header": header,
                    "header_flag": option["header_flag"],
                    "sample_column": option["sample_col"],
                    "heading_row": heading_row,
                }
            except Exception as exc:  # pragma: no cover - passthrough for runtime
                errors.append(f"{option['sheet']}: {exc}")
        if errors:
            raise RuntimeError("; ".join(errors))
        raise RuntimeError("No readable sheet found in metadata Excel")

    def _validate_payload(
        self,
        *,
        payload: dict[str, Any],
        sample_label: str | None,
        row_number: int | None,
        available_props: set[str],
        folder: str,
        file_errors: list[dict[str, Any]],
    ) -> None:
        if not payload:
            return
        validation_errors = list(self.validator.iter_errors(payload))
        validation_errors = relecov_tools.assets.schema_utils.custom_validators.validate_with_exceptions(
            self.schema, payload, validation_errors
        )
        if not validation_errors:
            return

        schema_props = self.schema_properties
        schema_sample = (
            payload.get(self.sample_id_property) if self.sample_id_property else None
        )
        display_label = sample_label or schema_sample
        if not display_label and row_number is not None:
            display_label = f"row {row_number}"

        for error in validation_errors:
            if error.cause:
                error.message = str(error.cause)
            if error.validator == "required":
                try:
                    missing_field = list(error.message.split("'"))[1]
                except Exception:
                    missing_field = None
                if missing_field and missing_field not in available_props:
                    continue
            error_text = self._format_validation_error(error, schema_props)
            self._record_error(
                folder,
                file_errors,
                error_text,
                sample=display_label,
                row=row_number,
            )

    def _format_validation_error(self, error, schema_props: dict[str, Any]) -> str:
        def get_property_label(prop_key: str) -> str:
            prop_def = schema_props.get(prop_key, {})
            return prop_def.get("label", prop_key)

        try:
            if error.validator == "required":
                error_field = list(error.message.split("'"))[1]
            elif error.validator == "anyOf":
                multi_errdict = {}
                for suberror in error.context:
                    error_type = suberror.validator
                    failing_field = suberror.validator_value[0]
                    sub_label = get_property_label(failing_field)
                    label_message = suberror.message.replace(failing_field, sub_label)
                    multi_errdict.setdefault(error_type, []).append(
                        (sub_label, label_message)
                    )
                error_field = ""
                multi_message = {}
                for errtype, fieldtups in multi_errdict.items():
                    failed_fields = " or ".join([t[0] for t in fieldtups])
                    clean_message = (
                        fieldtups[0][1].replace(fieldtups[0][0], "").strip("'")
                    )
                    if error_field:
                        error_field = error_field + " and"
                    error_field = error_field + failed_fields
                    multi_message[errtype] = f"{failed_fields}: {clean_message}"
                error.message = "Any of the following: " + " --- ".join(
                    multi_message.values()
                )
            elif error.absolute_path:
                error_field = str(error.absolute_path[0])
            else:
                error_field = error.validator_value
        except Exception:
            return f"Validation error: {error.message}"

        field_label = get_property_label(error_field)
        message = error.message.replace(str(error_field), field_label)
        return f"Error in column {field_label}: {message}"

    def _record_error(
        self,
        folder: str,
        file_errors: list[dict[str, Any]],
        message: str,
        sample: str | None = None,
        row: int | None = None,
    ) -> None:
        if sample:
            self.logsum.add_error(entry=message, key=folder, sample=sample)
        else:
            self.logsum.add_error(entry=message, key=folder)
        file_errors.append({"message": message, "sample": sample, "row": row})

    def _record_warning(
        self,
        folder: str,
        file_warnings: list[dict[str, Any]],
        message: str,
        sample: str | None = None,
        row: int | None = None,
    ) -> None:
        if sample:
            self.logsum.add_warning(entry=message, key=folder, sample=sample)
        else:
            self.logsum.add_warning(entry=message, key=folder)
        file_warnings.append({"message": message, "sample": sample, "row": row})

    def _update_lab_summary(
        self, lab_code: str, folder: str, file_summary: dict[str, Any]
    ) -> None:
        lab_entry = self.lab_summary[lab_code]
        lab_entry["total_samples"] += file_summary.get("samples", 0)
        lab_entry["invalid_sample_count"] += len(
            file_summary.get("invalid_samples", [])
        )
        if file_summary.get("valid"):
            lab_entry["valid_files"].append(
                remote_path := file_summary.get("remote_file")
            )
        else:
            lab_entry["invalid_files"].append(
                remote_path := file_summary.get("remote_file")
            )
        lab_entry["files"].append(
            {
                "folder": folder,
                "remote_file": remote_path,
                "samples": file_summary.get("samples", 0),
                "valid": file_summary.get("valid", False),
                "errors": file_summary.get("errors", []),
                "warnings": file_summary.get("warnings", []),
                "invalid_samples": file_summary.get("invalid_samples", []),
            }
        )

    def _export_report(self) -> None:
        report = {
            "generated_at": self.generated_at,
            "total_labs": len(self.lab_summary),
            "labs": self.lab_summary,
            "folders": self.folder_reports,
        }
        report_path = self.tag_filename(
            os.path.join(self.basemod_outdir, "metadata_precheck_report.json")
        )
        try:
            os.makedirs(os.path.dirname(report_path), exist_ok=True)
            with open(report_path, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=2, ensure_ascii=False)
            stderr.print(f"[green]Metadata precheck report saved at {report_path}")
        except OSError as exc:
            self.log.error("Could not write report %s: %s", report_path, exc)
            stderr.print(f"[red]Could not write metadata precheck report: {exc}")

    def _print_summary(self) -> None:
        if not self.lab_summary:
            return
        table = Table(title="Metadata precheck summary", show_lines=False)
        table.add_column("Lab", justify="left", no_wrap=True)
        table.add_column("Valid files", justify="right")
        table.add_column("Invalid files", justify="right")
        table.add_column("Samples", justify="right")
        table.add_column("Invalid samples", justify="right")
        table.add_column("Top issues", justify="left")

        def _collect_lab_messages(files: list[dict[str, Any]], limit: int = 3):
            messages = []
            for fdata in files:
                for inv in fdata.get("invalid_samples", []):
                    messages.extend(inv.get("messages", []))
            unique_msgs = []
            for msg in messages:
                if msg not in unique_msgs:
                    unique_msgs.append(msg)
                if len(unique_msgs) >= limit:
                    break
            if len(unique_msgs) < len(set(messages)):
                unique_msgs.append("...")
            truncated = []
            for msg in unique_msgs:
                if len(msg) > 120:
                    truncated.append(msg[:117] + "...")
                else:
                    truncated.append(msg)
            return truncated

        for lab_code, entry in sorted(self.lab_summary.items()):
            lab_messages = _collect_lab_messages(entry.get("files", []))
            summary_text = "\n".join(lab_messages) if lab_messages else "—"
            table.add_row(
                lab_code,
                str(len(entry.get("valid_files", []))),
                str(len(entry.get("invalid_files", []))),
                str(entry.get("total_samples", 0)),
                str(entry.get("invalid_sample_count", 0)),
                summary_text,
            )
        stderr.print(table)
