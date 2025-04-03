import os
import shutil
import sys
import logging
import rich.console
import relecov_tools.utils

log = logging.getLogger(__name__)
stderr = rich.console.Console(stderr=True, style="dim", highlight=False)

def extract_file(file, dest_folder, sample_name=None, path_key=None, log_report=None):
    dest_folder = os.path.join(dest_folder, "analysis_results")
    os.makedirs(dest_folder, exist_ok=True)
    out_filepath = os.path.join(dest_folder, os.path.basename(file))

    if os.path.isfile(out_filepath):
        return True
    if file == "Not Provided [SNOMED:434941000124101]":
        if log_report:
            log_report.update_log_report(
                "extract_file", "warning",
                f"File for {path_key} not provided in sample {sample_name}"
            )
        return False
    try:
        shutil.copy(file, out_filepath)
    except (IOError, PermissionError) as e:
        if log_report:
            log_report.update_log_report(
                "extract_file", "warning", f"Could not extract {file}: {e}"
            )
        return False
    return True


def merge_metadata(batch_filepath, batch_data):
    merged_metadata = relecov_tools.utils.read_json_file(batch_filepath)
    prev_metadata_dict = {
        item["sequencing_sample_id"]: item for item in merged_metadata
    }

    for item in batch_data:
        sample_id = item["sequencing_sample_id"]
        if sample_id in prev_metadata_dict:
            if prev_metadata_dict[sample_id] != item:
                stderr.print(
                    f"[red] Sample {sample_id} has different data in {batch_filepath} and new metadata. Can't merge."
                )
                log.error(
                    "Sample %s has different data in %s and new metadata. Can't merge.",
                    sample_id, batch_filepath
                )
                sys.exit(1)
        else:
            merged_metadata.append(item)

    relecov_tools.utils.write_json_to_file(merged_metadata, batch_filepath)
    return merged_metadata


def mapping_over_table(j_data, map_data, mapping_fields, table_name, schema_types, software_name, config_key, log_report):
    method_name = f"mapping_over_table:{software_name}.{config_key}"
    errors = []
    field_errors = {}
    field_valid = {}

    for row in j_data:
        if not row.get("sequencing_sample_id"):
            log_report.update_log_report(
                method_name,
                "warning",
                f'Sequencing_sample_id missing in {row.get("collecting_sample_id")}... Skipping...'
            )
            continue
        sample_name = row["sequencing_sample_id"]
        if sample_name in map_data:
            for field, value in mapping_fields.items():
                expected_type = schema_types.get(field, {}).get("type", "string")
                try:
                    raw_value = map_data[sample_name][value]
                    if raw_value is None or str(raw_value).strip().lower() in ["", "none"]:
                        row[field] = "Not Provided [SNOMED:434941000124101]"
                        field_errors.setdefault(sample_name, {})[field] = "Empty or None"
                        continue
                    casted_value = relecov_tools.utils.cast_value_to_schema_type(raw_value, expected_type)
                    row[field] = casted_value
                    field_valid[sample_name] = {field: value}
                except KeyError as e:
                    field_errors[sample_name] = {field: e}
                    row[field] = "Not Provided [SNOMED:434941000124101]"
        else:
            errors.append(sample_name)
            for field in mapping_fields:
                row[field] = "Not Provided [SNOMED:434941000124101]"

    # Workaround: get string table name
    if isinstance(table_name, list) and len(table_name) > 2:
        table_name = os.path.dirname(table_name[0])
    elif isinstance(table_name, list):
        table_name = table_name[0]

    if errors:
        log_report.update_log_report(
            method_name,
            "warning",
            f"{len(errors)} samples missing in '{table_name}': {', '.join(errors)}"
        )
    else:
        log_report.update_log_report(
            method_name,
            "valid",
            f"All samples were successfully found in {table_name}."
        )
    if field_errors:
        log_report.update_log_report(
            method_name,
            "warning",
            f"Missing fields in {table_name}:\n\t{field_errors}"
        )
    else:
        log_report.update_log_report(
            method_name,
            "valid",
            f"Successfully mapped fields in {', '.join(field_valid.keys())}"
        )
    log_report.print_log_report(method_name, ["valid", "warning"])
    return j_data
