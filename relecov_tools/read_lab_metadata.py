#!/usr/bin/env python
import json
import logging
import rich.console
import os
import sys
import re
import relecov_tools.utils
from relecov_tools.config_json import ConfigJson
import relecov_tools.json_schema

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)
#


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
            self.sample_list_file = relecov_tools.utils.prompt_path(
                msg="Select the file which contains the sample information"
            )
        else:
            self.sample_list_file = sample_list_file

        if not os.path.exists(self.sample_list_file):
            log.error(
                "Sample information file %s does not exist ", self.sample_list_file
            )
            stderr.print(
                "[red] Sample information " + self.sample_list_file + " does not exist"
            )
            sys.exit(1)

        if output_folder is None:
            self.output_folder = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_folder = output_folder

        config_json = ConfigJson()
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
                    "[orange] Property " + prop + " does not have 'label' attribute"
                )
                continue

        self.json_req_files = config_json.get_topic_data(
            "lab_metadata", "lab_metadata_req_json"
        )
        self.schema_name = self.relecov_sch_json["title"]
        self.schema_version = self.relecov_sch_json["version"]

    def adding_fixed_fields(self, m_data):
        """Include fixed data that are always the same for each samples"""
        p_data = self.configuration.get_topic_data("lab_metadata", "fixed_fields")
        for idx in range(len(m_data)):
            for key, value in p_data.items():
                m_data[idx][key] = value
            m_data[idx]["schema_name"] = self.schema_name
            m_data[idx]["schema_version"] = self.schema_version
        return m_data

    def adding_copy_from_other_field(self, m_data):
        """Add in a new field the information that is already set in another
        field.
        """
        p_data = self.configuration.get_topic_data(
            "lab_metadata", "required_copy_from_other_field"
        )
        for idx in range(len(m_data)):
            for key, value in p_data.items():
                m_data[idx][key] = m_data[idx][value]
        return m_data

    def adding_post_processing(self, m_data):
        """Add the fields that requires to set based on the existing value
        in other field
        """
        p_data = self.configuration.get_topic_data(
            "lab_metadata", "required_post_processing"
        )
        for idx in range(len(m_data)):
            for key, p_values in p_data.items():
                value = m_data[idx][key]
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
        """Read the schema to get the properties enum and for those fields
        which has an enum property value then replace the value for the one it
        is defined in the schema
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

        for idx in range(len(m_data)):
            for key, e_values in enum_dict.items():
                if key in m_data[idx]:
                    if m_data[idx][key] in e_values:
                        m_data[idx][key] = e_values[m_data[idx][key]]
                    else:
                        continue

        return m_data

    def process_from_json(self, m_data, json_fields):
        """ """
        map_field = json_fields["map_field"]

        json_data = json_fields["j_data"]
        if isinstance(json_data, dict):
            for idx in range(len(m_data)):
                m_data[idx].update(json_data[m_data[idx][map_field]])
        elif isinstance(json_data, list):
            # to avoid searching for data for each row, for the first time searchs
            # it is stored temporary.
            tmp_data = {}
            for idx in range(len(m_data)):
                if m_data[idx][map_field] not in tmp_data:
                    for item in json_data:
                        if m_data[idx][map_field] == item[map_field]:
                            if json_fields["adding_fields"] == "__all__":
                                m_data[idx].update(item)
                                tmp_data[m_data[idx][map_field]] = item
                            else:
                                tmp_data[m_data[idx][map_field]] = {}
                                for field in json_fields["adding_fields"]:
                                    m_data[idx][field] = item[field]
                                    tmp_data[m_data[idx][map_field]][field] = item[
                                        field
                                    ]
                            break
                else:
                    m_data[idx].update(tmp_data[m_data[idx][map_field]])
        return m_data

    def adding_fields(self, metadata):
        """Add information that requires to handle json files to include in
        the  fields"""

        for key, values in self.json_req_files.items():
            stderr.print(f"[blue] Processing {key}")
            f_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "conf", values["file"]
            )
            values["j_data"] = relecov_tools.utils.read_json_file(f_path)
            metadata = self.process_from_json(metadata, values)
            stderr.print(f"[green] Processed {key}")

        # Because sample data file is comming in an input parameter it cannot
        # be inside the configuration json file.
        # Include Sample informatin data from sample json file
        stderr.print("[blue] Processing sample data file")
        s_json = {}
        s_json["map_field"] = "sequencing_sample_id"
        s_json["adding_field"] = "__all__"
        s_json["j_data"] = relecov_tools.utils.read_json_file(self.sample_list_file)
        metadata = self.process_from_json(metadata, s_json)
        stderr.print("[green] Processed sample data file.")
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
        """Read the input metadata file from row 4, changes the metadata heading
        with their property name values defined in schema.
        Convert the date colunms value to the dd/mm/yyyy format.
        Return list of dict with data, and errors
        """
        heading_row_number = 4
        ws_metadata_lab = relecov_tools.utils.read_excel_file(
            self.metadata_file, "METADATA_LAB", heading_row_number, False
        )
        metadata_values = []
        errors = {}
        row_number = heading_row_number
        for row in ws_metadata_lab:
            row_number += 1
            property_row = {}
            try:
                sample_number = row["Sample ID given for sequencing"]
            except KeyError:
                log.error(
                    "Sample ID given for sequencing not found in row  %s", row_number
                )
                stderr.print(
                    f"[red] Sample ID given for sequencing not found in row {row_number}"
                )
                continue
            for key in row.keys():
                # skip the first column of the Metadata lab file
                if "Campo" in key:
                    continue
                if "date" in key.lower():
                    if row[key] is not None:
                        try:
                            row[key] = row[key].strftime("%Y-%m-%d")
                        except AttributeError:
                            # check if date is in string format
                            str_date = re.search(r"(\d{4}-\d{2}-\d{2}).*", row[key])
                            if str_date:
                                row[key] = str_date.group(1)
                            else:
                                if sample_number not in errors:
                                    errors[sample_number] = {}
                                errors[sample_number][key] = "Invalid date format"
                                log.error(
                                    "Invalid date format in sample %s", row_number
                                )
                                stderr.print(
                                    f"[red] Invalid date format in sample {sample_number},  {key}"
                                )
                elif "sample id" in key.lower():
                    if isinstance(row[key], float) or isinstance(row[key], int):
                        row[key] = str(int(row[key]))

                else:
                    if isinstance(row[key], float) or isinstance(row[key], int):
                        row[key] = str(row[key])
                try:
                    property_row[self.label_prop_dict[key]] = row[key]
                except KeyError as e:
                    log.error("Error when mapping the label %s", e)
                    stderr.print(f"[red] Error when mapping the label {str(e)}")
                    continue

            metadata_values.append(property_row)
        return metadata_values, errors

    def create_metadata_json(self):
        # stderr.print("[blue] Reading configuration settings")
        # conf_json_data = self.read_configuration_json_files()
        stderr.print("[blue] Reading Lab Metadata Excel File")
        valid_metadata_rows, errors = self.read_metadata_file()
        if len(errors) > 0:
            stderr.print("[red] Stopped executing because the errors found")
            sys.exit(1)
        # Continue by adding extra information
        stderr.print("[blue] Including additional information")

        extended_metadata = self.adding_fields(valid_metadata_rows)
        stderr.print("[blue] Including post processing information")
        extended_metadata = self.adding_post_processing(extended_metadata)
        extended_metadata = self.adding_copy_from_other_field(extended_metadata)
        extended_metadata = self.adding_fixed_fields(extended_metadata)
        completed_metadata = self.adding_ontology_to_enum(extended_metadata)
        file_name = (
            "processed_"
            + os.path.splitext(os.path.basename(self.metadata_file))[0]
            + ".json"
        )
        stderr.print("[blue] Writting output json file")
        os.makedirs(self.output_folder, exist_ok=True)
        file_path = os.path.join(self.output_folder, file_name)
        relecov_tools.utils.write_json_fo_file(completed_metadata, file_path)
        return True
