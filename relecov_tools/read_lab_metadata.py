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
            stderr.print("[red] Metadata file " + self.meta_file + " does not exist")
            sys.exit(1)
        if sample_list_file is None:
            self.sample_list_file = relecov_tools.utils.prompt_path(
                msg="Select the file which contains the sample information"
            )
        else:
            self.sample_list_file = sample_list_file
        if not os.path.exists(self.sample_list_file):
            log.error("Sample information file %s does not exist ", self.metadata_file)
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
                continue
        self.json_req_files = config_json.get_topic_data("lab_metadata", "lab_metadata_req_json")
        self.schema_name = self.relecov_sch_json["schema"]
        self.schema_version = self.relecov_sch_json["version"]

    def include_fixed_data(self):
        """Include fixed data that are always the same for each samples"""
        fixed_data = {
            "host_disease": "COVID-19",
            "type": "betacoronavirus",
            "tax_id": "2697049",
            "organism": "Severe acute respiratory syndrome coronavirus 2",
        }
        fixed_data.update(self.configuration.get_topic_data("lab_metadata", "fields_required_for_ENA"))
        fixed_data["schema_name"] = self.schema_name
        fixed_data["schema_version"] = self.schema_version
        return fixed_data

    def include_processed_data(self, metadata):
        """Include the data that requires to be processed to set the value.
        This values are checked aginst the available options in the schema
        """
        new_data = {}
        p_data = {
            "host_common_name": {"Human": ["host_scientific_name", "Homo Sapiens"]},
            "collecting_lab_sample_id": [
                "isolate_sample_id",
                metadata["sequencing_sample_id"],
            ],
        }

        for key, values in p_data.items():
            v_data = metadata[key]
            if isinstance(values, dict):
                if v_data in values:
                    new_data[values[v_data][0]] = values[v_data][1]
            else:
                new_data[values[0]] = values[1]
        """New fields that required processing from other field """

        return new_data

    def process_from_json(self, m_data, json_fields):
        """ """
        if isinstance(json_fields["map_field"], dict):
            # Search for the value which contains data

            for m_field in json_fields["map_field"]["any_of"]:
                try:
                    m_data[0][m_field]
                    map_field = m_field
                except KeyError:
                    continue
        else:
            map_field = json_fields["map_field"]
        json_data = json_fields["j_data"]
        if isinstance(json_data, dict):
            # import pdb; pdb.set_trace()
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
        """Add fields"""

        for key, values in self.json_req_files.items():
            stderr.print(f"[blue] Processing {key}")
            f_path = os.path.join(
                os.path.dirname(os.path.realpath(__file__)), "conf", values["file"]
            )
            values["j_data"] = relecov_tools.utils.read_json_file(f_path)
            metadata = self.process_from_json(metadata, values)
            stderr.print(f"[green] Processed {key}")
        stderr.print("[blue] Reading sample list file")
        # Include Sample informatin data from sample json file
        s_json = {}
        s_json["map_field"] = "sequencing_sample_id"
        s_json["adding_field"] = "__all__"
        s_json["j_data"] = relecov_tools.utils.read_json_file(self.sample_list_file)
        metadata = self.process_from_json(metadata, s_json)
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
            self.metadata_file, "METADATA_LAB", heading_row_number
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
                else:
                    if isinstance(row[key], float) or isinstance(row[key], int):
                        row[key] = str(int(row[key]))
                try:
                    property_row[self.label_prop_dict[key]] = row[key]
                except KeyError as e:
                    continue
                    stderr.print(f"[red] Error when reading {sample_number} {str(e)}")

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

        completed_metadata = self.adding_fields(valid_metadata_rows)

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
