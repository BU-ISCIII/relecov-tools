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
        self.json_files = config_json.get_configuration("lab_metadata_req_json")
        self.schema_name = self.relecov_sch_json["schema"]
        self.schema_version = self.relecov_sch_json["version"]

    def get_laboratory_data(self, lab_json, geo_loc_json, lab_name):
        """Fetch the laboratory location  and return a dictionary"""
        data = {}
        if lab_name == "":
            data["geo_loc_city"] = ""
            data["geo_loc_latitude"] = ""
            data["geo_loc_longitude"] = ""
            data["geo_loc_country"] = ""
            stderr.print("[red] Empty Originating Laboratory.")
            log.error("Found empty Originating Laboratory")
            return data
        for lab in lab_json:
            if lab_name == lab["collecting_institution"]:
                for key, value in lab.items():
                    data[key] = value
                break

        for city in geo_loc_json:
            try:
                if city["geo_loc_city"] == lab["geo_loc_city"]:
                    data["geo_loc_latitude"] = city["geo_loc_latitude"]
                    data["geo_loc_longitude"] = city["geo_loc_longitude"]
                    data["geo_loc_country"] = data["geo_loc_country"]
                    break
            except KeyError as e:
                print(e)
        return data

    def include_fixed_data(self):
        """Include fixed data that are always the same for each samples"""
        fixed_data = {
            "host_disease": "COVID-19",
            "type": "betacoronavirus",
            "tax_id": "2697049",
            "organism": "Severe acute respiratory syndrome coronavirus 2",
            "common_name": "Severe acute respiratory syndrome",
            "sample_description": "Sample for surveillance",
        }
        fixed_data.update(self.configuration.get_configuration("ENA_configuration"))
        fixed_data["schema_name"] = self.schema_name
        fixed_data["schema_version"] = self.schema_version
        return fixed_data

    def include_fields_already_set(self, row_sample):
        processed_data = {}
        if row_sample["author_submitter"] == "":
            processed_data["collector_name"] = "unknown"
        else:
            processed_data["collector_name"] = row_sample["author_submitter"]
        processed_data["host_subject_id"] = row_sample["microbiology_lab_sample_id"]

        return processed_data

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
        seq_inst_plat = {
            "Illumina": [
                "Illumina iSeq 100",
                "Illumina MiSeq",
                "Illumina Miniseq",
                "Illumina NextSeq 550",
                "Illumina NextSeq 500",
                "Illumina NextSeq 1000",
                "Illumina NextSeq 2000",
                "Illumina NovaSeq 6000",
                "Illumina Miniseq",
                "Illumina Hiseq x five",
                "Illumina Hiseq x ten",
                "Illumina Hiseq x",
                "Illumina Genome analyzer",
                "Illumina Genome analyzer ii",
                "Illumina Genome analyzer iix",
                "Illumina Hiscansq",
                "Illumina Hiseq 1000",
                "Illumina Hiseq 1500",
                "Illumina Hiseq 2000",
                "Illumina Hiseq 2500",
                "Illumina Hiseq 3000",
                "Illumina Hiseq 4000",
            ],
            "Oxford Nanopore": ["MinION"],
            "Ion Torrent": ["Ion Torrent S5", "Ion Torrent PGM"],
        }
        for key, values in p_data.items():
            v_data = metadata[key]
            if isinstance(values, dict):
                if v_data in values:
                    new_data[values[v_data][0]] = values[v_data][1]
            else:
                new_data[values[0]] = values[1]
        """New fields that required processing from other field """
        for key, values in seq_inst_plat.items():
            if metadata["sequencing_instrument_model"] in values:

                new_data["sequencing_instrument_platform"] = key
                break

        return new_data

    def add_additional_data(self, metadata, conf_json_data):
        """Add the additional information that must be included in final metadata
        metadata Origin metadata
        extra_data  additional data to be included
        result_metadata    final metadata after adding the additional data
        """
        lab_data = {}
        additional_metadata = []

        samples_json = relecov_tools.utils.read_json_file(self.sample_list_file)
        for row_sample in metadata:
            """Include sample data from sample json"""
            try:
                row_sample.update(
                    samples_json[row_sample["microbiology_lab_sample_id"]]
                )
            except KeyError as e:

                stderr.print(f"[red] ERROR  fastq information not found for sample {e}")

            """ Fetch the information related to the laboratory.
                Info is stored in lab_data, to prevent to call get_laboratory_data
                each time for each sample that belongs to the same lab
            """
            if row_sample["collecting_institution"] not in lab_data:
                # from collecting_institution find city, and geo location latitude and longitude
                l_data = self.get_laboratory_data(
                    conf_json_data["laboratory_data"],
                    conf_json_data["geo_location_data"],
                    row_sample["collecting_institution"],
                )
                row_sample.update(l_data)
                lab_data[row_sample["collecting_institution"]] = l_data
            else:
                row_sample.update(lab_data[row_sample["collecting_institution"]])

            """ Fetch email and address for submitting_institution
            """
            row_sample["submitting_institution"] = row_sample[
                "submitting_institution"
            ].strip()
            if row_sample["submitting_institution"] not in lab_data:
                # Include in lab_data the submitting institution
                l_data = self.get_laboratory_data(
                    conf_json_data["laboratory_data"],
                    conf_json_data["geo_location_data"],
                    row_sample["submitting_institution"],
                )

                lab_data[row_sample["submitting_institution"]] = l_data
            row_sample["submitting_institution_email"] = lab_data[
                row_sample["submitting_institution"]
            ]["collecting_institution_email"]
            row_sample["submitting_institution_address"] = lab_data[
                row_sample["submitting_institution"]
            ]["collecting_institution_address"]

            """ Add Fixed information
            """
            row_sample.update(self.include_fixed_data())

            """ Add fields that are already in other fields
            """
            # row_sample.update(self.include_fields_already_set(row_sample))
            """Add information which requires processing
            """
            #   row_sample.update(self.include_processed_data(row_sample))

            # Add experiment_alias and run_alias
            row_sample["experiment_alias"] = str(
                row_sample["sequence_file_R1_fastq"]
                + "_"
                + row_sample["sequence_file_R2_fastq"]
            )
            row_sample["run_alias"] = str(
                row_sample["sequence_file_R1_fastq"]
                + "_"
                + row_sample["sequence_file_R2_fastq"]
            )
            additional_metadata.append(row_sample)

        return additional_metadata

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
        stderr.print("[blue] Reading configuration settings")
        conf_json_data = self.read_configuration_json_files()
        stderr.print("[blue] Reading Lab Metadata Excel File")
        valid_metadata_rows, errors = self.read_metadata_file()
        if len(errors) > 0:
            stderr.print("[red] Stopped executing because the errors found")
            sys.exit(1)
        # Continue by adding extra information
        stderr.print("[blue] Including additional information")
        completed_metadata = self.add_additional_data(
            valid_metadata_rows, conf_json_data
        )

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
