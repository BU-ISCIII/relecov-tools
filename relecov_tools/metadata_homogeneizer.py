#!/usr/bin/env python
import os
import sys
import logging
import rich.console

import relecov_tools.utils
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class MetadataHomogeneizer:
    """MetadataHomogeneizer object"""

    def __init__(self, institution=None, directory=None, output_dir=None):
        # open config
        self.config_json = ConfigJson()
        # read heading from config
        self.heading = self.config_json.get_topic_data(
            "read_lab_metadata", "metadata_lab_heading"
        )

        # handle institution
        if institution is None:
            self.institution = relecov_tools.utils.prompt_selection(
                msg="Select the available mapping institution",
                choices=["isciii", "hugtip", "hunsc-iter"],
            ).upper()
        else:
            self.institution = institution.upper()

        mapping_json_file = os.path.join(
            os.path.dirname(__file__),
            "schema",
            "institution_schemas",
            self.config_json.get_topic_data("generic", "institution_mapping_file")[
                self.institution
            ],
        )

        self.mapping_json_data = relecov_tools.utils.read_json_file(mapping_json_file)

        if directory is None:
            directory = relecov_tools.utils.prompt_path(
                msg="Select the directory which contains additional files for metadata"
            )
        if not os.path.exists(directory):
            log.error("Folder for additional files %s does not exist ", directory)
            stderr.print(
                "[red] Folder for additional files " + directory + " does not exist"
            )
            sys.exit(1)

        try:
            lab_metadata = self.mapping_json_data["required_files"]["metadata_file"][
                "file_name"
            ]
        except KeyError:
            log.error("Metadata File is not defined in schema")
            stderr.print("[red] Metadata File is not defined in schema")
            sys.exit(1)

        metadata_path = os.path.join(directory, lab_metadata)

        if not os.path.isfile(metadata_path):
            log.error("Metadata File %s does not exists", metadata_path)
            stderr.print("[red] Metadata File " + metadata_path + "does not exists")
            sys.exit(1)
        self.lab_metadata = self.mapping_json_data["required_files"]["metadata_file"]
        self.lab_metadata["file_name"] = metadata_path

        self.additional_files = []

        if len(self.mapping_json_data["required_files"]) > 1:
            for key, values in self.mapping_json_data["required_files"].items():
                if key == "metadata_file":
                    continue
                if values["file_name"] == "":
                    self.additional_files.append(values)
                    continue
                f_path = os.path.join(directory, values["file_name"])
                if not os.path.isfile(f_path):
                    log.error("Additional file %s does not exist ", f_path)
                    stderr.print("[red] Additional file " + f_path + " does not exist")
                    sys.exit(1)
                values["file_name"] = f_path
                self.additional_files.append(values)

        # Check if python file is defined
        function_file = self.mapping_json_data["python_file"]

        if function_file == "":
            self.function_file = None
        else:
            self.function_file = os.path.join(
                os.path.dirname(__file__), "institution_scripts", function_file
            )
            if not os.path.isfile(self.function_file):
                log.error("File with functions %s does not exist ", self.function_file)
                stderr.print(
                    "[red] File with functions "
                    + self.function_file
                    + " does not exist"
                )
                sys.exit(1)
        if output_dir is None:
            self.output_dir = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_dir = output_dir
        self.processed_metadata = False

    def mapping_metadata(self, ws_data):
        map_fields = self.mapping_json_data["required_files"]["metadata_file"][
            "mapped_fields"
        ]
        map_data = []
        for row in ws_data:
            row_data = {}
            for dest_map, orig_map in map_fields.items():
                row_data[dest_map] = row[orig_map]
            map_data.append(row_data)

        return map_data

    def add_fixed_fields(self, mapped_data):
        add_data = [self.heading]
        fixed_fields = self.mapping_json_data["fixed_fields"]
        for row in mapped_data:
            new_row_data = []
            for field in self.heading:
                if field in row:
                    data = row[field]
                elif field in fixed_fields:
                    data = fixed_fields[field]
                else:
                    data = ""
                new_row_data.append(data)
            add_data.append(new_row_data)
        return add_data

    def handling_files(self, file_data, data_to_add):
        """Added information based on the required file configuration.
        The first time this function is called is for mapping the laboratory
        metadata to ISCIII. For this time mapping_metadata method is used.
        and return the list that is going to be used later for adding/modifing
        information
        """
        if file_data["file_name"] != "":
            f_name = file_data["file_name"]
            stderr.print("[blue] Starting processing file " + f_name)
            if f_name.endswith(".json"):
                data = relecov_tools.utils.read_json_file(f_name)
            elif f_name.endswith(".tsv"):
                data = relecov_tools.utils.read_csv_file_return_dict(f_name, "\t")
            elif f_name.endswith(".csv"):
                data = relecov_tools.utils.read_csv_file_return_dict(f_name, ",")
            elif f_name.endswith(".xlsx"):
                header_flag = self.metadata_processing.get("header_flag")
                data = relecov_tools.utils.read_excel_file(
                    f_name, "Sheet", header_flag, leave_empty=True
                )
            else:
                log.error("Additional file extension %s is not supported ", f_name)
                stderr.print(
                    "[red] Additional file extension " + f_name + " is not supported"
                )
                sys.exit(1)
        else:
            data = ""
        if not self.processed_metadata:
            self.processed_metadata = True
            return self.mapping_metadata(data)

        if file_data["function"] == "None":
            mapping_idx = self.heading.index(file_data["mapped_key"])
            for row in data_to_add[1:]:
                # new_row_data = []
                s_value = str(row[mapping_idx])
                try:
                    item_data = data[s_value]
                except KeyError:
                    log.info(
                        "Additional file %s does not have the information for %s ",
                        f_name,
                        s_value,
                    )
                    stderr.print(
                        "[yellow] Additional file "
                        + f_name
                        + " does not have information for "
                        + str(s_value)
                    )
                    continue
                    # sys.exit(1)
                for m_field, f_field in file_data["mapped_fields"].items():
                    try:
                        meta_idx = self.heading.index(m_field)
                    except ValueError as e:
                        log.error("Field %s does not exist in Metadata ", e)
                        stderr.print(f"[red] Field {e} does not exist")
                        sys.exit(1)
                    row[meta_idx] = item_data[f_field]

        else:
            func_name = file_data["function"]
            stderr.print("[yellow] Start processing function " + func_name)
            exec(
                "from relecov_tools.institution_scripts."
                + self.institution
                + " import "
                + func_name
            )
            # somehow this overrides additional_data working as a pointer
            eval(
                func_name
                + "(data_to_add, data, file_data['mapped_fields'], self.heading)"
            )

        stderr.print("[green] Succesful processing of additional file ")
        return data_to_add

    def converting_metadata(self):
        stderr.print("[blue] Reading the metadata file to convert")

        # metadata_file contains the primary source of information. First we map it.
        mapped_data = self.handling_files(self.lab_metadata, "")
        stderr.print("[green] Successful conversion mapping to ISCIII metadata")
        stderr.print("[blue] Adding fixed information")

        # Then we add the fixed data
        additional_data = self.add_fixed_fields(mapped_data)

        # Fetch the additional files and include the information in metadata
        stderr.print("[blue] reading and mapping de information that are in files")
        for additional_file in self.additional_files:
            additional_data = self.handling_files(additional_file, additional_data)

        # write to excel mapped data
        f_name = os.path.join(self.output_dir, "converted_metadata_lab.xlsx")
        stderr.print("[blue] Dumping information to excel")
        post_process = {"insert_rows": 3, "insert_cols": 1}
        relecov_tools.utils.write_to_excel_file(
            additional_data, f_name, "METADATA_LAB", post_process
        )
        stderr.print("[green] Complete process for mapping to ISCIII metadata")
        return
