"""
=============================================================
HEADER
=============================================================
INSTITUTION: BU-ISCIII
AUTHOR: Luis D. Aranda Lillo
MAIL: laranda@isciii.es
VERSION: 0
CREATED: 31-5-2022
REVISED: 31-5-2022
REVISED BY: Luis Chapado
DESCRIPTION:

    Parse a csv file and save the results in a file

REQUIREMENTS:
    -Python

TO DO:

================================================================
END_OF_HEADER
================================================================
"""

from datetime import datetime
import json
import logging
import os.path
import sys
from pathlib import Path
import rich
import relecov_tools
from relecov_tools.config_json import ConfigJson


# from relecov_tools.rest_api import RestApi

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class LongTableParse:
    def __init__(self, file_path=None, output_directory=None):
        if file_path is None:
            self.file_path = relecov_tools.utils.prompt_path(
                msg="Select the csv file which contains metadata"
            )
        else:
            self.file_path = file_path

        if not os.path.exists(self.file_path):
            log.error("Metadata file %s does not exist ", self.file_path)
            stderr.print("[red] Metadata file " + self.file_path + " does not exist")
            sys.exit(1)

        if not self.file_path.endswith(".csv"):
            log.error("Metadata file %s is not a csv file ", self.file_path)
            stderr.print(
                "[red] Metadata file " + self.file_path + " must be a csv file"
            )
            sys.exit(1)

        if output_directory is None:
            self.output_directory = relecov_tools.utils.prompt_path(
                msg="Select the output folder"
            )
        else:
            self.output_directory = output_directory
        Path(self.output_directory).mkdir(parents=True, exist_ok=True)

        config_json = ConfigJson()
        self.long_table_heading = config_json.get_configuration("long_table_heading")

    def saving_file(self, generated_JSON):
        date_now = datetime.now()
        file_name = "long_table_JSON_" + str(date_now) + ".json"
        complete_path = os.path.join(self.output_directory, file_name)
        with open(complete_path, "w") as fh:
            fh.write(generated_JSON)

    def parse_a_list_of_dictionaries(self):
        try:
            with open(self.file_path) as fh:
                lines = fh.readlines()

        except FileNotFoundError:
            stderr.print("[red]The file we're looking for... doesn't exist")
            sys.exit(1)

        list_of_dictionaries = []
        headings_from_csv = lines[0].strip().split(",")
        dict_index_of_heading = {}

        for heading in self.long_table_heading:
            dict_index_of_heading[heading] = self.long_table_heading.index(heading)

        # check if the headers of both files are the same
        for heading_csv in headings_from_csv:
            if heading_csv not in dict_index_of_heading:
                stderr.print("[red]Incorrect Format, fields don't match")
                sys.exit(1)

        # check if both files contain the same number of fields
        if len(self.long_table_heading) is len(headings_from_csv):

            # check if the headers of both files have the same order
            for idx in range(len(self.long_table_heading)):
                if self.long_table_heading[idx] != headings_from_csv[idx]:
                    stderr.print(
                        "[red]Incorrect Format, fields don't have the same order"
                    )
                    sys.exit(1)

            for line in lines[1:]:
                data_dict_from_long_table = {}
                data_list = line.strip().split(",")

                data_dict_from_long_table["Chromosome"] = {"chromosome": data_list[1]}

                data_dict_from_long_table["Position"] = {
                    "pos": data_list[2],
                    "nucleotide": data_list[4],
                }

                data_dict_from_long_table["Filter"] = {"filter": data_list[5]}

                data_dict_from_long_table["VariantInSample"] = {
                    "dp": data_list[6],
                    "ref_dp": data_list[7],
                    "alt_dp": data_list[8],
                    "af": data_list[9],
                }

                data_dict_from_long_table["Gene"] = {"gene": data_list[10]}

                data_dict_from_long_table["Effect"] = {
                    "effect": data_list[11],
                    "hgvs_c": data_list[12],
                    "hgvs_p": data_list[13],
                    "hgvs_p_1_letter": data_list[14],
                }

                data_dict_from_long_table["Variant"] = {"ref": data_list[3]}

                data_dict_from_long_table["Sample"] = {"sample": data_list[0]}

                list_of_dictionaries.append(data_dict_from_long_table)

            return json.dumps(list_of_dictionaries, indent=4)
        else:
            print(
                "[red]Incorrect format, the headers do not have the same number of fields"
            )
            sys.exit(1)

    def parsing_csv(self):
        generated_json = self.parse_a_list_of_dictionaries()
        self.saving_file(generated_JSON=generated_json)
