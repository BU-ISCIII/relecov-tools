import json
import re
import logging
import sys
import rich
import os.path

from pathlib import Path
from datetime import datetime

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
#


class LongTableParse:
    """
    - parse_a_list_of_dictionaries() : returns generated_JSON
    - saving_file(generated_JSON)
    - parsing_csv() : It manages all this proccess:
       - calling first to parse_a_list_of_dictionaries() and then calling to saving_file()

    """

    def __init__(self, file_path=None, output_directory=None):
        if file_path is None:
            self.file_path = relecov_tools.utils.prompt_path(
                msg="Select the csv file which contains variant long table information"
            )
        else:
            self.file_path = file_path

        if not os.path.exists(self.file_path):
            log.error("Variant long table file %s does not exist ", self.file_path)
            stderr.print(
                f"[red] Variant long table file {self.file_path} does not exist"
            )
            sys.exit(1)

        if not self.file_path.endswith(".csv"):
            log.error("Variant long table file %s is not a csv file ", self.file_path)
            stderr.print(
                f"[red] Variant long table file {self.file_path} must be a csv file"
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

    def validate_file(self, heading):
        """Check if long table file has all mandatory fields defined in
        configuration file
        """
        for field in self.long_table_heading:
            if field not in heading:
                log.error("Incorrect format file. %s is missing", field)
                stderr.print(f"[red]Incorrect Format. {field} is missing in file")
                sys.exit(1)
        return True

    def parse_file(self):
        """This function generates a json file from the csv file entered by
        the user (long_table.csv).
        Validate the file by checking the header line
        """

        with open(self.file_path, encoding="utf-8-sig") as fh:
            lines = fh.readlines()

        stderr.print("[green]Successful checking heading fields")
        stderr.print("[blue]Parsing the input file")
        heading_index = {}
        headings_from_csv = lines[0].strip().split(",")
        for heading in self.long_table_heading:
            heading_index[heading] = headings_from_csv.index(heading)

        config = ConfigJson()
        aux_dict = config.get_configuration("long_table_parse_aux")

        samp_dict = {}
        for line in lines[1:]:
            line_s = line.strip().split(",")

            sample = line_s[heading_index["SAMPLE"]]
            if sample not in samp_dict:
                samp_dict[sample] = []

            variant_dict = {
                key: (
                    {key2: line_s[heading_index[val2]] for key2, val2 in value.items()}
                    if isinstance(value, dict)
                    else line_s[heading_index[value]]
                )
                for key, value in aux_dict.items()
            }

            if re.search("&", line_s[heading_index["GENE"]]):
                # Example
                # 215184,NC_045512.2,27886,AAACGAACATGAAATT,A,PASS,1789,1756,1552,0.87,ORF7b&ORF8,gene_fusion,n.27887_27901delAACGAACATGAAATT,.,.,ivar,B.1.1.318
                # This only occurs (for now) as gene fusion, so we just duplicate lines with same values
                genes = re.split("&", line_s[heading_index["GENE"]])
                for gene in genes:
                    variant_dict_copy = variant_dict.copy()
                    variant_dict_copy["Gene"] = gene
                    samp_dict[sample].append(variant_dict_copy)
            else:
                variant_dict["Gene"] = line_s[heading_index["GENE"]]
                samp_dict[sample].append(variant_dict)

        stderr.print("[green]Successful parsing data")
        return samp_dict

    def convert_to_json(self, samp_dict):
        j_list = []
        # Grab date from filename
        result_regex = re.search(
            "variants_long_table_(.*).csv", os.path.basename(self.file_path)
        )
        if result_regex is None:
            log.error("Analysis date not found in filename, aborting")
            stderr.print(
                "[red]Error: filename must include analysis date in format YYYYMMDD"
            )
            stderr.print("[red]e.g. variants_long_table_20220830.csv")
            sys.exit(1)
        for key, values in samp_dict.items():
            j_dict = {"sample_name": key, "analysis_date": result_regex.group(1)}
            j_dict["variants"] = values
            j_list.append(j_dict)
        return j_list

    def save_to_file(self, j_list):
        """Transform the parsed data into a json file"""
        stderr.print("[blue]Saving parsed data to file")
        date_now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_name = "long_table_" + date_now + ".json"
        file_path = os.path.join(self.output_directory, file_name)

        with open(file_path, "w") as fh:
            fh.write(json.dumps(j_list, indent=4))
        return

    def parsing_csv(self):
        """
        Function called when using the relecov-tools long-table-parse function.
        """
        stderr.print("[blue]Starting reading the input file")
        parsed_data = self.parse_file()
        j_list = self.convert_to_json(parsed_data)
        self.save_to_file(j_list)
        stderr.print("[green]Process completed")
