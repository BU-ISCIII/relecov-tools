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
                f"[red] Variant long table file {self.file_path}  does not exist"
            )
            sys.exit(1)

        if not self.file_path.endswith(".csv"):
            log.error("Variant long table file %s is not a csv file ", self.file_path)
            stderr.print(
                f"[red] Variant long table file  {self.file_path}  must be a csv file"
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
        self.long_table_heading = config_json.get_topic_data(
            "headings", "long_table_heading"
        )

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
        """_summary_
        This function generates a json file from the csv file entered by the user
        (long_table.csv).
        - Checks if expected headers match with file headers
        Returns:
            dictionary with key as sample and value the list of variants
        """
        with open(self.file_path) as fh:
            lines = fh.readlines()

        self.validate_file(lines[0].strip().split(","))
        stderr.print("[green]Successful checking heading fields")
        stderr.print("[blue]Parsing the input file")
        heading_index = {}
        headings_from_csv = lines[0].strip().split(",")
        for heading in self.long_table_heading:
            heading_index[heading] = headings_from_csv.index(heading)

        samp_dict = {}
        for line in lines[1:]:
            line_s = line.strip().split(",")

            sample = line_s[heading_index["SAMPLE"]]
            if sample not in samp_dict:
                samp_dict[sample] = []
            variant_dict = {}
            variant_dict["Chromosome"] = line_s[heading_index["CHROM"]]

            variant_dict["Variant"] = {
                "pos": line_s[heading_index["POS"]],
                "alt": line_s[heading_index["ALT"]],
                "ref": line_s[heading_index["REF"]],
            }

            variant_dict["Filter"] = line_s[heading_index["FILTER"]]

            variant_dict["VariantInSample"] = {
                "dp": line_s[heading_index["DP"]],
                "ref_dp": line_s[heading_index["REF_DP"]],
                "alt_dp": line_s[heading_index["ALT_DP"]],
                "af": line_s[heading_index["AF"]],
            }

            variant_dict["Gene"] = line_s[heading_index["GENE"]]

            variant_dict["Effect"] = line_s[heading_index["EFFECT"]]
            variant_dict["VariantAnnotation"] = {
                "hgvs_c": line_s[heading_index["HGVS_C"]],
                "hgvs_p": line_s[heading_index["HGVS_P"]],
                "hgvs_p_1_letter": line_s[heading_index["HGVS_P_1LETTER"]],
            }

            samp_dict[sample].append(variant_dict)
        stderr.print("[green]Successful parsing data")
        return samp_dict

    def convert_to_json(self, samp_dict):
        """ """
        j_list = []
        for key, values in samp_dict.items():
            j_dict = {"sample_name": key}
            j_dict["variants"] = values
            j_list.append(j_dict)
        return j_list

    def saving_file(self, j_list):
        """
        Transform the p0arsed data into a jsonf file, naming as
        "long_table_" + "current date" + ".json"
        """
        stderr.print("[blue]Saving parsed data to file")
        date_now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
        file_name = "long_table_" + date_now + ".json"
        file_path = os.path.join(self.output_directory, file_name)

        with open(file_path, "w") as fh:
            fh.write(json.dumps(j_list, indent=4))
        return

    def parsing_csv(self):
        """
        function called when using the relecov-tools long-table-parse function.
        """
        stderr.print("[blue]Starting reading the input file")
        parsed_data = self.parse_file()
        j_list = self.convert_to_json(parsed_data)
        self.saving_file(j_list)
        stderr.print("[green]Process completed")
