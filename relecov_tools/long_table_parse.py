from datetime import datetime
import json
import os.path
import sys
from pathlib import Path

# from relecov_tools.rest_api import RestApi


class LongTableParse:
    def __init__(self, file_path, output_directory):
        self.file_path = file_path
        self.output_directory = output_directory

    def check_if_dir_exits(self, output_dir):
        dir_exits = False
        if not os.path.exists(output_dir):
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            dir_exits = True
        else:
            dir_exits = True
        return dir_exits

    def saving_file(self, generated_JSON, output_dir):
        if self.check_if_dir_exits(output_dir):
            date_now = datetime.now()
            file_name = "long_table_JSON_" + str(date_now) + ".txt"
            complete_path = os.path.join(output_dir, file_name)
            with open(complete_path, "xt") as file:
                file.write(generated_JSON)
        """
        if not os.path.exists(output_dir):
            Path(output_dir).mkdir(parents=True, exist_ok=True)
        elif os.path.exists(output_dir):
            date_now = datetime.now()
            file_name = "long_table_JSON_" + str(date_now) + ".txt"
            complete_path = os.path.join(output_dir, file_name)
            with open(complete_path, "xt") as file:
                file.write(generated_JSON)
        """
        """
        else:
            print("Sorry the directory we're looking for... doesn't exist")
            sys.exit(1)
        """

    def parsing_csv(self):
        try:
            with open(self.file_path) as fh:
                lines = fh.readlines()

        except FileNotFoundError:
            print("Sorry the file we're looking for... doesn't exist")
            sys.exit(1)

        else:
            list_of_dictionaries = []
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

            generated_json = json.dumps(list_of_dictionaries)

            self.saving_file(
                generated_JSON=generated_json, output_dir=self.output_directory
            )
