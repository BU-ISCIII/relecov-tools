from datetime import datetime
import json
from relecov_tools.rest_api import RestApi


class LongTableParse:
    def __init__(self, file_path, output_directory):
        self.file_path = file_path
        self.output_directory = output_directory

    def parsing_csv(self):
        list_of_dictionaries = []

        with open(self.file_path) as fh:
            lines = fh.readlines()

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

        date_now = datetime.now()

        file_to_save = open(
            self.output_directory + "long_table_JSON_" + str(date_now) + ".txt", "xt"
        )
        file_to_save.write(generated_json)

        file_to_save.close()

        return generated_json
