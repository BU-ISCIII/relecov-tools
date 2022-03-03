import json
import openpyxl


def validateJsonFile(jsonFile):
    try:
        json.load(jsonFile)

    except ValueError as err:
        print(err)
        return False
    return True


with open("schema/phage_plus_V0.json") as f:
    print("Given JSON file is valid:", validateJsonFile)


sample_list = []
wb_file = openpyxl.load_workbook("example_data/dummy_data.xlsx", data_only=True)
ws_metadata_lab = wb_file["METADATA_LAB"]
heading = []
print(ws_metadata_lab)
