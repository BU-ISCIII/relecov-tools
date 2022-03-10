from logging import exception
from tkinter import E
import logging
import rich
from rich.console import Console

# from types import NoneType
import jsonschema
from jsonschema import validate
from jsonschema import Draft202012Validator
import json
import sys

# from openpyxl import Workbook
import openpyxl
from itertools import islice
import argparse
from questionary import ValidationError

import utils
from schema_json import PhagePlusSchema

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True, style="dim", highlight=False, force_terminal=utils.rich_force_colors()
)


def check_arg(args=None):
    """
    The function is used for parsing the input parameters form the command line
    using the standard python package argparse. The package itself is handling
    the validation and the return errors messages
    Input:
        args    # Contains the arguments from the command line
    Return:
        parser.parse_args()     # The variable contains the valid parameters
    """
    parser = argparse.ArgumentParser(
        prog="validation_jsons.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Read the excel input user files and store them in the LIMS",
    )

    parser.add_argument("-v", "--version", action="version", version="%(prog)s 0.0.1")
    parser.add_argument(
        "-p",
        "--phagePlusSchema",
        required=True,
        help="file where the phage plus schema is located",
    )
    parser.add_argument(
        "-i",
        "--inputFile",
        required=True,
        help="Execl file with the user collected data",
    )
    parser.add_argument(
        "-c", "--convertedSchema", required=True, help="schema to be mapped"
    )
    return parser.parse_args()


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("Usage: validation_jsons.py [ARGUMENTS] ")
        print("Try  validation_jsons.py --help for more information.")
        exit(2)
    arguments = check_arg(sys.argv[1:])
    if not utils.file_exists(arguments.phagePlusSchema):
        print("phage plus schema file does not exist\n")
        exit(2)
    if not utils.file_exists(arguments.inputFile):
        print("excel file does not exist\n")
        exit(2)
    if not utils.file_exists(arguments.convertedSchema):
        print("file for converting schema does not exist\n")
        exit(2)
    # schema_file = open('/home/lchapado/Projects/Proyecto_ERA/relecov-tools/schema/phage_plus_V0.json')
    schema_file = open(arguments.phagePlusSchema)

    json_phage_plus_schema = json.load(schema_file)
    map_file = open("schema/mapping_file.json")
    mapping_file = json.load(map_file)
    try:
        Draft202012Validator.check_schema(json_phage_plus_schema)
    except:
        print("Invalid schema")
        exit(1)
    phage_plus_schema = PhagePlusSchema(json_phage_plus_schema)

    sample_list = []
    wb_file = openpyxl.load_workbook(arguments.inputFile, data_only=True)
    ws_metadata_lab = wb_file["METADATA_LAB"]
    heading = []
    for cell in ws_metadata_lab[1]:

        heading.append(cell.value)

    for i in range(len(heading)):
        if heading[i] in list(mapping_file.keys()):
            index = list(mapping_file).index(heading[i])
            heading[index] = list(mapping_file.values())[index]

    for row in islice(ws_metadata_lab.values, 1, ws_metadata_lab.max_row):
        sample_data_row = {}
        for idx in range(len(heading)):

            if "date" in heading[idx] or "Date" in heading[idx]:
                try:
                    sample_data_row[heading[idx]] = row[idx].strftime("%d/%m/%Y")
                except AttributeError:
                    pass
            else:
                sample_data_row[heading[idx]] = row[idx]

        try:
            import pdb

            pdb.set_trace()

            validate(
                instance=sample_data_row,
                schema=json_phage_plus_schema,
            )
            print("Success")
        except jsonschema.ValidationError as e:

            print(e)

            # log.error(e)
            continue

    # sample_list.append(PhagePlusData(sample_data_row, phage_plus_schema))
    # create the information mapped to the new schema

    print("Completed")
