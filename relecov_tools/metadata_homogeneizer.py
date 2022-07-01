#!/usr/bin/env python
# Imports
import os
import sys
import logging
import pandas as pd
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


def check_extension(instring):
    """Given a file as a string and a list of possible extensions,
    returns the type of file extension can be found in the file"""

    # hard-coded extensions
    extensions = {
        "excel": [".xlsx", ".xls", ".xlsm", ".xlsb"],
        "odf": [".odf"],
        "csv": [".csv"],
        "tsv": [".tsv"],
    }

    for extension, termination_list in extensions.items():
        for termination in termination_list:
            if instring.endswith(termination):
                return extension


def identify_load_dataframe(filename):
    """Detect possible extensions for the metadata file
    Open it into a dataframe"""

    if check_extension(filename) == "excel":
        df = pd.read_excel(filename, header=0)

    elif check_extension(filename) == "odf":
        # Needs a special package
        df = pd.read_excel(filename, engine="odf", header=0)

    elif check_extension(filename) == "csv":
        df = pd.read_csv(filename, sep=",", header=0)

    elif check_extension(filename) == "tsv":
        df = pd.read_csv(filename, sep="\t", header=0)

    # not real sure how to do this
    elif check_extension(filename) == "json":
        pass
        # config_json = ConfigJson(filename="")

    else:
        print(f"The extension of the file '{filename}' could not be identified.")
        return None

    # remove spaces before and after
    df.columns = df.columns.str.strip()

    return df


class Homogeneizer:
    """Homogeneizer object"""

    def __init__(self, filename):
        self.filename = filename
        self.dictionary_path = None
        self.dicionary = None
        self.centre = None
        self.dataframe = None

        config_json = ConfigJson()
        self.header = config_json.get_configuration("new_table_headers")
        self.translated_dataframe = pd.DataFrame(columns=self.header)
        return

    def associate_dict(self):
        """Detect the origin centre of the metadata, and finds the corresponding json file to use"""

        # Check name of the file attribute of the object
        # Check schema with all centres and find their json
        # Associate centre and json with object
        # Raise error when in doubt
        # Must check on schema/institution_schemas

        try:
            config_json = ConfigJson(
                json_file=os.path.join(
                    os.path.dirname(__file__), "schema", "institution_to_schema.json"
                )
            )
            institution_dict = config_json.json_data
            detected = {}

            for key in institution_dict.keys():
                # cap insensitive
                if key.lower() in self.filename.split("/")[-1].lower():
                    detected[key] = institution_dict[key]

            if len(set(detected.values())) == 0:
                log.error(f"No institution pattern could be found in the '{self.filename}' filename given.")
                stderr.print(
                    f"[red]No institution pattern could be found in the '{self.filename}' filename given."
                )
                sys.exit(1)

            elif len(set(detected.values())) > 1:
                repeated = ", ".join(list(set(detected.values)))
                log.error(f"The following matches were identified in the '{self.filename}' filename given: {repeated    }'")
                stderr.print(
                    f"[red]The following matches were identified in the '{self.filename}' filename given: {repeated}"
                )
                sys.exit(1)

            else:
                self.dictionary_path = detected[0]  # first item, they are all equal
                stderr.print(
                    f"[green]JSON file found successfully: {self.dictionary_path}"
                )

        except FileNotFoundError:
            log.error(
                "JSON file relating institutions and their JSON file could not be found or does not exist."
            )
            stderr.print(f"[red]JSON file relating institutions and their JSON file could not be found or does not exist.")
            sys.exit(1)

        return

    def load_dataframe(self):
        """Detect possible extensions for the metadata file and
        open it into a dataframe"""
        self.dataframe = identify_load_dataframe(self.filename)
        return

    def load_dictionary(self):
        """Load the corresponding dictionary"""

        # To Do: replace string with local file system for testing
        try:
            config_json = ConfigJson(
                json_file=os.path.join(
                    os.path.dirname(__file__), "schema", self.dictionary_path
                )
            )
            self.dictionary = config_json.json_data
        except FileNotFoundError:
            log.error(
                f"JSON file {self.dictionary_path} could not be found or does not exist in the schema directory."
            )
            stderr.print(f"[red]JSON file {self.dictionary_path} could not be found or does not exist in the schema directory.")
            sys.exit(1)
        return

    def translate_dataframe(self):
        """Use the corresponding dictionary to translate the df"""
        # if dictionary is "none" or similar, do nothing

        try:  # not sure if a try is the best here, gotta check
            for key, value in self.dictionary["equivalence"].items():
                if len(value) == 0:
                    log.error(
                        f"Found empty equivalence in the '{self.dictionary_path}' schema: '{key}'"
                    )
                    stderr.print(
                        f"[red]Found empty equivalence in the '{self.dictionary_path}' schema: '{key}'"
                    )
                    sys.exit(1)
                elif value in self.dataframe.columns:
                    self.translated_dataframe[key] = self.dataframe[value.strip()]
                else:
                    log.error(
                        f"Column '{value}' indicated in the '{self.dictionary_path}' schema could not be found in the input dataframe."
                    )
                    stderr.print(
                        f"[red]Column '{value}' indicated in the '{self.dictionary_path}' schema could not be found in the input dataframe."
                    )
                    sys.exit(1)

            for key, value in self.dictionary["constants"].items():
                self.translated_dataframe[key] = value

        except Exception:
            log.error(
                "Found empty equivalence in the '{self.dictionary_path}' schema: '{key}'"
            )

        # Nightmare
        if len(self.dictionary["outer"]) == 0:
            pass
        else:
            for key, value in self.dictionary["outer"].items():
                value["filename"]

        return

    def verify_translated_dataframe(self):
        """Checks if the dataframe holds all the needed values for the relecov tools suite"""

        if self.dataframe.shape[0] != self.translated_dataframe.shape[0]:
            log.error("Different number of rows after translation")
            sys.exit(1)
        else:
            stderr.print(f"[green]Number of rows: OK")

        # search for missing values
        missing_values = list(set(self.header) - set(self.translated_dataframe.columns))
        if len(missing_values) > 0:
            msg = ", ".join(missing_values)
            log.error(f"Found the following missing values during translated table validation: {msg}")
            stderr.print(f"[red]Found the following missing values during translated table validation: {msg}")
        else:
            stderr.print(f"[green]No missing values in the translated table")
        # search for extra values
        extra_values = list(set(self.translated_dataframe.columns) - set(self.header))
        if len(extra_values) > 0:
            msg = ", ".join(extra_values)
            log.error(f"Found the following extra values during translated table validation: {msg}")
            stderr.print(f"[red]Found the following extra values during translated table validation: {msg}")
        else:
            stderr.print(f"[green]No extra values in the translated values")
        return

    def export_translated_dataframe(self):
        # expected only one dot per file
        filename, extension = self.filename.split(".")
        self.translated_dataframe.to_excel(
            excel_writer=filename + "_modified." + extension
        )
        return
