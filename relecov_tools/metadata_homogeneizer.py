#!/usr/bin/env python

# Imports
import json
import pandas as pd

# Homogeneizer object
class Homogeneizer:
    def __init__(filename, self):
        self.filename = filename
        self.centre = None
        self.dataframe = None
        self.translated_dataframe = None

        pass
        return

    def detect_centre(self):
        """Detect the origin centre of the metadata"""
        # Check name of the file attribute of the object
        # Check schema with all centres and find their json
        # associate centre and json with object
        pass
        return

    def load_dataframe(self):
        """Read the metadata file"""
        # check possible extensions
        # load with pandas

        pass
        return

    def load_dictionary(self):
        """Load the corresponding dictionary"""
        pass
        return

    def translate_dataframe(self):
        """Use the corresponding dictionary to translate the df"""
        pass
        return

    def verify_translated_dataframe(self):
        """Checks if the dataframe holds all the needed values for the relecov tools suite"""

        pass
        return