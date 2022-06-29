#!/usr/bin/env python

# Imports
import sys
import json
import pandas as pd

# Homogeneizer object
class Homogeneizer:
    def __init__(filename, self):
        self.filename = filename
        self.dictionary = None
        self.centre = None
        self.dataframe = None
        self.translated_dataframe = None

        pass
        return

    def detect_centre(self):
        """Detect the origin centre of the metadata, and finds the corresponding json file to use"""


        # Check name of the file attribute of the object
        # Check schema with all centres and find their json
        # associate centre and json with object
        # raise error when in doubt
        # must check on schema/institution_schemas

        path_to_institution_json = ""
        detected = []
        institution_dict = json.load(path_to_json)
        
        for key in institution_dict.keys():
            if key in self.filename:
                detected.append(institution_dict[key])
        
        if len(set(detected)) != 1:
            print("some problems arised!!!") # change this to an elegant form
            sys.exit() # maybe check which ones are being mixed or when none is being found
        else:
            print("works fine") # delete this after testing
            self.dictionary = detected[0] # first item, they are all equal
        
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

