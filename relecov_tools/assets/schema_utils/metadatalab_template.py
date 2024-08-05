#!/usr/bin/env python
import logging
import rich.console
import os
import json
import xlsxwriter
import pandas as pd

import relecov_tools.utils


log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)

def schema_to_flatten_json(json_data):
    """This function flattens schema when nested items are found"""
    flatten_json = {}
    for property_id, features in json_data.items():
        # TODO: might be another way to identify a nested dict property
        if features.get('type') == 'array':
            complex_properties= json_data[property_id]["items"].get('properties')
            for complex_property_id, complex_feature in complex_properties.items():
                flatten_json.update({complex_property_id : complex_feature})
        else:
            flatten_json.update({property_id : features} )
    return flatten_json

def schema_properties_to_df(json_data):
    # Initialize an empty list to store the rows of the DataFrame
    rows = []

    # Iterate over each property in the JSON data
    for property_id, property_features in json_data.items():
        # Create a dictionary to hold the property features
        row = {'property_id': property_id}
        row.update(property_features)
        
        # Append the row to the list of rows
        rows.append(row)
    
    # Create a DataFrame from the list of rows
    df = pd.DataFrame(rows)
    
    # Return the DataFrame
    return df
