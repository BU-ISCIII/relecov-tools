#!/usr/bin/env python
import logging
import rich.console
import os
import json
import xlsxwriter
import pandas as pd
import xlsxwriter


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
    try:
        flatten_json = {}
        for property_id, features in json_data.items():
            try:
                if features.get('type') == 'array':
                    complex_properties = json_data[property_id]["items"].get('properties')
                    for complex_property_id, complex_feature in complex_properties.items():
                        flatten_json.update({complex_property_id: complex_feature})
                else:
                    flatten_json.update({property_id: features})
            except Exception as e:
                stderr.print(f"[red]Error processing property {property_id}: {e}")
        return flatten_json
    except Exception as e:
        stderr.print(f"[red]Error in schema_to_flatten_json: {e}")
        return None

def schema_properties_to_df(json_data):
    try:
        # Initialize an empty list to store the rows of the DataFrame
        rows = []

        # Iterate over each property in the JSON data
        for property_id, property_features in json_data.items():
            try:
                # Create a dictionary to hold the property features
                row = {'property_id': property_id}
                row.update(property_features)

                # Append the row to the list of rows
                rows.append(row)
            except Exception as e:
                stderr.print(f"[red]Error processing property {property_id}: {e}")

        # Create a DataFrame from the list of rows
        df = pd.DataFrame(rows)

        # Return the DataFrame
        return df
    except Exception as e:
        stderr.print(f"[red]Error in schema_properties_to_df: {e}")
        return None


def excel_formater(df, writer, sheet, out_file, have_index=True, have_header=True):
    try:
        
        # Write the DataFrame to the specified sheet
        df.to_excel(writer, sheet_name=sheet, startrow=1, index=have_index, header=have_header)
        
        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets[sheet]
        
        # setup excel formaters
        worksheet.set_column(0, len(df.columns), 30)
        header_formater = workbook.add_format({
                'bold': True,
                'text_wrap': False,
                'valign': 'top',
                'fg_color': '#ADD8E6',
                'border': 1
            })
        first_col_formater =  workbook.add_format({
                'bold': True,
                'text_wrap': False,
                'valign': 'center',
                'fg_color': '#ADD8E6',
                'border': 1
            })
        if sheet == 'OVERVIEW':
            # Write the column headers with the defined format.
            for col_num, value in enumerate(df.columns.values):
                try:
                    worksheet.write(0, col_num + 1, value, header_formater)
                except Exception as e:
                    stderr.print(f"Error writing header at column {col_num + 1}: {e}")
        
            # Write the first column with the defined format.
            for row_num in range(1, len(df) + 1):
                try:
                    worksheet.write(row_num, 0, df.iloc[row_num - 1, 0], first_col_formater)
                except Exception as e:
                    stderr.print(f"Error writing first column at row {row_num}: {e}")
        
        if sheet == 'METADATA_LAB':
            # Write the column headers with the defined format.
            for col_num, value in enumerate(df.columns.values):
                for row_num in range(0, len(df)):
                    try:
                        worksheet.write(row_num + 1, col_num + 1, df.iloc[row_num-1, col_num], header_formater)
                    except Exception as e:
                        stderr.print(f"Error writing header at column {col_num + 1}: {e}")

            # Write the first column with the defined format.
            for row_num in range(1, len(df)+1):
                try:
                    worksheet.write(row_num, 0, df.index[row_num-1], first_col_formater)
                except Exception as e:
                    stderr.print(f"Error writing first column at row {row_num}: {e}")
        
    except Exception as e:
        stderr.print(f"Error in excel_formater: {e}")
