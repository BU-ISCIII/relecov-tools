#!/usr/bin/env python
import logging
import rich.console
import pandas as pd


import relecov_tools.utils
from openpyxl.worksheet.datavalidation import DataValidation


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
                if features.get("type") == "array":
                    complex_properties = json_data[property_id]["items"].get(
                        "properties"
                    )
                    for (
                        complex_property_id,
                        complex_feature,
                    ) in complex_properties.items():
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
                row = {"property_id": property_id}
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
        df.to_excel(
            writer, sheet_name=sheet, startrow=1, index=have_index, header=have_header
        )

        # Get the xlsxwriter workbook and worksheet objects.
        workbook = writer.book
        worksheet = writer.sheets[sheet]

        # Set up general column width
        worksheet.set_column(0, len(df.columns), 30)

        # General header format
        header_formater = workbook.add_format(
            {
                "bold": True,
                "text_wrap": False,
                "valign": "top",
                "fg_color": "#B9DADE",  # Light blue
                "border": 1,
                "locked": True,
            }
        )

        # Custom header format for METADATA_LAB (red text starting from column 2)
        red_header_formater = workbook.add_format(
            {
                "bold": True,
                "text_wrap": False,
                "valign": "top",
                "fg_color": "#B9DADE",  # Light blue background
                "color": "#E05959",  # Red text color
                "border": 1,
                "locked": True,
            }
        )

        # First column format
        first_col_formater = workbook.add_format(
            {
                "bold": True,
                "text_wrap": False,
                "valign": "center",
                "fg_color": "#B9DADE",  # Light blue
                "border": 1,
                "locked": True,
            }
        )

        if sheet == "OVERVIEW":
            # Write the column headers with the defined format.
            for col_num, value in enumerate(df.columns.values):
                try:
                    worksheet.write(0, col_num, value, header_formater)
                except Exception as e:
                    stderr.print(f"Error writing header at column {col_num + 1}: {e}")

            # Write the first column with the defined format.
            for row_num in range(1, len(df) + 1):
                try:
                    worksheet.write(
                        row_num, 0, df.iloc[row_num - 1, 0], first_col_formater
                    )
                except Exception as e:
                    stderr.print(f"Error writing first column at row {row_num}: {e}")

        if sheet == "METADATA_LAB" or sheet == "DATA_VALIDATION":
            # Write the column headers with the defined format.
            for col_num in range(0, len(df.columns)):
                for row_num in range(0, len(df)):
                    if row_num < 4:
                        try:
                            worksheet.write(
                                row_num,
                                col_num + 1,
                                df.iloc[row_num, col_num],
                                header_formater,
                            )
                        except Exception as e:
                            stderr.print(
                                f"Error writing first column at row {row_num}: {e}"
                            )
                        if row_num == 0 and col_num >= 1 and sheet == "METADATA_LAB":
                            try:
                                worksheet.write(
                                    row_num,
                                    col_num,
                                    df.iloc[row_num, col_num],
                                    red_header_formater,  # Apply format
                                )
                            except Exception as e:
                                stderr.print(
                                    f"Error writing first row at column {col_num}: {e}"
                                )
            # Write the first column with the defined format.
            for index_num, index_val in enumerate(df.index):
                try:
                    worksheet.write(index_num, 0, index_val, first_col_formater)
                except Exception as e:
                    stderr.print(f"Error writing first column at row {row_num}: {e}")
    except Exception as e:
        stderr.print(f"Error in excel_formater: {e}")

def create_condition(ws_metadata):
    """This function creates conditions on METADATA_LAB template sheet"""
    host_age_col = None
    host_age_months_col = None
    date_columns = []
    for cell in ws_metadata[4]:
        if cell.value == "Host Age":
            host_age_col = cell.column_letter
        elif cell.value == "Host Age Months":
            host_age_months_col = cell.column_letter
        elif "Date" in str(cell.value):
            date_columns.append(cell.column_letter)
        if host_age_col:
            age_range = f"{host_age_col}5:{host_age_col}1000"
            age_validation = DataValidation(
                type="whole",
                operator="between",
                formula1="3",
                formula2="110",
                showErrorMessage=True,
            )
            age_validation.error = "El valor debe estar entre 3 y 110 años. Si es inferior a 3 debe introducir los meses en la columna [Host Age Months]"
            age_validation.errorTitle = "Valor no permitido"
            ws_metadata.add_data_validation(age_validation)
            age_validation.add(age_range)

            ws_metadata.add_data_validation(age_validation)
            age_validation.add(age_range)

        if host_age_months_col:
            age_months_range = (
                f"{host_age_months_col}5:{host_age_months_col}1000"
            )
            age_months_validation = DataValidation(
                type="whole",
                operator="between",
                formula1="0",
                formula2="35",
                showErrorMessage=True,
            )
            age_months_validation.error = (
                "El valor debe estar entre 0 y 35 meses."
            )
            age_months_validation.errorTitle = "Valor no permitido"
            ws_metadata.add_data_validation(age_months_validation)
            age_months_validation.add(age_months_range)

        for date_col in date_columns:
            date_range = f"{date_col}5:{date_col}1000"
            date_validation = DataValidation(
                type="custom",
                formula1=f'ISNUMBER(DATEVALUE(TEXT({date_col}5, "yyyy-mm-dd")))',
                showErrorMessage=True,
            )
            date_validation.error = "Ingrese la fecha en formato correcto YYYY-MM-DD (ejemplo: 2024-02-12)."
            date_validation.errorTitle = "Formato de fecha incorrecto"
            ws_metadata.add_data_validation(date_validation)
            date_validation.add(date_range)
        return ws_metadata