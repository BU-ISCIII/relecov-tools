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
                "color": "#f60606",  # Red text color
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

        cell_formater = workbook.add_format(
            {
                "border": 1,  # Apply border to every cell
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

            for row_num in range(1, len(df) + 1):
                for col_num in range(1, len(df.columns)):
                    try:
                        worksheet.write(
                            row_num,
                            col_num,
                            df.iloc[row_num - 1, col_num],
                            cell_formater,
                        )
                    except Exception as e:
                        stderr.print(
                            f"Error writing cell at row {row_num}, column {col_num}: {e}"
                        )

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
                        if row_num == 0 and col_num >= 0 and sheet == "METADATA_LAB":
                            try:
                                worksheet.write(
                                    row_num,
                                    col_num + 1,
                                    df.iloc[row_num, col_num],
                                    red_header_formater,
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


def create_condition(ws_metadata, conditions, df_filtered):
    """This function creates conditions on METADATA_LAB template sheet"""
    label_to_property = dict(zip(df_filtered["label"], df_filtered["property_id"]))
    column_map = {}

    for cell in ws_metadata[4]:
        property_id = label_to_property.get(cell.value)
        if property_id in conditions:
            column_map[property_id] = cell.column_letter

    for property_id, rules in conditions.items():
        col_letter = column_map.get(property_id)
        if col_letter:
            start_row = rules.get("header_row_idx", 5)
            end_row = rules.get("max_rows", 1000)
            validation_type = rules.get("validation_type")
            formula1 = rules.get("formula1", "").replace("{col_letter}", col_letter)
            formula2 = rules.get("formula2")
            error_message = rules.get("error_message", "Valor no permitido")
            error_title = rules.get("error_title", "Error")

            cell_range = f"{col_letter}{start_row}:{col_letter}{end_row}"

            if validation_type:
                validation = DataValidation(
                    type=validation_type,
                    operator=rules.get("operator", "between"),
                    formula1=formula1,
                    formula2=formula2,
                    showErrorMessage=True,
                )
                validation.error = error_message
                validation.errorTitle = error_title
                ws_metadata.add_data_validation(validation)
                validation.add(cell_range)
    return ws_metadata
