#!/usr/bin/env python
from jsonschema import FormatChecker, exceptions
import datetime


def validate_with_exceptions(schema, data, errors):
    """Filter validation errors based on known exceptions.

    This function filters out specific validation errors from the provided list
    of errors based on exceptions defined in the schema. It allows:
      - Numeric fields (integer/float) to contain the placeholder
        'Not Provided [SNOMED:434941000124101]'.
      - String fields with format 'date' to contain the same placeholder.

    Args:
        schema (dict): Dictionary representing the JSON schema.
        data (dict): The input data being validated.
        errors (list): List of validation errors returned by a JSON schema validator.

    Returns:
        filtered_errors (list): List of validation errors excluding known exceptions.
    """
    filtered_errors = []

    for error in errors:
        property_path = ".".join(str(p) for p in error.path)
        prop_schema = schema["properties"].get(property_path, {})

        # allow not provided for numeric types
        if (
            error.validator == "type"
            and error.instance == "Not Provided [SNOMED:434941000124101]"
            and prop_schema.get("type") in ["integer", "number"]
        ):
            continue

        # allow not provided for date format types
        if (
            error.validator == "format"
            and error.instance == "Not Provided [SNOMED:434941000124101]"
            and prop_schema.get("type") == "string"
            and prop_schema.get("format") == "date"
        ):
            continue

        # allow not evaluable for numeric types
        if (
            error.validator == "type"
            and error.instance == "Data Not Evaluable [NCIT:C186292]"
            and prop_schema.get("type") in ["integer", "number"]
        ):
            continue

        # allow not applicable for numeric types
        if (
            error.validator == "type"
            and error.instance == "Not Applicable [GENEPIO:0001619]"
            and prop_schema.get("type") in ["integer", "number"]
        ):
            continue

        # allow not applicable for date format types
        if (
            error.validator == "format"
            and error.instance == "Not Applicable [GENEPIO:0001619]"
            and prop_schema.get("type") == "string"
            and prop_schema.get("format") == "date"
        ):
            continue

        # Keep all other errors
        filtered_errors.append(error)

    return filtered_errors


def make_date_checker(start_date, end_date):
    """Create a format checker to validate dates in the given range only

    Args:
        start_date(datetime.date()): Starting date to set valid range
        end_date(datetime.date()): End date for the valid range

    Returns:
        datechecker (FormatChecker()): Object that will test if properties of
        given format are within the desired range
    """
    checker = FormatChecker()

    @checker.checks("date", raises=exceptions.FormatError)
    def date_range_checker(date):
        if isinstance(date, datetime.datetime):
            date = date.date()
        elif isinstance(date, str):
            try:
                date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
            except ValueError:
                raise exceptions.FormatError(
                    f"'{date}' is not a valid date. Check if it exists or if its format is 'YYYY-MM-DD'"
                )
        elif not isinstance(date, datetime.datetime):
            # Default data type validator will arise this error anyway ('date' is not of type string)
            return True
        if not (start_date <= date <= end_date):
            raise exceptions.FormatError(
                f"Error in date '{date}'. Please provide a date from {start_date} to {end_date}"
            )
        else:
            return True

    return checker
