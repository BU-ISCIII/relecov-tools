#!/usr/bin/env python


def validate_with_exceptions(schema, data, errors):
    """Filter validation errors based on known exceptions.

    This function filters out specific validation errors from the provided list
    of errors based on exceptions defined in the schema. It allows:
      - Numeric fields (integer/float) to contain the placeholder
        'Not Provided [GENEPIO:0001668]'.
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
            and error.instance == "Not Provided [GENEPIO:0001668]"
            and prop_schema.get("type") in ["integer", "number"]
        ):
            continue

        # allow not provided for date format types
        if (
            error.validator == "format"
            and error.instance == "Not Provided [GENEPIO:0001668]"
            and prop_schema.get("type") == "string"
            and prop_schema.get("format") == "date"
        ):
            continue

        # Keep all other errors
        filtered_errors.append(error)

    return filtered_errors
