#!/usr/bin/env python


def validate_with_exceptions(schema, data, errors):
    """Filter out type errors for:
    - integer/float fields containing 'Not Provided'
    - string fields with format: date containing 'Not Provided'
    - return filtered errors.
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
