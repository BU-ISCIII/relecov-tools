import os
import json
from jsonschema import ValidationError

# TODO: ADD AN USAGE DOC HERE. 

# Disable default enum validation for amr_genes
def ignore_enum(validator, enums, instance, schema):
    pass

def validate_amr_genes(validator, value, instance, schema):
    # Load Config from File
    amr_config = os.path.join(os.path.dirname(__file__), "conf", "amr_genes.config")
    with open(amr_config, 'r') as file:
        amr_json = json.load(file)

    amr_genes = instance.get("amr_genes", [])
    for gene in amr_genes:
        if gene not in amr_json.keys():
            yield ValidationError(f"Gene '{gene}' is not annotated in any group.")

# Map of custom validators
available = {
    "amr_genes_validator": validate_amr_genes
}