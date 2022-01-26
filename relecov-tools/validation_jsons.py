#!/usr/bin/env python
from jsonschema import validate
from jsonschema import Draft202012Validator
import json

data = {"sample_name":'s1', "collecting_institution" :'inst2',
    "submitting_institution":'sub',
    "sample_collection_date": '12/02/2022',
    "geo_loc_country":'Afghanistan',
    "geo_loc_state":'Western',
    "organism":'Coronaviridae',
    "isolate":'SARS-CoV-2/',
    "host_scientific_name":'Bos taurus',
    "host_disease":'Homo sapiens',
    "sequencing_instrument_model" :'COVID-19',
    "sequencing_instrument_platform": 'Illumina sequencing instrument',
    "consensus_sequence_software_name":'MinIon',
    "consensus_sequence_software_version":'Ivar'
}

schema_file = open('/home/lchapado/Projects/Proyecto_ERA/relecov-tools/schema/phage_plus_V0.json')

fage_plus_schema = json.load(schema_file)
try:
    Draft202012Validator.check_schema(fage_plus_schema)
except:
    print('Invalid schema')
    exit(1)
try:
    validate(instance=data,schema=fage_plus_schema)
except:
    print('Invalid input data')
    exit()
print('Completed')
