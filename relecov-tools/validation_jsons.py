#!/usr/bin/env python
from jsonschema import validate
from jsonschema import Draft202012Validator
import json
#import pandas as pd
#import excel2json-3

class PhagePlusSchema :
    def __init__ (self, schema):
        self.schema = schema

    def get_gontology(self,property_item):
        '''
        Description:
            The function return the geontology value for a property in the schema
        Input:
            property_item    # property name to fetch its geontology
        Return:
            Return ontology value or None
        '''
        try:
             return self.schema['properties'][property_item]['ontology']
        except:
            return None

    def maping_schemas_based_on_geontology(mappep_to_schema):
        '''

        '''
        return


class PhagePlusData :
    def __init__ (self,data, json_schema):
        self.data = data
        self.schema = json_schema


    def convert_json(schema):
        pass

    def get_data(self, field):
        return self.data[field]

data ={"sample_name":'s1', "collecting_institution" :'inst2',
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
data2 = [{"sample_name":'s1', "collecting_institution" :'inst2',
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
},
{"sample_name":'s1', "collecting_institution" :'inst3',
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
}]

# schema_file = open('/home/lchapado/Projects/Proyecto_ERA/relecov-tools/schema/phage_plus_V0.json')
schema_file = open('/home/bioinfo/Projects/relecov-tools/schema/phage_plus_V0.json')


json_phage_plus_schema = json.load(schema_file)
try:
    Draft202012Validator.check_schema(json_phage_plus_schema)
except:
    print('Invalid schema')
    exit(1)
phage_plus_schema = PhagePlusSchema(json_phage_plus_schema)
#try:
#    validate(instance=data,schema=json_phage_plus_schema)
#except:
#    print('Invalid input data')
#    exit(1)

sample_list = []
#df = pd.read_excel('sample.xlsx')
#for idx, row in df.iterrows():
#    sample_list.append(PhagePlus(row.to_dict(),phage_plus_schema))
from openpyxl import Workbook
import openpyxl
from itertools import islice
excel_input_file = '/home/bioinfo/Projects/relecov-tools/test/my_test_file.xlsx'
wb_file = openpyxl.load_workbook(excel_input_file, data_only=True)
ws_metadata_lab = wb_file['METADATA_LAB']
heading = []
for cell in ws_metadata_lab[1]:
    heading.append(cell.value)

for row in islice(ws_metadata_lab.values,1,ws_metadata_lab.max_row):
    sample_data_row = {}
    for idx in range(len(heading)):
        if 'date' in heading[idx]:
            sample_data_row[heading[idx]] = row[idx].strftime('%d/%m/%Y')
        else:
            sample_data_row[heading[idx]] = row[idx]
    try:
        validate(instance=sample_data_row,schema=json_phage_plus_schema)
    except:
        print('Unsuccessful validation for sample ' , sample_data_row['sample_name'])

        continue
    sample_list.append(PhagePlusData(sample_data_row,phage_plus_schema))

print('Completed')
