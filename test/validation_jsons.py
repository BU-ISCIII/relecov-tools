#!/usr/bin/env python
from jsonschema import validate
from jsonschema import Draft202012Validator
import json ,sys
from openpyxl import Workbook
import openpyxl
from itertools import islice
from utils import *

'''
References:
    genepio.owl https://github.com/GenEpiO/genepio/blob/master/genepio.owl


'''

class PhagePlusSchema :
    def __init__ (self, schema):
        self.schema = schema
        self.ontology = {}
        for key , values in schema['properties'].items():
            self.ontology[values['ontology']] = key


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

    def maping_schemas_based_on_geontology(mapped_to_schema):
        '''
        Description:
            The function return a dictionnary with the properties of the mapped_to_schema as key and
            properties of phagePlusSchema as value
        Input:
            mapped_to_schema    # json schema to be mapped
        Return:
            mapped_dict contains as key the property in the mapped_to_schema and value de property in the self.schema
        '''
        mapped_dict = OrderedDict()
        for key, values in mapped_to_schema['properties'].items():
            try:
                mapped_dict[key] = self.ontology[values['ontology']]
            except:
                # There is no exact match on ontology. Search for the parent
                # to be implemented later
                pass
        return mapped_dict


class PhagePlusData :
    def __init__ (self,data, json_schema):
        self.data = data
        self.schema = json_schema


    def convert_json(schema):
        pass

    def get_data(self, field):
        return self.data[field]

    def map_sample_to_schema(mapped_structure):
        map_sample_dict = OrderedDict()
        for item, value in mapped_structure.items:
            mapped_sample_list[item] = self.data[value]
        return map_sample_dict



def check_arg (args=None) :
    '''
    The function is used for parsing the input parameters form the command line
    using the standard python package argparse. The package itself is handling
    the validation and the return errors messages
    Input:
        args    # Contains the arguments from the command line
    Return:
        parser.parse_args()     # The variable contains the valid parameters
    '''
    parser = argparse.ArgumentParser(prog = 'validation_jsons.py',
                                     formatter_class=argparse.RawDescriptionHelpFormatter,
                                     description= 'Read the excel input user files and store them in the LIMS')

    parser.add_argument('-v', '--version', action='version', version='%(prog)s 0.0.1')
    parser.add_argument('-p', '--phagePlusSchema' , required = True,
                        help='file where the phage plus schema is located')
    parser.add_argument('-i', '--inputFile', required =True,
                        help='Execl file with the user collected data')
    parser.add_argument('-c', '--convertedSchema', required = True,
                        help='schema to be mapped')
    return parser.parse_args()



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


if __name__ == '__main__':
    if len (sys.argv) == 1 :
        print('Usage: validation_jsons.py [ARGUMENTS] ')
        print('Try  validation_jsons.py --help for more information.')
        exit(2)
    arguments = check_arg(sys.argv[1:])
    if not check_if_file_exists(arguments.phagePlusSchema):
        print('phage plus schema file does not exist\n')
        exit(2)
    if not check_if_file_exists(arguments.inputFile):
        print('excel file does not exist\n')
        exit(2)
    if not check_if_file_exists(arguments.convertedSchema):
        print('file for converting schema does not exist\n')
        exit(2)
    # schema_file = open('/home/lchapado/Projects/Proyecto_ERA/relecov-tools/schema/phage_plus_V0.json')
    schema_file = open(arguments.phagePlusSchema)


    json_phage_plus_schema = json.load(schema_file)
    try:
        Draft202012Validator.check_schema(json_phage_plus_schema)
    except:
        print('Invalid schema')
        exit(1)
    phage_plus_schema = PhagePlusSchema(json_phage_plus_schema)

    sample_list = []
    wb_file = openpyxl.load_workbook(arguments.inputFile, data_only=True)
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
    # create the information mapped to the new schema
    mapped_structure = phage_plus_schema.maping_schemas_based_on_geontology(arguments.convertedSchema)
    mapped_sample_list = []
    for sample in sample_list:
        mapped_sample_list.append(map_sample_to_schema(mapped_structure))


    print('Completed')
