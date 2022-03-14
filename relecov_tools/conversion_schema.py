#!/usr/bin/env python
from collections import OrderedDict

# import jsonschema


class PhagePlusData:
    def __init__(self, data, json_schema):
        self.data = data
        self.schema = json_schema

    def convert_json(schema):
        pass

    def get_data(self, field):
        return self.data[field]

    def map_sample_to_schema(self, mapped_structure):
        mapped_sample_list = []
        map_sample_dict = OrderedDict()
        for item, value in mapped_structure.items:
            mapped_sample_list[item] = self.data[value]
        return map_sample_dict
