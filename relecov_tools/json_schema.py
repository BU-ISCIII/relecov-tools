#!/usr/bin/env python
import logging
from collections import OrderedDict

log = logging.getLogger(__name__)


class PhagePlusSchema:
    def __init__(self, schema):
        self.schema = schema
        self.ontology = {}
        for key, values in schema["properties"].items():
            self.ontology[values["ontology"]] = key
        self.properties = list(schema["properties"].keys())

    def get_gontology(self, property_item):
        """Return the geontology value for a property in the schema"""
        try:
            return self.schema["properties"][property_item]["ontology"]
        except KeyError as e:
            log.error("geontology value %s %s", property_item, e)
            return None

    def maping_schemas_based_on_geontology(self, mapped_to_schema):
        """Return a dictionnary with the properties of the mapped_to_schema
        as key and properties of phagePlusSchema as value
        """
        mapped_dict = OrderedDict()
        for key, values in mapped_to_schema["properties"].items():
            try:
                mapped_dict[key] = self.ontology[values["ontology"]]
            except KeyError as e:
                log.error("Enable to map schema, because of %s is not defined", e)
                # There is no exact match on ontology. Search for the parent
                # to be implemented later
                pass
        return mapped_dict

    def get_schema_properties(self):
        """Return the properties defined in the schema"""
        return self.properties
