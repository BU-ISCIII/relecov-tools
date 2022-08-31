#!/usr/bin/env python
import json
import os


# pass test
class ConfigJson:
    def __init__(
        self,
        json_file=os.path.join(os.path.dirname(__file__), "conf", "configuration.json"),
    ):
        fh = open(json_file)
        self.json_data = json.load(fh)
        fh.close()
        self.topic_config = list(self.json_data.keys())

    def get_configuration(self, topic):
        """Obtain the topic configuration from json data"""
        if topic in self.topic_config:
            return self.json_data[topic]
        return None

    def get_topic_data(self, topic, found):
        """Obtain from topic any forward items from json data"""
        if found in self.json_data[topic]:
            return self.json_data[topic][found]
        else:
            for key, value in self.json_data[topic].items():
                if isinstance(value, dict):
                    if found in self.json_data[topic][key]:
                        return self.json_data[topic][key][found]
            return None
