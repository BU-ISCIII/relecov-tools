import json
from types import SimpleNamespace

# data = '{"name": "John Smith", "hometown": {"name": "New York", "id": 123}}'

schema_file = open('/Users/erika/Desktop/BU-ISCIII/Relecov/ECDC-HERA/JSON SCHEMA/26_ENERO/ena_v01.json')

# +
import json
 
# Opening JSON file
f = open('ena_v01.json')
 
# returns JSON object as
# a dictionary
data = json.load(f)
 

 
# Closing file
f.close()
# -

# Parse JSON into an object with attributes corresponding to dict keys.
x = json.loads(fage_plus_schema, object_hook=lambda d: SimpleNamespace(**d))
#print(x.name, x.hometown.name, x.hometown.id)
