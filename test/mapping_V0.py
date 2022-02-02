import json
from types import SimpleNamespace

module_path = str(Path.cwd().parents[0])
module_path

# %cd ..

# +
import sys
from pathlib import Path

# in jupyter (lab / notebook), based on notebook path
module_path = str(Path.cwd().parents[0])
# in standard python
#module_path = str(Path.cwd(__file__).parents[0] / "py")

#if module_path not in sys.path:
#    sys.path.append(module_path)

#from modules import preparations
#import tools
# -

module_path

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
