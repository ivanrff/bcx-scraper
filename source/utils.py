from bson.json_util import dumps
from json import loads

def mongo_to_json(entry):
    entry_dict = (loads(dumps(entry)))
    if isinstance(entry_dict, dict):
        entry_dict.pop("_id", None)
    
    return entry_dict