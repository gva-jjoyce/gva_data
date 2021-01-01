import cProfile
import time

import shutil
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..\..'))
from gva.data.validator import Schema
try:
    import orjson as json
except ImportError:
    import ujson as json


schema_definition = {
    "fields": [
        {"name":"userid","type":"numeric"},
        {"name":"username","type":"string"},
        {"name":"user_verified","type":"boolean"},
        {"name":"followers","type":"numeric"},
        {"name":"tweet","type":"string"},
        {"name":"location","type":["string","nullable"]},
        {"name":"sentiment","type":"numeric"},
        {"name":"timestamp","type":"date"}
    ]
}

data = {
    "userid":1.2681557490473e18,
    "username":"USAgovernmentu1",
    "user_verified":False,
    "followers":16,
    "tweet":"The United States is currently a Democracy\nThe leader is known as \"President\"\nThe current President is Donald John Trump",
    "location":None,
    "sentiment":0.05555555555555555,
    "timestamp":"2020-12-01T00:00:02"
    }

def validate():
    for i in range(5000):
        s = Schema(schema_definition)
        s.validate(data)

start = time.time_ns()
cProfile.run("validate()")
print((time.time_ns() - start) / 1e9)