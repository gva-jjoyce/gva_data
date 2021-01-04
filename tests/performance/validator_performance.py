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

# random tweet with no sensitive or creative information
data = {
    "userid":12681557490473,
    "username":"USAgovernmentu1",
    "user_verified":False,
    "followers":16,
    "tweet":"The United States is currently a Democracy\nThe leader is known as \"President\"\nThe current President is Donald John Trump",
    "location":None,
    "sentiment":0.05555555555555555,
    "timestamp":"2020-12-01T00:00:02"
    }

def validate():
    for i in range(1000000):
        s = Schema(schema_definition)
        s.validate(data)


def type_check_performance():

    test_1 = 'test'
    test_2 = 100
    cycles = 10000000

    start = time.time_ns()
    for i in range(cycles):
        s = isinstance(test_1, (str, int))
        n = isinstance(test_2, (str, int))
    print(time.time_ns() - start, s, n)

    start = time.time_ns()
    for i in range(cycles):
        s = type(test_1).__name__ in {'str', 'int'}
        n = type(test_2).__name__ in {'str', 'int'}
    print(time.time_ns() - start, s, n)


start = time.time_ns()
#cProfile.run("validate()")
#cProfile.run("type_check_performance()")
print((time.time_ns() - start) / 1e9)


import datetime
print(type(datetime.datetime(2002,1,1)).__name__)
print(type(datetime.date(2002,1,1)).__name__)
print(type(datetime.time(12,0)).__name__)