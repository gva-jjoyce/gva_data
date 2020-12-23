"""
JSON parsing and serialization performance tests so a decision on 
which library(s) to use can be made - previously the selection was
inconsistent.
"""
import time

def _inner_file_reader(
        file_name: str,
        chunk_size: int = 16*1024*1024,
        delimiter: str = "\n"):
    """
    This is the guts of the reader - it opens a file and reads through it
    chunk by chunk. This allows huge files to be processed as only a chunk
    at a time is in memory.
    """
    with open(file_name, 'r', encoding="utf8") as f:
        carry_forward = ""
        chunk = "INITIALIZED"
        while len(chunk) > 0:
            chunk = f.read(chunk_size)
            augmented_chunk = carry_forward + chunk
            lines = augmented_chunk.split(delimiter)
            carry_forward = lines.pop()
            yield from lines
        if carry_forward:
            yield carry_forward



def test_parser(parser):
    reader = _inner_file_reader('data.jsonl')
    for item in reader:
        parser(item)

def test_serializer(serializer):
    reader = _inner_file_reader('data.jsonl')
    for item in reader:
        dic = orjson.loads(item)
        serializer(dic)

def time_it(test, *args):
    start = time.perf_counter_ns()
    test(*args)
    return (time.perf_counter_ns() - start) / 1e9

import json
import ujson
import orjson

print('json parse  :', time_it(test_parser, json.loads))
print('ujson parse :', time_it(test_parser, ujson.loads))
print('orjson parse:', time_it(test_parser, orjson.loads))  # <- fastest

print('json serialize   :', time_it(test_serializer, json.dumps))
print('ujson serializer :', time_it(test_serializer, ujson.dumps))
print('orjson serializer:', time_it(test_serializer, orjson.dumps))  # <- fastest