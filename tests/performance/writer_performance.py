"""
Testing writer performance after observing some jobs which
were a few minutes were observed to take over an hour.

Results (seconds to process 57,581 rows of 8 field records):

┌─────────────┬────────────┬────────┬───────┬─────────────┐
│ compression │ validation │  time  │ ratio │ rows/second │
├─────────────┼────────────┼────────┼───────┼─────────────┤
│    False    │   False    │ 0.374  │  1.0  │    153634   │
│     True    │   False    │ 16.255 │ 0.023 │     3542    │
│    False    │    True    │ 0.791  │ 0.473 │    72719    │
│     True    │    True    │ 16.75  │ 0.022 │     3437    │
└─────────────┴────────────┴────────┴───────┴─────────────┘
"""
import shutil
import sys
import os
import time
sys.path.insert(1, os.path.join(sys.path[0], '..\..'))
from gva.data.writers import Writer, file_writer
from gva.logging import verbose_logger, get_logger
from gva.data.validator import Schema
from gva.data.formats import dictset
try:
    import orjson as json
except ImportError:
    import ujson as json
    

logger = get_logger()
logger.setLevel(5)

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


def read_jsonl(filename, limit=-1, chunk_size=16*1024*1024, delimiter="\n"):
    """"""
    file_reader = read_file(filename, chunk_size=chunk_size, delimiter=delimiter)
    line = next(file_reader, None)
    while line:
        yield json.loads(line)
        limit -= 1
        if limit == 0:
            return
        try:
            line = next(file_reader)
        except StopIteration:
            return


def read_file(filename, chunk_size=16*1024*1024, delimiter="\n"):
    """
    Reads an arbitrarily long file, line by line
    """
    with open(filename, "r", encoding="utf8") as f:
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

def execute_test(compress, schema, reader):
    writer = Writer(
            writer=file_writer,
            to_path='%datefolders',
            compress=compress,
            schema=schema
    )

    #reader = read_jsonl('tweets.jsonl')
    start = time.perf_counter_ns()
    for record in reader:
        writer.append(record)
    writer.finalize()
    return (time.perf_counter_ns() - start) / 1e9

schema = Schema(schema_definition)
lines = list(read_jsonl('tweets.jsonl'))

print(len(lines))
print(lines[1])

results = []
result = {
    'compression': False,
    'validation': False,
    'time': execute_test(False, None, lines)
}
results.append(result)
shutil.rmtree("year_2021")

result = {
    'compression': True,
    'validation': False,
    'time': execute_test(True, None, lines)
}
results.append(result)
shutil.rmtree("year_2021")

result = {
    'compression': False,
    'validation': True,
    'time': execute_test(False, schema, lines)
}
results.append(result)
shutil.rmtree("year_2021")

result = {
    'compression': True,
    'validation': True,
    'time': execute_test(True, schema, lines)
}
results.append(result)
shutil.rmtree("year_2021")

fastest = 100000000000
for result in results:
    if result['time'] < fastest:
        fastest = result['time']
results = dictset.set_column(results, 'ratio', lambda r: int((1000 * fastest) / r['time']) / 1000)
results = dictset.set_column(results, 'rows/second', lambda r: int(len(lines)/r['time']))
results = dictset.set_column(results, 'time', lambda r: int(1000 * r['time'])/1000)

print(dictset.to_ascii_table(results))