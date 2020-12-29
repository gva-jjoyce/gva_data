"""
Testing writer performance after observing some jobs which
were a few minutes were observed to take over an hour.

Results (seconds to process 250,000 rows of 8 field records):

 compression | validation |    time | ratio  | through-put
-----------------------------------------------------------
  no         |  no        |   2.719 |  1.00  |        100%
  yes        |  no        |  38.031 | 13.33  |          8%
  no         |  yes       |   4.843 |  1.75  |         57%
  yes        |  yes       |  41.859 | 14.30  |          7%
-----------------------------------------------------------
"""
import shutil
import orjson as json
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..\..'))
from gva.data.writers import Writer, file_writer
from gva.logging import verbose_logger, get_logger
from gva.data.validator import Schema

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


def read_jsonl(filename, limit=-1, chunk_size=16 * 1024 * 1024, delimiter="\n"):
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

@verbose_logger
def execute_test(compress, schema):
    writer = Writer(
            writer=file_writer,
            to_path='%datefolders',
            compress=compress,
            schema=schema
    )

    reader = read_jsonl('tweets.jsonl')
    for record in reader:
        writer.append(record)

    writer.finalize()

schema = Schema(schema_definition)


logger.debug("no compression, no validation")
execute_test(False, None)
shutil.rmtree("year_2020")

logger.debug("compression, no validation")
execute_test(True, None)
shutil.rmtree("year_2020")

logger.debug("no compression, validation")
execute_test(False, schema)
shutil.rmtree("year_2020")

logger.debug("compression, validation")
execute_test(True, schema)
shutil.rmtree("year_2020")
