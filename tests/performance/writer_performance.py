"""
Testing writer performance after observing some jobs which
were a few minutes were observed to take over an hour.
"""
import shutil
import orjson as json
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..\..'))
from gva.data.writers import Writer, file_writer
from gva.logging import verbose_logger, get_logger

logger = get_logger()
logger.setLevel(5)


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
def execute_test(compress):

    writer = Writer(
            writer=file_writer,
            compress=compress
    )

    reader = read_jsonl('data.jsonl')
    for record in reader:
        writer.append(record)

    writer.finalize()


logger.debug("no compression")
execute_test(False)
shutil.rmtree("year_2020")

logger.debug("compression")
execute_test(True)
shutil.rmtree("year_2020")
