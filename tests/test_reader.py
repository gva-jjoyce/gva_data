import datetime
import time
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.readers import Reader, FileReader, MinIoReader
from gva.data.formats import dictset
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

from gva.logging import get_logger
get_logger().setLevel(5)

def test_reader_can_read():
    r = Reader(
        inner_reader=FileReader,
        from_path='tests/data/tweets'
    )
    assert len(list(r)) == 50


def test_unknown_format():
    failed = False

    try:
        r = Reader(
            inner_reader=FileReader,
            from_path='tests/data/tweets',
            data_format='csv'
        )
    except TypeError:
        failed = True

    assert failed


def test_reader_context():
    counter = 0
    with Reader(inner_reader=FileReader, from_path='tests/data/tweets') as r:
        n = r.read_line()
        while n:
            counter += 1
            n = r.read_line()

    assert counter == 50


def test_reader_to_pandas():
    r = Reader(inner_reader=FileReader, from_path='tests/data/tweets')
    df = r.to_pandas()

    assert len(df) == 50


def test_threaded_reader():
    r = Reader(
            thread_count=2,
            inner_reader=FileReader,
            from_path='tests/data/tweets')
    df = r.to_pandas()
    assert len(df) == 50


def test_multiprocess_reader():
    r = Reader(
            fork_processes=True,
            inner_reader=FileReader,
            from_path='tests/data/tweets')
    df = r.to_pandas()
    assert len(df) == 50


if __name__ == "__main__":
    test_reader_can_read()
    test_unknown_format()
    test_reader_context()
    test_reader_to_pandas()
    test_threaded_reader()
    test_multiprocess_reader()

    print('okay')
    