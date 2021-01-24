import shutil
import datetime
import os
import sys
import glob
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers import Writer, NullWriter, FileWriter
from gva.data.readers import Reader, FileReader
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

from gva.logging import get_logger
get_logger().setLevel(5)


def do_writer():
    w = Writer(
        inner_writer=FileWriter,
        to_path='_tests/year_%Y/test.jsonl',
        date_exchange=datetime.date.today()
    )
    for i in range(int(1e5)):
        w.append({"test":True})
        w.append({"test":False})
    w.finalize()


def do_writer_compressed():
    w = Writer(
        inner_writer=FileWriter,
        to_path='_tests/year_%Y/test.jsonl',
        compress=True,
        date_exchange=datetime.date.today()
    )
    for i in range(int(1e5)):
        w.append({"test":True})
        w.append({"test":False})
    w.finalize()
    del w


def test_reader_writer():

    do_writer()

    r = Reader(
        inner_reader=FileReader,
        from_path='_tests/year_%Y/'
    )
    l = len(list(r))
    shutil.rmtree("_tests", ignore_errors=True)
    assert l == 200000, l


def test_reader_writer_compressed():

    do_writer_compressed()

    g = glob.glob('_tests/**/*.lzma')
    assert len(g) > 0, g

    r = Reader(
        inner_reader=FileReader,
        from_path='_tests/year_%Y/'
    )
    l = len(list(r))
    shutil.rmtree("_tests", ignore_errors=True)
    assert l == 200000, l


def get_data():
    r = Reader(
        inner_reader=FileReader,
        from_path='tests/data/tweets')
    return r


if __name__ == "__main__":
    test_reader_writer()
    test_reader_writer_compressed()

    print('okay')