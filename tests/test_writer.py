import shutil
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers import Writer, file_writer
from gva.data.readers import Reader, FileReader
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def do_writer():
    w = Writer(
        writer=file_writer,
        to_path='_tests/year_%Y/test.jsonl',
        date=datetime.date.today()
    )
    w.append({"test":True})
    w.append({"test":False})
    w.finalize()
    del w


def test_reader_writer():

    do_writer()

    r = Reader(
        reader=FileReader,
        from_path='_tests/year_%Y/'
    )

    l = len(list(r))
    shutil.rmtree("_tests", ignore_errors=True)

    assert l == 2


if __name__ == "__main__":
    test_reader_writer()

    print('okay')