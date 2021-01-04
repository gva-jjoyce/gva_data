"""
Test the file reader
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.readers.file_reader import FileReader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_can_find_files():
    """
    ensure we can find the test files
    the test folder has two files in it
    """

    # with a trailing /
    r = FileReader(from_path='tests/data/tweets/')
    assert len(list(r.list_of_sources())) == 2

    # without a trailing /
    r = FileReader(from_path='tests/data/tweets')
    assert len(list(r.list_of_sources())) == 2


def test_can_read_files():
    """ ensure we can read the test files """
    r = FileReader(from_path='tests/data/tweets/')
    for file in r.list_of_sources():
        for index, item in enumerate(r.read_from_source(file)):
            pass
        assert index == 24


if __name__ == "__main__":
    test_can_find_files()
    test_can_read_files()

    print('okay')
    