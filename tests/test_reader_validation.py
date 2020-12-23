"""
Test the parameter validation on the gva.data.reader are working
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data import Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_reader_all_good():
    failed = False
    try:
        reader = Reader(
                project='',
                select=['a', 'b'],
                # deepcode ignore CopyPasteError/test: Test Code
                where=lambda x: x == x,
                date_range=(datetime.datetime.now(), datetime.datetime.now()),
                data_format='json')
    except TypeError:
        failed = True

    assert not failed
    

def test_reader_select_not_list():
    failed = False
    try:
        reader = Reader(
                project='',
                select='everything',
                # deepcode ignore CopyPasteError/test: Test Code
                where=lambda x: x == x,
                date_range=(datetime.datetime.now(), datetime.datetime.now()),
                data_format='json')
    except TypeError:
        failed = True

    assert failed


def test_reader_where_not_callable():
    failed = False
    try:
        reader = Reader(
                project='',
                select=['a', 'b'],
                where=True,
                date_range=(datetime.datetime.now(), datetime.datetime.now()),
                data_format='json')
    except TypeError:
        failed = True

    assert failed


def test_dates_not_tuple():
    failed = False
    try:
        reader = Reader(
                project='',
                select=['a', 'b'],
                # deepcode ignore CopyPasteError/test: Test Code
                where=lambda x: x == x,
                date_range=datetime.datetime.now(),
                data_format='json')
    except TypeError:
        failed = True

    assert failed


def test_format_not_known():
    failed = False
    try:
        reader = Reader(
                project='',
                select=['a', 'b'],
                # deepcode ignore CopyPasteError/test: Test Code
                where=lambda x: x == x,
                date_range=datetime.datetime.now(),
                data_format='excel')
    except TypeError:
        failed = True

    assert failed


if __name__ == "__main__":
    test_reader_all_good()
    test_reader_select_not_list()
    test_reader_where_not_callable()
    test_dates_not_tuple()
    test_format_not_known()