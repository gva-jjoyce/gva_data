import time
import os
import sys
import datetime
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers.null_writer import NullWriter
from gva.data import Writer
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

from gva.logging import get_logger
get_logger().setLevel(5)



def test_writer_partition_from_data():

    ds = [
        {'key': 1, 'value': 'one', 'date_field': '2020-01-01'},
        {'key': 2, 'value': 'two', 'date_field': '2020-01-01'},
        {'key': 3, 'value': 'three', 'date_field': '2020-01-01'},
        {'key': 4, 'value': 'four', 'date_field': '2020-01-02'},
        {'key': 5, 'value': 'five', 'date_field': '2020-01-02'}
    ]


    # none of these should do anything
    w = Writer(
            to_path='bucket/%datefolders/file.extention',
            inner_writer=NullWriter,
            date_exchange=lambda row: datetime.datetime.fromisoformat(row['date_field']))

    results = []
    for row in ds:
        l = w.append(row)
        results.append(l)

    assert results == [1,2,3,1,2]

if __name__ == "__main__":
    test_writer_partition_from_data()

    print('okay')