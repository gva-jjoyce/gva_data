"""
JSON parsing is fundamental to many of the other tests however, ensuring we
can parse some types without issue is worth testing in isolation so if it
fails, it's clear this is a failing case.

When this does fail, a large portion of other tests will also fail.
"""
import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.utils.json import parse, serialize


will_normally_fail = {
    "string": "string",
    "number": 100,
    "date": datetime.date(2015,6,1),
    "datetime": datetime.datetime(1979,9,10,23,13),
    "list": ["item"]
}


def test_json_parsing():

    failed = False

    try:
        b = serialize(will_normally_fail)
    except:
        failed = True

    assert not failed, "didn't process all types"
    assert isinstance(b, bytes), "didn't return bytes"


if __name__ == "__main__":
    test_json_parsing()
