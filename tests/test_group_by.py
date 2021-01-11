"""
Tests for paths to ensure the split and join methods
of paths return the expected values for various
stimulus.
"""
import datetime
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.formats import dictset
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_group_by():
    ds = [
        {'user': 'bob',   'value': 1},
        {'user': 'bob',   'value': 2},
        {'user': 'alice', 'value': 3},
        {'user': 'alice', 'value': 4},
        {'user': 'alice', 'value': 5},
        {'user': 'eve',   'value': 6},
        {'user': 'eve',   'value': 7}
    ]

    groups = dictset.group_by(ds, 'user')

    # the right number of groups
    assert len(groups) == 3

    # the groups have the right number of records
    assert groups.count('bob') == 2
    assert groups.count('alice') == 3
    assert groups.count('eve') == 2

    # the aggregations work
    assert groups.aggregate('value', max).get('bob') == 2
    assert groups.aggregate('value', min).get('alice') == 3
    assert groups.aggregate('value', sum).get('eve') == 13


if __name__ == "__main__":
    test_group_by()

    
    print('okay')
