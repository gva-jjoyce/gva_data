import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers.null_writer import NullWriter
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_null_writer():

    # none of these should do anything
    nw = NullWriter(to_path='bucket/path/file.extension')
    assert nw.get_partition_list() == []
    assert nw.commit('') == 'NullWriter'

if __name__ == "__main__":
    test_null_writer()

    print('okay')