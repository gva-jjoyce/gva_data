import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers import null_writer
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_null_writer():

    # none of these should do anything
    assert null_writer() is None
    assert null_writer(file='%$£@£$') is None
    assert null_writer(to_path='%$£@£$') is None


if __name__ == "__main__":
    test_null_writer()

    print('okay')