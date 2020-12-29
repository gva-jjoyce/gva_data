import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.readers import GoogleCloudStorageReader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_blockers():

    # project is required
    failed = False
    try:
        r = GoogleCloudStorageReader(from_path='path')
    except (ValueError, TypeError):
        failed = True
    assert failed

    # path is required
    failed = False
    try:
        r = GoogleCloudStorageReader(project='project')
    except (ValueError, TypeError):
        failed = True
    assert failed



if __name__ == "__main__":
    test_blockers()

    print('okay')
    