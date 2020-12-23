import datetime
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.readers import blob_reader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def _test_blockers():

    # project is required
    failed = False
    #try:
    print('here')
    r = blob_reader(path='path')
    #except ValueError:
    #    failed = True
    #assert failed

    # path is required
    failed = False
    try:
        r = blob_reader(project='project')
    except ValueError:
        failed = True
    assert failed

    # date_range is required
    failed = False
    try:
        r = blob_reader(project='project', path='path', date_range=(None,None))
    except:
        failed = True
    assert failed

if __name__ == "__main__":
    _test_blockers()

    print('okay')
    