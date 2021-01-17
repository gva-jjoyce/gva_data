import time
import os
import sys
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


EXPECTED_RESULTS = {
    1:1,
    2:2,
    3:3,
    4:1
}

def test_writer_timeout():

    # none of these should do anything
    w = Writer(
            to_path='bucket/path/file.extention',
            inner_writer=NullWriter,
            idle_timeout_seconds=2)

    for i in range(1,5):
        l = w.append({'data':'data'})
        assert l == EXPECTED_RESULTS[i]
        time.sleep(i + 0.05)

if __name__ == "__main__":
    test_writer_timeout()

    print('okay')