import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers import FileWriter
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass

from gva.logging import get_logger
get_logger().setLevel(5)


def test_get_partition_list():
    f = FileWriter(to_path='tests/data/tweets/tweets.json')
    assert len(f.get_partition_list()) == 2


if __name__ == "__main__":
    test_get_partition_list()

    print('okay')
