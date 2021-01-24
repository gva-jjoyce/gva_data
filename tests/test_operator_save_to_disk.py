import os
import sys
import shutil
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.flows.operators import SaveToDiskOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_save_to_disk_operator():

    try:
        os.remove("_test/save_to_disk_operator-0000.jsonl")
    except:
        pass

    n = SaveToDiskOperator(
            to_path="_test/save_to_disk_operator.jsonl",
            compress=False)
    n.execute(data={"this":"is", "a":"record"}, context={})
    n.finalize()

    assert os.path.exists("_test/save_to_disk_operator-0000.jsonl")

    try:
        os.remove("_test/save_to_disk_operator-0000.jsonl")
    except:
        pass


if __name__ == "__main__":
    test_save_to_disk_operator()

    print('okay')