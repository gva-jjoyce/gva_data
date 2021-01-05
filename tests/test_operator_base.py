import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.flows.operators import BaseOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

class invalid_operator_call(BaseOperator):
    def execute(self): pass
    def __call__(self): pass

class invalid_operator_version(BaseOperator):
    def execute(self): pass
    def version(self): pass


def test_invalid_op_call():
    failed = False
    try:
        o = invalid_operator_call()
    except:
        failed = True
    assert failed, 'overridden __call__'

def test_invalid_op_vers():
    failed = False
    try:
        o = invalid_operator_version()
    except:
        failed = True
    assert failed, 'overridden version'


if __name__ == "__main__":
    test_invalid_op_call()
    test_invalid_op_vers()

    print('okay')