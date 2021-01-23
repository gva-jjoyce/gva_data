import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.flows.operators import BaseOperator, EndOperator
from gva.errors import IntegrityError
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

fail_counter = 0

class failing_operator(BaseOperator):
    """ create an operator which always fails """
    def execute(self, data, context): 
        global fail_counter
        fail_counter += 1
        raise Exception('Failure')
        
def test_retry():
    flow = failing_operator(retry_count=3, retry_wait=1) > EndOperator()
    flow.run(data='', context={})
    global fail_counter
    assert fail_counter == 3

def test_uninitted():
    failed = False
    try:
        flow = failing_operator(retry_count=3, retry_wait=1) > EndOperator
    except TypeError:
        failed = True
    assert failed

if __name__ == "__main__":
    test_retry()
    test_uninitted()

    print('okay')
