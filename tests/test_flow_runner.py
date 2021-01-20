"""
Tests for the execution of flows. Create a basic flow
and push a payload through it.
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.logging import get_logger
from gva.flows.operators import EndOperator, NoOpOperator, UndefinedOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_flow_runner():
    """
    Run a basic flow
    """
    e = EndOperator()
    n = NoOpOperator()
    flow = n > e

    errored = False
    try:
        flow.run(data="payload")
        flow.finalize()
    except Exception:
        errored = True

    assert not errored


if __name__ == "__main__":

    test_flow_runner()

    print('okay')
