"""
These for the flow builder, the flow builder is the code 
which build flows from operators using the greater than
mathematical operator (>). The resultant flow is a 
networkx DiGraph with some methods monkey patched.
"""
import os
import sys
import networkx
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.logging import get_logger
from gva.flows.operators import FilterOperator, EndOperator, NoOpOperator
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


def test_flow_builder_valid():
    """
    Test the flow builder
    The flow builder creates a networkx graph and adds some methods to it
    """
    e = EndOperator()
    f = FilterOperator()
    n = NoOpOperator()
    flow = f > n > e

    assert isinstance(flow, networkx.DiGraph)
    assert F"EndOperator-{id(e)}" in flow.nodes()
    assert F"FilterOperator-{id(f)}" in flow.nodes()
    assert F"NoOpOperator-{id(n)}" in flow.nodes()
    assert len(flow.edges()) == 2

    assert hasattr(flow, 'run')
    assert hasattr(flow, 'finalize')


def test_flow_builder_invalid_uninstantiated():
    """
    Test the flow builder doesn't succeed with an invalid Operator
    """
    e = EndOperator      # <- this should fail
    n = NoOpOperator()

    failed = False
    try:
        flow = n > e
    except TypeError:
        failed = True

    assert failed


def test_flow_builder_invalid_wrong_type():
    """
    Test the flow builder doesn't succeed with an invalid Operator
    """
    e = get_logger()      # <- this should fail
    n = NoOpOperator()

    failed = False
    try:
        flow = n > e
    except TypeError:
        failed = True

    assert failed


if __name__ == "__main__":

    test_flow_builder_valid()
    test_flow_builder_invalid_uninstantiated()
    test_flow_builder_invalid_wrong_type()
