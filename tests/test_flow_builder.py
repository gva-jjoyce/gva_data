"""
These for the flow builder, the flow builder is the code 
which build flows from operators using the greater than
mathematical operator (>). 
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.logging import get_logger
from gva.flows.operators import FilterOperator, EndOperator, NoOpOperator
from gva.flows import Flow
try:
    from rich import traceback
    traceback.install()
except ImportError:  # pragma: no cover
    pass


def test_flow_builder_valid():
    """
    Test the flow builder
    """
    e = EndOperator()
    f = FilterOperator()
    n = NoOpOperator()
    flow = f > n > e

    assert isinstance(flow, Flow)
    assert F"EndOperator-{id(e)}" in flow.nodes.keys()
    assert F"FilterOperator-{id(f)}" in flow.nodes.keys()
    assert F"NoOpOperator-{id(n)}" in flow.nodes.keys()
    assert len(flow.edges) == 2

    assert hasattr(flow, 'run')
    assert hasattr(flow, 'finalize')

    print('\n', flow.nodes)
    print(flow.edges, '\n')


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

    print('okay')
