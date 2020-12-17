import os
import sys
import networkx
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.logging import get_logger
from gva.data.flows.operators import FilterOperator, EndOperator, NoOpOperator


def test_dag_builder():
    """
    Test the DAG builder
    The DAG builder creates a networkx graph and adds some methods to it
    """
    e = EndOperator()
    f = FilterOperator()
    n = NoOpOperator()
    flow = e > f > n

    assert isinstance(flow, networkx.DiGraph)
    assert F"EndOperator-{id(e)}" in flow.nodes()
    assert F"FilterOperator-{id(f)}" in flow.nodes()
    assert F"NoOpOperator-{id(n)}" in flow.nodes()
    assert len(flow.edges()) == 2

    assert hasattr(flow, 'run')
    assert hasattr(flow, 'finalize')


def test_dag_runner():
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

    test_dag_builder()
    test_dag_runner()
