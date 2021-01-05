"""
Flow Runner

This handles the logic for moving data through a pipeline.
"""
import uuid
import random
from ..utils import TraceBlocks
import gva.logging  # type:ignore
import networkx  # type:ignore
from typing import List, Union  # type:ignore
from .bins import GoogleCloudStorageBin, FileBin, MinioBin


def _inner_runner(
        flow: networkx.DiGraph,
        node: str = None,
        data: dict = {},
        context: dict = {}):
    """
    Walk the dag/flow by:
    - Getting the function of the current node
    - Execute the function, wrapped in the base class
    - Find the next step by finding outgoing edges
    - Call this method for the next step
    """
    func = flow.nodes()[node].get("function")
    if not func:
        raise Exception("Invalid Flow - node is missing a 'function' attribute")
    if not hasattr(func, "error_writer") and hasattr(flow, "error_writer"):
        func.error_writer = flow.error_writer
    next_nodes = flow.out_edges(node, default=[])

    outcome = func(data, context)

    if outcome:
        if not type(outcome).__name__ in ["generator", "list"]:
            outcome_data, outcome_context = outcome
            outcome = [(outcome_data, outcome_context)]
        for outcome_data, outcome_context in outcome:
            for next_node in next_nodes:
                _inner_runner(flow=flow, node=next_node[1], data=outcome_data, context=outcome_context.copy())


def go(
        flow: networkx.DiGraph,
        data: dict = {},
        context: dict = {},
        trace_sample_rate: float = 0.001) -> Union[List[dict], None]:
    """
    Execute a flow by discovering starting nodes and then
    calling a recursive function to walk the flow

    returns block trace for the execution
    """
    # create a uuid for the message if it doesn't already have one
    if not context.get('uuid'):
        context['uuid'] = str(uuid.uuid4())

    # create a tracer for the message
    if not context.get('execution_trace'):
        context['execution_trace'] = TraceBlocks(uuid=context['uuid'])

    # if trace hasn't been explicitly set - randomly select based on a sample rate
    if not context.get('trace') and trace_sample_rate:
        context['trace'] = random.randint(1, round(1 / trace_sample_rate)) == 1  # nosec

    # walk through the flow, by calling each of the operators
    nodes = [node for node in flow.nodes() if len(flow.in_edges(node)) == 0]
    for node in nodes:
        _inner_runner(flow=flow, node=node, data=data, context=context)

    # if being traced, send the trace to the trace writer
    if context.get('trace', False):
        if hasattr(flow, 'trace_writer'):
            flow.trace_writer(context['execution_trace'], id_=context.get('uuid'))
    return None


def finalize(flow: networkx.DiGraph) -> List[dict]:
    """
    Finalize concludes the flow and returns the sensor information
    """
    result = []
    for node_id in flow.nodes():
        node = flow.nodes()[node_id]
        function = node.get('function')
        if function:
            result.append(node.get('function').read_sensors())
            if hasattr(function, 'finalize'):
                function.finalize()
    return result


def attach_writer(flow: networkx.DiGraph, writer):
    """
    Attach the writer to each node in the flow
    """
    logger = gva.logging.get_logger()
    try:
        for node_id in flow.nodes():
            node = flow.nodes()[node_id]
            function = node.get('function')
            setattr(function, str(writer.name), writer)
            logger.debug(F"added {writer.name} to {type(function).__name__}")
        setattr(flow, str(writer.name), writer)
        return True
    except Exception as err:
        logger.error(F"Failed to add writer to flow - {type(err).__name__} - {err}")
        return False


def attach_writers(flow: networkx.DiGraph, writers: List[dict]):

    for writer in writers:
        name = writer.get('name')
        class_name = writer.get('class')

        if class_name == 'gcs':
            writer = GoogleCloudStorageBin(                 # type: ignore
                    bin_name=name,                          # type: ignore
                    project=writer.get('project'),          # type: ignore
                    bucket=writer.get('bucket'),            # type: ignore
                    path=writer.get('path'))                # type: ignore
            attach_writer(flow, writer)
        if class_name == 'file':
            writer = FileBin(                               # type: ignore
                    bin_name=name,                          # type: ignore
                    path=writer.get('path'))                # type: ignore
            attach_writer(flow, writer)
        if class_name == 'minio':
            writer = MinioBin(                              # type: ignore
                    bin_name=name,                          # type: ignore
                    end_point=writer.get('end_point'),      # type: ignore
                    bucket=writer.get('bucket'),            # type: ignore
                    path=writer.get('path'),                # type: ignore
                    access_key=writer.get('access_key'),    # type: ignore
                    secret_key=writer.get('secret_key'),    # type: ignore
                    secure=writer.get('secure', True))      # type: ignore
            attach_writer(flow, writer)
