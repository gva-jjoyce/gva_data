from typing import Union, List
import uuid
import random
from ..utils import TraceBlocks
from ..logging import get_logger
from .bins import FileBin, MinioBin, GoogleCloudStorageBin


class Flow():
    """
    Flow represents Directed Acyclic Graphs which are used to describe GVA data
    pipelines.
    """
    def __init__(self):
        self.nodes = {}
        self.edges = [] 

    def add_operator(self, name, operator):
        self.nodes[name] = operator

    def link_operators(self, source_operator, target_operator):
        self.edges.append((source_operator, target_operator))

    def get_outgoing_links(self, name):
        return [target for source,target in self.edges if source == name]

    def get_entry_points(self):
        targets = {target for source,target in self.edges}
        return [k for k,v in self.nodes.items() if k not in targets]

    def get_operator(self, name):
        return self.nodes.get(name)

    def merge(self, assimilatee):
        self.nodes = {**self.nodes, **assimilatee.nodes}
        self.edges += assimilatee.edges

    def run(
            self,
            data: dict = {},
            context: dict = {},
            trace_sample_rate: float = 1/1000):
        """
        Create a `run` of a flow and execute with a specific data object.

        Parameters:
            data: dictionary, any (optional)
                The data the flow is to process, opinionated to be a dictionary
                however, any data type is accepted.
            context: dictionary (optional)
                Additional information to support the processing of the data
            trace_sample_rate: float (optional)
                The sample for for to emit trace messages for, default is 
                1/1000.
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

        # start the flow, walk from the nodes with no incoming links
        for operator_name in self.get_entry_points():
            self._inner_runner(operator_name=operator_name, data=data, context=context)

        # if being traced, send the trace to the trace writer
        if context.get('trace', False) and hasattr(self, 'trace_writer'):
            self.trace_writer(context['execution_trace'], id_=str(context.get('uuid')))  #type:ignore


    def _inner_runner(
            self,
            operator_name: str = None,
            data: dict = {},
            context: dict = {}):
        """
        Walk the dag/flow by:
        - Getting the function of the current node
        - Execute the function, wrapped in the base class
        - Find the next step by finding outgoing edges
        - Call this method for the next step
        """
        operator = self.get_operator(operator_name)
        if operator is None:
            raise Exception(F"Invalid Flow - operation {operator_name} is invalid")
        if not hasattr(operator, "error_writer") and hasattr(self, "error_writer"):
            operator.error_writer = self.error_writer  # type:ignore
        out_going_links = self.get_outgoing_links(operator_name)

        outcome = operator(data, context)

        if outcome:
            if not type(outcome).__name__ in ["generator", "list"]:
                outcome_data, outcome_context = outcome
                outcome = [(outcome_data, outcome_context)]
            for outcome_data, outcome_context in outcome:
                for operator_name in out_going_links:
                    self._inner_runner(operator_name=operator_name, data=outcome_data, context=outcome_context.copy())


    def finalize(self):
        """
        Finalize concludes the flow and returns the sensor information
        """
        for operator_name in self.nodes.keys():
            operator = self.get_operator(operator_name)
            if operator:
                get_logger().audit(operator.read_sensors())
                if hasattr(operator, 'finalize'):
                    operator.finalize()
        return True


    def attach_writers(self, writers: List[dict]):

        for writer in writers:
            name = writer.get('name')
            class_name = writer.get('class')

            if class_name == 'gcs':
                writer = GoogleCloudStorageBin(                 # type: ignore
                        bin_name=name,                          # type: ignore
                        project=writer.get('project'),          # type: ignore
                        bucket=writer.get('bucket'),            # type: ignore
                        path=writer.get('path'))                # type: ignore
                self._attach_writer(writer)
            if class_name == 'file':
                writer = FileBin(                               # type: ignore
                        bin_name=name,                          # type: ignore
                        path=writer.get('path'))                # type: ignore
                self._attach_writer(writer)
            if class_name == 'minio':
                writer = MinioBin(                              # type: ignore
                        bin_name=name,                          # type: ignore
                        end_point=writer.get('end_point'),      # type: ignore
                        bucket=writer.get('bucket'),            # type: ignore
                        path=writer.get('path'),                # type: ignore
                        access_key=writer.get('access_key'),    # type: ignore
                        secret_key=writer.get('secret_key'),    # type: ignore
                        secure=writer.get('secure', True))      # type: ignore
                self._attach_writer(writer)

    def _attach_writer(self, writer):
        """
        Attach the writer to each node in the flow
        """
        logger = get_logger()
        try:
            for operator_name in self.nodes:
                operator = self.get_operator(operator_name)
                setattr(operator, str(writer.name), writer)
                logger.debug(F"added {writer.name} to {type(operator).__name__}")
            setattr(self, str(writer.name), writer)
            return True
        except Exception as err:
            logger.error(F"Failed to add writer to flow - {type(err).__name__} - {err}")
            return False



