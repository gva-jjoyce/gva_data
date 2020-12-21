"""
Base Operator

All Operations should inherit from this class, it will help ensure a common
structure to Operation classes and provide some common functionality and
interfaces.

This Base Operator is relatively complex but serves to simplify the
implementation of functional Operators so engineers can focus on handling data,
not getting a pipeline to work.
"""
import abc  # abstract base class library
import inspect
import functools
import hashlib
import time
import ujson as json
import datetime
import types
import traceback
import networkx as nx   # type:ignore
from ...logging import get_logger  # type:ignore
from ..runner import go, finalize, attach_writer
from typing import Union, List
from ...errors import RenderErrorStack


# inheriting ABC is part of ensuring that this class only ever
# interited from
class BaseOperator(abc.ABC):

    def __init__(self, **kwargs):
        """
        Operator Base Class

        You are expected to override the execute method with the logic of the
        operation, this should return None, if the pipeline should terminate
        at this operation (for example, if the Operator filters records),
        return a tuple of (data, context), or a generator/list of (data,
        context).

        The '__call__' and 'version' methods should not be overriden, steps
        are taken to help ensure they aren't.

        - retry_count: the number of times to attempt an operation before
          aborting, this defaults to 2 and is limited between 1 and 5
        - retry_wait: the number of seconds to wait between retries, this
          defaults to 5 and is limited between 1 and 300
        - rolling_failure_window: the number of previous operations to
          remember the success/failure, when >50% of the operations in this
          window are failures, the job aborts. This defaults to 10 and is
          limited between 1 (single failure aborts) and 100
        """
        self.graph = None               # part of drawing dags
        self.records_processed = 0      # number of times this operator has been run
        self.execution_time_ns = 0      # nano seconds of cpu execution time
        self.errors = 0                 # number of errors
        self.commencement_time = None   # the time processing started
        self.first_run = True           # so some things only run once
        self.logger = get_logger()      # get the GVA logger

        # read retry settings, clamp values to practical ranges
        self.retry_count = self._clamp(kwargs.get('retry_count', 2), 1, 5)
        self.retry_wait = self._clamp(kwargs.get('retry_wait', 5), 1, 300)
        rolling_failure_window = self._clamp(kwargs.get('rolling_failure_window', 10), 1, 100)
        self.last_few_results = [1] * rolling_failure_window  # track the last n results

        # error on detecting override of some methods
        if inspect.getsource(self.execute) != inspect.getsource(self.__class__.execute):
            raise Exception("Operator's __call__ method must not be overridden.")
        if inspect.getsource(self.version) != inspect.getsource(self.__class__.version):
            raise Exception("Operator's version method must not be overridden.")

    @abc.abstractmethod
    def execute(self, data: dict = {}, context: dict = {}):
        """
        YOU MUST OVERRIDE THIS METHOD

        This is where the main logic for the Operator is implemented.
        It should expect a single record and return:

        - None = do not continue further through the flow
        - (data, context) = pass data to the next operation
        - list(data, context) = run the next operator multiple times

        The list can be a generator.
        """
        raise NotImplementedError("execute method must be overridden")

    def __call__(self, data: dict = {}, context: dict = {}):
        """
        DO NOT OVERRIDE THIS METHOD
        """
        if self.first_run:
            self.first_run = False
            self.commencement_time = datetime.datetime.now()
        self.records_processed += 1
        attempts_to_go = self.retry_count
        while attempts_to_go > 0:
            try:
                start_time = time.perf_counter_ns()
                outcome = self.execute(data, context)
                self.execution_time_ns += (time.perf_counter_ns() - start_time)
                # add a success to the last_few_results list
                self.last_few_results.append(1)
                self.last_few_results.pop(0)
                break
            except Exception as err:
                self.errors += 1
                attempts_to_go -= 1
                self.logger.error(F"{self.__class__.__name__} - {type(err).__name__} - {err}")
                if attempts_to_go:
                    time.sleep(self.retry_wait)
                else:
                    try:
                        error_payload = (
                                F"timestamp : {datetime.datetime.today().isoformat()}\n"
                                F"operator  : {self.__class__.__name__}\n"
                                F"error     : {type(err).__name__}\n"
                                F"details   : {err}\n"
                                F"{RenderErrorStack()}\n"
                                "---------------------------------------------  context  --------------------------------------------\n"
                                F"{context}\n"
                                "----------------------------------------------  data  ----------------------------------------------\n"
                                F"{data}\n"
                                "----------------------------------------------------------------------------------------------------\n")
                        self.error_writer(error_payload)  # type:ignore
                    except Exception as err:
                        self.logger.error(F"Problem writing to the error bin, a record has been lost. {type(err).__name__} - {err}")
                    outcome = None
                    # add a failure to the last_few_results list
                    self.last_few_results.append(0)
                    self.last_few_results.pop(0)

        # message tracing
        if context.get('trace', False):
            data_hash = self.hash(data)
            self.logger.trace(F"{context.get('uuid')} {self.__class__.__name__} {data_hash}")
            context['execution_trace'].add_block(data_hash=data_hash,
                                                 operator=self.__class__.__name__,
                                                 version=self.version())
            try:
                self.trace_writer(context['execution_trace'].blocks[-1])  # type:ignore
            except:  # nosec
                pass # nosec

        # if there is a high failure rate, abort
        if sum(self.last_few_results) < (len(self.last_few_results) / 2):
            self.logger.critical(F"Failure Rate for {self.__class__.__name__} over last {len(self.last_few_results)} executions is over 50%, aborting.")
            quit()

        return outcome

    def read_sensors(self):
        """
        Format data about the transformation, this can be overridden but it
        should include this information
        """
        response = {
            "operator": self.__class__.__name__,
            "version": self.version(),
            "records_processed": self.records_processed,
            "error_count": self.errors,
            "execution_sec": self.execution_time_ns / 1e9
        }
        if self.commencement_time:
            response['commencement_time'] = self.commencement_time.isoformat()
        return response

    @functools.lru_cache(1)
    def version(self):
        """
        DO NOT OVERRIDE THIS METHOD.

        The version of the operator code, this is intended to facilitate
        reproducability and auditability of the pipeline. The version is the
        last 12 characters of the hash of the source code of the 'execute'
        method. This removes the need for the developer to remember to
        increment a version variable.

        Hashing isn't security sensitive here, it's to identify changes
        rather than protect information.
        """
        source = inspect.getsource(self.execute)
        full_hash = hashlib.sha224(source.encode())
        return full_hash.hexdigest()[-12:]

    def __del__(self):
        # do nothing - prevents errors if someone calls super().__del__
        pass

    def __gt__(self, next_operators: Union[List[nx.DiGraph], nx.DiGraph]):
        """
        Smart flow/DAG builder. This allows simple flows to be defined using
        the following syntax:

        Op1 > Op2 > Op3

        The builder adds support functions to the resulting 'flow' object.
        """
        # make sure the next_operator is iterable
        if not isinstance(next_operators, list):
            next_operators = [next_operators]
        if self.graph:
            # if I have a graph already, build on it
            graph = self.graph
        else:
            # if I don't have a graph, create one
            graph = nx.DiGraph()
            graph.add_node(F"{self.__class__.__name__}-{id(self)}", function=self)
        for operator in next_operators:
            if isinstance(operator, nx.DiGraph):
                # if we're pointing to a graph, merge with the current graph,
                # we need to find the node with no incoming nodes we identify
                # the entry-point
                graph = nx.compose(operator, graph)
                graph.add_edge(
                    F"{self.__class__.__name__}-{id(self)}",
                    [node for node in operator.nodes() if len(graph.in_edges(node)) == 0][0],
                )
            else:
                # otherwise add the node and edge and set the graph further
                # down the line
                graph.add_node(F"{operator.__class__.__name__}-{id(operator)}", function=operator)
                graph.add_edge(F"{self.__class__.__name__}-{id(self)}", F"{operator.__class__.__name__}-{id(operator)}")
                operator.graph = graph
        # this variable only exists to build the graph, we don't need it
        # anymore so destroy it
        self.graph = None

        # extend the base DiGraph class with flow helper functions
        graph.run = types.MethodType(go, graph)
        graph.finalize = types.MethodType(finalize, graph)
        graph.attach_writer = types.MethodType(attach_writer, graph)

        return graph

    def _clamp(self, value, low_bound, high_bound):
        """
        'clamping' is fixing a value within a range
        """
        if value <= low_bound:
            return low_bound
        if value >= high_bound:
            return high_bound
        return value

    def hash(self, block):
        string_object = json.dumps(block, sort_keys=True)
        block_string = string_object.encode()
        raw_hash = hashlib.sha256(block_string)
        hex_hash = raw_hash.hexdigest()
        return hex_hash