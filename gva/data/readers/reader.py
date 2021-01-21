"""
Reader

Reads records from a data store, opinionated toward Google Cloud Storage
but a filesystem reader was created primarily to assist with development.

The reader will iterate over a set of files and return them to the caller
as a single stream of records. The files can be read from a single folder
or can be matched over a set of date/time formatted folder names. This is
useful to read over a set of logs. The date range is provided as part of 
the call; this is essentially a way to partition the data by date/time.

The reader can filter records to return a subset, for json formatted data
the records can be converted to dictionaries before filtering. json data
can also be used to select columns, so not all read data is returned.

The reader does not support aggregations, calculations or grouping of data,
it is a log reader and returns log entries. The reader can convert a set
into Pandas dataframe, or the dictset helper library can perform some 
activities on the set in a more memory efficient manner.
"""
from typing import Callable, Optional
from ..formats.dictset import select_record_fields, select_from
from ..formats.display import html_table, ascii_table
from ...logging import get_logger
from .google_cloud_storage_reader import GoogleCloudStorageReader
from .internals import BaseReader, threaded_reader, processed_reader
from ...utils import json


# available line parsers
PARSERS = {
    "json": json.parse,
    "text": lambda x: x
}


class Reader():

    def __init__(
        self,
        *,  # force all paramters to be keyworded
        select: list = ['*'],
        from_path: str = None,
        where: Callable = None,
        reader: BaseReader = GoogleCloudStorageReader,   # type:ignore
        data_format: str = "json",
        **kwargs):
        """
        


        """
        if not isinstance(select, list):
            raise TypeError("Reader 'select' parameter must be a list")
        if where is not None and not hasattr(where, '__call__'):
            raise TypeError("Reader 'where' parameter must be Callable or None")

        # load the line converter
        self.parser = PARSERS.get(data_format.lower())
        if self.parser is None:
            raise TypeError(F"data format unsupported: {data_format}.")

        # instantiate the injected reader class
        self.reader_class = reader(from_path=from_path, **kwargs)  # type:ignore

        self.select = select.copy()
        self.where: Optional[Callable] = where

        # initialize the reader
        self._inner_line_reader = None

        args_passed_in_function = [
                F"select={select}",
                F"from_path='{from_path}'",
                F"where={where.__name__ if not where is None else 'Select All'}",
                F"reader={reader.__name__}",     # type:ignore
                F"data_format='{data_format}'"]
        kwargs_passed_in_function = [f"{k}={v!r}" for k, v in kwargs.items()]
        formatted_arguments = ", ".join(args_passed_in_function + kwargs_passed_in_function)

        # threaded reader
        self.thread_count = int(kwargs.get('thread_count', 0))

        get_logger().debug(f"Reader({formatted_arguments})")


        """ FEATURES IN DEVELOPMENT """

        # number of days to walk backwards to find records
        self.step_back_days = int(kwargs.get('step_back_days', 0))
        if self.step_back_days > 0:
            get_logger().warning("STEP BACK DAYS IS IN DEVELOPMENT")

        # multiprocessed reader
        self.fork_processes = int(kwargs.get('fork_processes', False))
        if self.thread_count > 0 and self.fork_processes:
            raise Exception('Forking and Threading can not be used at the same time')
        if self.fork_processes:
            get_logger().warning("MULTI-PROCESS READER IS EXPERIMENTAL, IT IS LIKELY TO NOT RETURN ALL DATA")


    """
    Iterable

    Use this class as an iterable:

        for line in Reader("file"):
            print(line)
    """
    def create_line_reader(self):
        sources = list(self.reader_class.list_of_sources())
        get_logger().debug(F"Reader found {len(sources)} sources to read data from.")
        if self.thread_count > 0:
            ds = threaded_reader(sources, self.reader_class, self.thread_count)
            ds = self._parse(ds)
            yield from select_from(ds, where=self.where)
        elif self.fork_processes:
            yield from processed_reader(sources, self.reader_class, self.parser, self.where)
        else:
            for partition in sources:
                ds = self.reader_class.read_from_source(partition)
                ds = self._parse(ds)
                yield from select_from(ds, where=self.where)

    def _parse(self, ds):
        for item in ds:
            yield self.parser(item)

    def __iter__(self):
        return self

    def __next__(self):
        if self._inner_line_reader is None:
            self._inner_line_reader = self.create_line_reader()

        # get the the next line from the reader
        record = self._inner_line_reader.__next__()
        if self.select != ['*']:
            record = select_record_fields(record, self.select)
        return record


    """
    Context Manager

    Use this class using the 'with' statement:

        with Reader("file") as r:
            line = r.read_line()
            while line:
                print(line)
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # exist needs to exist to be a context manager

    def read_line(self):
        try:
            return self.__next__()
        except StopIteration:
            return None

    """
    Exports
    """
    def to_pandas(self):
        """
        Only import Pandas if needed
        """
        try:
            import pandas as pd  # type:ignore
        except ImportError:
            raise ImportError("Pandas must be installed to use 'to_pandas'")
        return pd.DataFrame(self)

    def __repr__(self):

        def is_running_from_ipython():
            """
            True when running in Jupyter
            """
            try:
                from IPython import get_ipython  # type:ignore
                return get_ipython() is not None
            except Exception:
                return False

        if is_running_from_ipython():
            from IPython.display import HTML, display  # type:ignore
            html = html_table(self, 5)
            display(HTML(html))
            return ''  # __repr__ must return something
        else:
            return ascii_table(self, 5)
