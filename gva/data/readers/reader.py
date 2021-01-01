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
from typing import Callable, Tuple, Optional
from ..formats.dictset import select_all, select_record_fields, limit, to_html_table, to_ascii_table
import datetime
from ...logging import get_logger
from .base_reader import BaseReader
from .gcs_reader import GoogleCloudStorageReader
from .experimental_threaded_reader import threaded_reader
try:
    import orjson as json
except ImportError:
    import ujson as json
    

FORMATTERS = {
    "json": json.loads,
    "text": lambda x: x
}


class Reader():

    def __init__(
        self,
        select: list = ['*'],
        from_path: str = None,
        where: Callable = select_all,
        reader: BaseReader = GoogleCloudStorageReader,   # type:ignore
        data_format: str = "json",
        thread_count: int = 0,
        **kwargs):
        """
        Reader accepts a method which iterates over a data source and provides
        functionality to filter, select and truncate records which are
        returned. The default reader is a GCS blob reader, a file system
        reader is also implemented.

        Reader roughly follows a SQL Select:

        SELECT column FROM data.store WHERE size == 'large'

        Reader(
            select=['column'],
            from_path='data/store',
            where=lambda record: record['size'] == 'large',
            limit=1
        )

        Data are automatically partitioned by date.
        """
        if not isinstance(select, list):
            raise TypeError("Reader 'select' parameter must be a list")
        if not hasattr(where, '__call__'):
            raise TypeError("Reader 'where' parameter must be Callable")


        self.format = data_format
        self.formatter = FORMATTERS.get(self.format.lower())
        if self.formatter is None:
            raise TypeError(F"data format unsupported: {self.format}.")

        self.reader_class = reader(from_path=from_path, **kwargs)  # type:ignore
        self.thread_count = thread_count
        if thread_count > 0:
            get_logger().warning("THREADED READER IS EXPERIMENTAL, USE IN SYSEMS IS NOT RECOMMENDED")

        self.select = select.copy()
        self.where: Callable = where

        # initialize the reader
        self._inner_line_reader = None

        logger = get_logger()
        logger.debug(F"Reader(reader={reader.__name__}, from_path='{from_path}')")  # type:ignore

    """
    Iterable

    Use this class as an iterable:

        for line in Reader("file"):
            print(line)
    """
    def new_raw_lines_reader(self):
        if self.thread_count > 0:
            sources = list(self.reader_class.list_of_sources())
            yield from threaded_reader(sources, self.reader_class, self.thread_count)
        else:
            for partition in self.reader_class.list_of_sources():
                yield from self.reader_class.read_from_source(partition)


    def __iter__(self):
        self._inner_line_reader = None
        return self


    def __next__(self):
        """
        This wraps the primary filter and select logic
        """
        if self._inner_line_reader is None:
            self._inner_line_reader = self.new_raw_lines_reader()
        while True:
            record = self._inner_line_reader.__next__()
            record = self.formatter(record)
            if not self.where(record):
                continue
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
            try:
                from IPython import get_ipython
                return get_ipython() is not None
            except:
                return False

        if is_running_from_ipython():
            from IPython.display import HTML, display
            html = to_html_table(self, 5)
            display(HTML(html))
        else:
            return to_ascii_table(self, 5)
