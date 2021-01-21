import time
import threading
import datetime
from dateutil import parser
from typing import Any
from ..validator import Schema  # type:ignore
from ...errors import ValidationError
from .internals.writer_pool import WriterPool
from ...logging import get_logger
from ...utils import paths


class Writer():

    def __init__(
            self,
            *,
            to_path: str = '%datefolders/file.jsonl',
            schema: Schema = None,
            compress: bool = False,
            idle_timeout_seconds: int = 30,
            date_exchange: Any = None,
            maximum_writers: int = 5,
            **kwargs):

            # partition_size: 64Mb
            # inner_writer: NullWriter
        """
        Create a Data Writer

        Parameters:
            to_path: string (optional)
                The path to same data to, this can contain date placeholder
            schema: gva.validator.Schema (optional)
                Schema used to test records for conformity, default is no 
                schema and therefore no validation
            compress: boolean (optional)
                Apply lzma compression to records as they are written, default
                is no compression
            idle_timeout_seconds: integer (optional)
                The number of seconds to wait before evicting writers from the
                pool for inactivity, default is 30 seconds
            date_exchange: date, string or callable (optional)
                A date, a string representation of a date or a function which
                is run against records to determine the date to use for
                creating the partition
            maximum_writers: integer (optional)
            partition_size: integer (optional)
            inner_writer: BaseWriter (optional)

        Note:
            inner_writer may have additional parameters.

        Yields:
            dictionary
        """
        self.to_path = to_path
        self.schema = schema
        self.idle_timeout_seconds = idle_timeout_seconds
        self.compress = compress

        kwargs['compress'] = compress

        # to work out which member of the pool is going to accept the data
        # we define a get_date method
        self.get_date = lambda record: datetime.datetime.now()
        if isinstance(date_exchange, datetime.date):
            self.get_date = lambda record: date_exchange  # type:ignore
        if isinstance(date_exchange, str):
            self.get_date = lambda record: record.get(date_exchange)
        if hasattr(date_exchange, '__call__'):
            self.get_date = date_exchange  # type:ignore

        # we have a pool of writers of size maximum_writers
        self.maximum_writers = maximum_writers
        self.writer_pool = WriterPool(
                pool_size=maximum_writers,
                **kwargs)

        # establish the background thread which manages the pool
        self.thread = threading.Thread(target=self.worker_thread)
        self.thread.daemon = True
        self.thread.start()

    def append(self, record: dict = {}):
        """
        Append a new record to the Writer

        Parameters:
            record: dictionary
                The record to append to the Writer

        Returns:
            integer
                The number of records in the current partition
        """
        # Check the new record conforms to the schema before continuing
        if self.schema and not self.schema.validate(subject=record, raise_exception=False):
            raise ValidationError(F'Schema Validation Failed ({self.schema.last_error})')

        # get the appropritate writer from the pool and append the record
        # the writer identity is the base of the path where the partitions
        # are written.
        data_date = self.get_date(record)
        if isinstance(data_date, str):
            data_date = parser.parse(data_date, yearfirst=True)
        identity = paths.date_format(self.to_path, data_date)

        with threading.Lock():
            partition_writer = self.writer_pool.get_writer(identity)
            return partition_writer.append(record)

    def __del__(self):
        self.finalize()

    def finalize(self):
        try:
            self.writer_pool.close()
        except Exception as e:
            get_logger().error(F"Writer failed to close pool {type(e).__name__} - {e}")
            pass

    def worker_thread(self):
        """
        Writer Pool Management
        """
        while True:
            with threading.Lock():
                # search for pool occupants who haven't had a write recently
                for partition_writer_identity in self.writer_pool.get_stale_writers(self.idle_timeout_seconds):
                    get_logger().debug(F'Evicting {partition_writer_identity} from the writer pool due to inactivity - limit is {self.idle_timeout_seconds} seconds')
                    self.writer_pool.remove_writer(partition_writer_identity)
                # if we're over capacity, evict the LRU writers
                for partition_writer_identity in self.writer_pool.nominate_writers_to_evict():
                    get_logger().debug(F'Evicting {partition_writer_identity} from the writer pool due the pool being over its {self.maximum_writers} capacity')
                    self.writer_pool.remove_writer(partition_writer_identity)
            time.sleep(2)
