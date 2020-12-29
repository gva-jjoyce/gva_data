"""
Data Writer

Writes records to a data set partitioned by write time.

Default behaviour is to create a folder structure for year, month and day, and
partitioning data into files of 16Mb or are written continuously without a 60
second gap.

Records can be validated against a schema and data can be automatically 
compressed; schema validation ensures correctness and compression can
save storage requirements, but both come with a cost; results will differ
depending on exact data but as an approximation.

Tests were run using a 8 field, 250k row data set:

- no compression, no validation - 100%
- no compression, validation    - 57%
- compression, no validation    - 8%
- compression, validation       - 7%

Your results will vary, but clearly compression has a significant impact on
performance.

Paths for the data writer can contain datetime string formatting, the string
will be formatted before being created into folders. The changing of dates is
handled by the worker thread, this may lag a second before it forces the folder
to change.
"""
import lzma
import time
import os
import threading
import tempfile
import datetime
from .blob_writer import blob_writer
from typing import Callable, Optional, Any, Union
from ..validator import Schema  # type:ignore
from ...errors import ValidationError
import orjson as json


class Writer():

    def __init__(
        self,
        *,
        writer: Callable = blob_writer,
        to_path: str = '%datefolders',
        partition_size: int = 16*1024*1024,
        schema: Schema = None,
        compress: bool = False,
        use_worker_thread: bool = True,
        idle_timeout_seconds: int = 30,
        date: Optional[datetime.date] = None,
        **kwargs):
        """
        DataWriter

        Parameters:
        - to_path: the path to save records to, this is a folder name
        - partition_size: the number of bytes per partition (16Mb default)
        - schema: Schema object - if set records are validated before being
          written
        - use_worker_thread: creates a thread which performs regular checks
          and corrections
        - idle_timeout_seconds: the time with no new writes to a partition before
          closing it and creating a new partition regardless of the records
        - compress: compress the completed file using LZMA
        """
        self.to_path = to_path
        self.partition_size = partition_size
        self.bytes_left_to_write_in_partition = partition_size
        self.schema = schema
        self.file_writer: Optional[_PartFileWriter] = None
        self.last_write = time.time_ns()
        self.idle_timeout_seconds = idle_timeout_seconds
        self.use_worker_thread = use_worker_thread
        self.writer = writer
        self.kwargs = kwargs
        self.compress = compress
        self.file_name: Optional[str] = None
        self.fixed_date = False
        self.date = date
        if date:
            self.fixed_date = True
        if use_worker_thread:
            self.thread = threading.Thread(target=_worker_thread, args=(self,))
            self.thread.daemon = True
            self.thread.start()


    def _get_temp_file_name(self):
        """
        Create a tempfile, get the name and then deletes the tempfile.

        The behaviour of tempfiles is inconsisten between operating systems,
        this helps to ensure consistent behaviour.
        """
        file = tempfile.NamedTemporaryFile(prefix='gva-', delete=True)
        file_name = file.name
        file.close()
        try:
            os.remove(file_name)
        except OSError:
            pass
        return file_name


    def append(self, record: dict = {}):
        """
        Saves new entries to the partition; creating a new partition
        if one isn't active.
        """
        # Check the new record conforms to the schema before continuing
        if self.schema and not self.schema.validate(subject=record):
            raise ValidationError(F'Validation Failed ({self.schema.last_error})')

        self.last_write = time.time_ns()

        # serialize the record
        serialized = json.dumps(record).decode() + '\n'
        len_serial = len(serialized)

        with threading.Lock():
            # if this write would exceed the partition
            self.bytes_left_to_write_in_partition -= len_serial
            if self.bytes_left_to_write_in_partition < 0:
                if len_serial > self.partition_size:
                    raise ValueError('Record size is larger than partition.')
                self.on_partition_closed()

            # if we don't have a current file to write to, create one
            if not self.file_writer:
                if not self.fixed_date:
                    self.date = datetime.date.today()

                self.file_name = self._get_temp_file_name()
                self.file_writer = _PartFileWriter(
                        file_name=self.file_name,  # type:ignore
                        compress=self.compress)
                self.bytes_left_to_write_in_partition = self.partition_size

            # write the record to the file
            self.file_writer.append(serialized)

        return True


    def __enter__(self):
        return self


    def __exit__(self, type, value, traceback):
        self.on_partition_closed()


    def on_partition_closed(self):
        with threading.Lock():
            # finalize the writer
            if self.file_writer:
                self.file_writer.finalize()
            # save the file to it's destination
            if self.file_name:
                self.writer(
                        source_file_name=self.file_name,
                        target_path=self.to_path,
                        add_extention='.lzma' if self.compress else '',
                        date=self.date if self.fixed_date else datetime.date.today(),
                        **self.kwargs)
                try:
                    os.remove(self.file_name)
                except ValueError:
                    pass
            self.file_writer = None
            self.file_name = None


    def __del__(self):
        self.on_partition_closed()
        self.use_worker_thread = False


    def finalize(self):
        if self.file_writer:
            self.on_partition_closed()


class _PartFileWriter():
    """ simple wrapper for file writing to a temp file """
    __slots__ = ['file']
    def __init__(
            self,
            file_name: str,  # type:ignore
            compress: bool = False):
        self.file: Any = open(file_name, mode='wb')
        if compress:
            self.file = lzma.open(self.file, mode='wb')

    def append(self, record: str = ""):
        self.file.write(record.encode())

    def finalize(self):
        try:
            with threading.Lock():
                self.file.flush()
                self.file.close()
        except Exception:   # nosec - ignore errors
            pass

    def __del__(self):
        self.finalize()


def _worker_thread(data_writer: Writer):
    """
    Method to run an a separate thread performing the following tasks

    - when the day changes, it closes the existing partition so a new one is
      opened with today's date
    - close partitions when new records haven't been recieved for a period of
      time (default 30 seconds)
    - attempt to flush writes to disk regularly

    These are done in a separate thread so the 'append' method doesn't need to
    perform these checks every write - it can just assume they are being
    handled and focus on writes
    """
    while data_writer.use_worker_thread:
        if data_writer.file_name:

            # timeout since last write
            if (time.time_ns() - data_writer.last_write) > (data_writer.idle_timeout_seconds * 1e9):
                data_writer.on_partition_closed()

            # date has changed
            if not data_writer.fixed_date and (data_writer.date != datetime.date.today()):
                data_writer.on_partition_closed()

        time.sleep(1)
