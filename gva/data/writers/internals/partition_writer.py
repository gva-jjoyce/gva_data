import lzma
import threading
import tempfile
import os
from typing import Any
from ....logging import get_logger
from ....utils.json import serialize
from .base_writer import BaseWriter
from ..null_writer import NullWriter

BUFFER_SIZE = 1024*1024  # 1Mb


class PartitionWriter():

    def __init__(
            self,
            *,    # force params to be named
            inner_writer: BaseWriter = NullWriter,
            partition_size: int = 64*1024*1024,
            compress: bool = True,
            **kwargs):

        self.inner_writer = inner_writer(**kwargs)
        self.compress = compress
        self.maximum_partition_size = partition_size
        self.open_partition()

    def append(self, record: dict = {}):
        # serialize the record
        serialized = serialize(record) + '\n'
        # the newline isn't counted so add 1 to get the actual length
        len_serial = len(serialized) + 1

        with threading.Lock():
            # if this write would exceed the partition, close it so another
            # partition will be created
            self.bytes_in_partition += len_serial
            if self.bytes_in_partition > self.maximum_partition_size:
                self.commit()
                self.open_partition()

            # write the record to the file
            self.file.write(serialized.encode())
            self.records_in_partition += 1

        return self.records_in_partition

    def commit(self):
        if self.bytes_in_partition > 0:
            with threading.Lock():
                try:
                    self.file.flush()
                    self.file.close()
                except ValueError:
                    pass

                if self.file is not None:
                    committed_partition_name = self.inner_writer.commit(source_file_name=self.file_name)
                    get_logger().debug(F"Partition Committed - {committed_partition_name} - {self.records_in_partition} records, {self.bytes_in_partition} bytes")
                    try:
                        os.remove(self.file_name)
                    except ValueError:
                        pass

                self.bytes_in_partition = 0
                self.file_name = None

    def open_partition(self):
        self.file_name = self.create_temp_file_name()
        self.file: Any = open(self.file_name, mode='wb', buffering=BUFFER_SIZE)
        if self.compress:
            self.file = lzma.open(self.file, mode='wb')
        self.bytes_in_partition = 0
        self.records_in_partition = 0

    def __del__(self):
        try:
            self.commit()
        except Exception as e:
            get_logger().error(f"Error whilst destroying partition - {type(e).__name__} - {e}")

    def create_temp_file_name(self):
        """
        Create a tempfile, get the name and then deletes the tempfile.

        The behaviour of tempfiles is inconsistent between operating systems,
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
