"""
Null Writer

Impotent writer for testing, writes to the log to help with debugging.
"""
from ...logging import get_logger
from .internals.base_writer import BaseWriter


class NullWriter(BaseWriter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        kwargs_passed = [f"{k}={v!r}" for k, v in kwargs.items()]
        self.formatted_args = ", ".join(kwargs_passed)

    def commit(
            self,
            source_file_name):
        get_logger().debug(f'null_writer({self.formatted_args}, source_file_name={source_file_name})')
        return "NullWriter"

    def get_partition_list(self):
        return []
