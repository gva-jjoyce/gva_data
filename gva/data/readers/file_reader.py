"""
File System Reader
"""
from typing import Iterator, Tuple, Optional, List
import datetime
from ...utils import paths
import lzma
from ...utils import common
from .internals import BaseReader
import glob
from os.path import isfile, exists


class FileReader(BaseReader):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self.extention = kwargs.get('extention', '.jsonl')
        self.chunk_size = kwargs.get('chunk_size', 16*1024*1024)
        self.delimiter = kwargs.get('delimiter', '\n')
        self.encoding = kwargs.get('encoding', 'utf8')


    def _inner_file_reader(self, file_name: str):
        """
        Read an uncompressed file in chunks
        """
        with open(file_name, 'r', encoding=self.encoding) as f:
            carry_forward = ""
            chunk = "INITIALIZED"
            while len(chunk) > 0:
                chunk = f.read(self.chunk_size)
                augmented_chunk = carry_forward + chunk
                lines = augmented_chunk.split(self.delimiter)
                carry_forward = lines.pop()
                yield from lines
            if carry_forward:
                yield carry_forward


    def _inner_compressed_file_reader(self, file_name: str):
        """
        Read an entire compressed file at once.
        """
        with lzma.open(file_name, 'r') as f:
            yield from f.readlines()


    def list_of_sources(self):
        # cycle through each day in the range
        for cycle_date in common.date_range(self.start_date, self.end_date):

            # build the path name - it says 'blob' but works for filesystems
            cycle_path = paths.build_path(path=self.from_path, date=cycle_date)

            # get the list of files at that path
            if exists(cycle_path):  # skip non-existant folders
                files = glob.iglob(cycle_path + '**', recursive=True)
                yield from [f.replace('\\', '/') 
                        for f in files 
                        if isfile(f) 
                                and self.extention in f]


    def read_from_source(self, file_name: str):

        if file_name.endswith('.lzma'):
            reader = self._inner_compressed_file_reader(file_name=file_name)
        else:
            reader = self._inner_file_reader(file_name=file_name)
        yield from reader
