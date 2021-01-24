"""
Writers are the target specific implementations which commit a temporary file
created by the PartitionWriter to different systems, such as the filesystem,
Google Cloud Storage or MinIO.

The primary activity is contained in the .commit() method.
"""
from ....utils import paths
import abc


class BaseWriter(abc.ABC):

    def __init__(
            self,
            to_path: str,
            **kwargs):

        self.bucket, path, filename, self.extension = paths.get_parts(to_path)

        if self.bucket == '/':
            self.bucket = ''
        if path == '/':
            path = ''

        self.filename = self.bucket + '/' + path + filename
        self.filename_without_bucket = path + filename
        if len(self.extension) == 0 or self.extension is None:
            self.extension = '.jsonl'
        if kwargs.get('compress', False):
            self.extension = self.extension + '.lzma'

    def _build_path(self, index):
        return f"{self.filename}-{index:04d}{self.extension}"

    @abc.abstractclassmethod
    def commit(
            self,
            source_file_name):
        pass

    @abc.abstractclassmethod
    def get_partition_list(self):
        pass
