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
            to_path: str):

        self.bucket, path, filename, self.extention = paths.get_parts(to_path)
        self.filename = self.bucket + '/' + path + filename
        self.filename_without_bucket = path + filename

    @abc.abstractclassmethod
    def commit(
            self,
            source_file_name):
        pass

    @abc.abstractclassmethod
    def get_partition_list(self):
        pass
