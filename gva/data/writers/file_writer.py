import glob
import os
import shutil
from ...utils import paths
from .internals.base_writer import BaseWriter


class FileWriter(BaseWriter):

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def _build_path(self, index):
        return f"{self.filename}-{index:04d}{self.extention}"

    def commit(
            self,
            source_file_name):
        existing_partitions = self.get_partition_list()

        collision_tests = 0
        maybe_colliding_filename = self._build_path(collision_tests)

        while maybe_colliding_filename in existing_partitions:
            collision_tests += 1
            maybe_colliding_filename = self._build_path(collision_tests)

        bucket, path, filename, ext = paths.get_parts(maybe_colliding_filename)
        os.makedirs(bucket + '/' + path, exist_ok=True)

        # save
        return shutil.copy(source_file_name, maybe_colliding_filename)

    def get_partition_list(self):
        return glob.glob(self.filename + '**', recursive=True)
