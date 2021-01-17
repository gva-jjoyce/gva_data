import os
from .internals.base_writer import BaseWriter
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass


class MinIOWriter(BaseWriter):

    def __init__(
            self,
            end_point,
            access_key,
            secret_key,
            secure: bool = False,
            **kwargs):
        super().__init__(**kwargs)

        self.client = Minio(end_point, access_key, secret_key, secure=secure)

    def _build_path(self, index):
        return f"{self.filename_without_bucket}-{index:04d}{self.extention}"

    def commit(
            self,
            source_file_name):

        existing_items = self.get_partition_list()
        # avoid collisions
        collision_tests = 0
        maybe_colliding_filename = self._build_path(collision_tests)

        while maybe_colliding_filename in existing_items:
            collision_tests += 1
            maybe_colliding_filename = self._build_path(collision_tests)

        # put the file using the MinIO API
        with open(source_file_name, 'rb') as file_data:
            file_stat = os.stat(source_file_name)
            self.client.put_object(
                    self.bucket,
                    maybe_colliding_filename,
                    file_data,
                    file_stat.st_size)

        return maybe_colliding_filename

    def get_partition_list(self):
        existing_items = {obj.object_name for obj in self.client.list_objects(bucket_name=self.bucket, prefix=self.filename_without_bucket)}
        return existing_items





