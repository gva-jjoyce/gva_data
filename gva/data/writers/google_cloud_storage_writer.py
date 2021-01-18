from .internals.base_writer import BaseWriter
try:
    from google.cloud import storage  # type:ignore
except ImportError:
    pass


class GoogleCloudStorageWriter(BaseWriter):
    def __init__(
            self,
            project: str,
            **kwargs):
        super().__init__(**kwargs)

        client = storage.Client(project=project)
        self.gcs_bucket = client.get_bucket(self.bucket)
        self.filename = self.filename_without_bucket

    def get_partition_list(self):
        blob_list = self.gcs_bucket.list_blobs(prefix=self.filename)
        return [blob.name for blob in blob_list]

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

        blob = self.gcs_bucket.blob(maybe_colliding_filename)
        blob.upload_from_filename(source_file_name)

        return maybe_colliding_filename
