"""
MinIO Bin

May support AWS S3 - untested
"""
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass
import time
import random
from .base_bin import BaseBin


class MinioBin(BaseBin):

    def __init__(
            self,
            bin_name: str,
            project: str,
            bucket: str,
            path: str):
        client = storage.Client(project=project)
        self.bucket = client.get_bucket(bucket)
        self.path = path
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    # does as little as possible to commit the record
    def __call__(
            self,
            record: str):
        # to reduce collisions we get the time in nanoseconds
        # and a random number between 1 and 1000
        object_name = F"{self.path}/{time.time_ns()}-{random.randrange(0,9999):04d}.txt"  # nosec - not crypto
        blob = self.bucket.blob(object_name)
        blob.upload_from_string(record)
