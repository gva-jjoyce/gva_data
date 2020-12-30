"""
MinIO Bin Writer

May support AWS S3 - untested
"""
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass
import time
import random
import io
from .base_bin import BaseBin


class MinioBin(BaseBin):

    def __init__(
            self,
            bin_name: str,
            end_point: str,
            bucket: str,
            path: str,
            access_key: str,
            secret_key: str,
            secure: bool = True):

        self.client = Minio(end_point, access_key, secret_key, secure=secure)
        self.bucket = bucket
        self.path = path
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    def __call__(
            self,
            record: str):
        # to reduce collisions we get the time in nanoseconds
        # and a random number between 1 and 1000
        object_name = F"{self.path}/{time.time_ns()}-{random.randrange(0,9999):04d}.txt"  # nosec - not crypto

        record_bytes = record.encode('utf-8')
        record_stream = io.BytesIO(record_bytes)

        self.client.put_object(
                self.bucket,
                object_name,
                record_stream,
                file_size=len(record_bytes))

        return object_name
