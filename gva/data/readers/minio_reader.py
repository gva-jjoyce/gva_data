"""
MinIo Reader - may work with AWS

There is a problem with with Minio library which prevents it from being used
with the Multi-Processor library to distribute the reading across multiple 
processes. The MinioReader code is near identical the other readers so the
conclusion is the issue is with the Minio library.
"""
from ...utils import paths, common
import lzma
import io
from .base_reader import BaseReader
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass


class MinioReader(BaseReader):

    def __init__(
            self,
            end_point: str,
            access_key: str,
            secret_key: str,
            **kwargs):
        super().__init__(**kwargs)

        secure = kwargs.get('secure', True)
        self.minio = Minio(end_point, access_key, secret_key, secure=secure)


    def list_of_sources(self):
        bucket, object_path, _, _ = paths.get_parts(self.from_path)
        for cycle_date in common.date_range(self.start_date, self.end_date):
            cycle_path = paths.build_path(path=object_path, date=cycle_date)
            objects = self.minio.list_objects(
                    bucket_name=bucket,
                    prefix=cycle_path,
                    recursive=True)
            for obj in objects:
                yield bucket + '/' + obj.object_name


    def read_from_source(self, object_name):
        bucket, object_path, name, extention = paths.get_parts(object_name)
        stream = self.minio.get_object(bucket, object_path + name + extention).read()

        if extention == '.lzma':
            # converting to an IO stream is about 10% faster
            io_stream = io.BytesIO(stream)
            with lzma.open(io_stream, 'rb') as file:
                yield from file
        else:
            for item in stream.splitlines():
                yield item.decode()
