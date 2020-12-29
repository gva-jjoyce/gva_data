"""
Minio Reader - may work with AWS
"""
from typing import Iterator, Tuple, Optional, List
import datetime
from ...utils import paths, common
import lzma
from .base_reader import BaseReader
from ..formats import dictset
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass


class MinioReader(BaseReader):

    def __init__(
            self,
            end_point: str,
            access_key: str,   # 57BTIM68ETSQ7ZQG
            secret_key: str,   # LXWODW6DSZX9AD9TX9XBTW292KEOATGB
            **kwargs):
        super().__init__(**kwargs)

        secure = kwargs.get('secure', True)
        self.minio = Minio(end_point, access_key, secret_key, secure=secure)

            
    def list_of_sources(self):

        bucket, object_path, _, _ = paths.get_parts(self.from_path)

        for cycle_date in common.date_range(self.start_date, self.end_date):

            cycle_path = paths.build_path(path=object_path, date=cycle_date)

            blobs = self.minio.list_objects(
                    bucket_name=bucket,
                    prefix=cycle_path,
                    recursive=True)
            for obj in blobs:
                yield bucket + '/' + obj.object_name


    def read_from_source(self, object_name):

        bucket, object_path, name, extention = paths.get_parts(object_name)

        stream = self.minio.get_object(bucket, object_path + name + extention)

        # untested compression routines
        if extention == '.lzma':
            stream = lzma.decompress(stream)
            

        for item in stream.readlines():
            yield item.decode()
