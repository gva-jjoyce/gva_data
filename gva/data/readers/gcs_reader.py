try:
    from google.cloud import storage  # type:ignore
except ImportError:   # pragma: no cover
    pass
import lzma
import datetime
from ...utils import paths
from typing import Tuple, Union, Optional
from ...logging import get_logger
from ...utils import common
from .base_reader import BaseReader


class GoogleCloudStorageReader(BaseReader):


    def __init__(self, project: str, **kwargs):
        super().__init__(**kwargs)
        self.project = project


    def list_of_sources(self):

        bucket, object_path, _, extention = paths.get_parts(self.from_path)

        for cycle_date in common.date_range(self.start_date, self.end_date):
            cycle_path = paths.build_path(path=object_path, date=cycle_date)
            blobs = find_blobs_at_path(project=self.project, bucket=bucket, path=cycle_path, extention=extention)
            for obj in blobs:
                yield bucket + '/' + obj.name


    def read_from_source(self, object_name):
        bucket, object_path, name, extention = paths.get_parts(object_name)

        blob = get_blob(project=self.project, bucket=bucket, blob_name=object_path + name + extention)
        stream = blob.download_as_string()

        if extention == '.lzma':
            stream = lzma.decompress(stream)
            
        stream = stream.decode('utf-8')
        yield from [item for item in stream.split('\n') if len(item) > 0]
    

def find_blobs_at_path(
        project: str,
        bucket: str,
        path: str,
        extention: str):

    client = storage.Client(project=project)
    gcs_bucket = client.get_bucket(bucket)
    blobs = client.list_blobs(bucket_or_name=gcs_bucket, prefix=path)
    if extention:
        blobs = [blob for blob in blobs if extention in blob.name]
    yield from blobs


def get_blob(
        project: str,
        bucket: str,
        blob_name: str):

    client = storage.Client(project=project)
    gcs_bucket = client.get_bucket(bucket)
    blob = gcs_bucket.get_blob(blob_name)
    return blob
