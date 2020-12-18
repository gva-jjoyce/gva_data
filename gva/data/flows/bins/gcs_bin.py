"""
Base Bin

Implements common functions for each of the bins.

Bins are where information that needs to be saved but isn't part of the
flow itself.

Two bins are supported initially:
- ErrorBin - where records unable to be processed are written so they aren't
  lost when they are dropped from the flow
- TraceBin - where trace information is written to be persisted.

This Bin is written to support only writing to Google Cloud Storage.
"""
from google.cloud import storage  # type:ignore
import time
import random


class GoogleCloudStorageBin():

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
        blob_name = F"{self.path}/{time.time_ns()}-{random.randrange(0,9999):04d}.txt"  # nosec - not crypto
        blob = self.bucket.blob(blob_name)
        blob.upload_from_string(record)

    def __ror__(self, flow):
        # set a attribute on the flow which calls this class
        setattr(flow, str(self), self)
