"""
A MongoDB Reader

This is a light-weight MongoDB reader to fulfil a specific purpose,
it needs some work to make it fully reusable.

collection can have date formatted
"""
from typing import Iterator, Tuple, Optional, List
import datetime
from .internals import BaseReader
try:
    import pymongo     # type:ignore
except ImportError:    # pragma: no cover
    pass


class MongoDbReader(BaseReader):

    def __init__(
            self,
            connection_string: str,
            database: str,
            **kwargs):
        connection = pymongo.MongoClient(connection_string)
        self.database = connection[database]

        # chunk size affects memory usage
        self.chunk_size = kwargs.get('chunk_size', 10000)
        self.query = kwargs.get('query', {})


    def list_of_sources(self):
        yield from self.database.list_collection_names()


    def read_from_source(self, item: str):
        collection = self.database[item]
        chunks = self._iterate_by_chunks(
                collection,
                self.chunk_size,
                0,
                query=self.query)
        for docs in chunks:
            yield from docs


    def _iterate_by_chunks(self, collection, chunksize=1, start_from=0, query={}):
        chunks = range(start_from, collection.find(query).count(), int(chunksize))
        num_chunks = len(chunks)
        for i in range(1,num_chunks+1):
            if i < num_chunks:
                yield collection.find(query)[chunks[i-1]:chunks[i]]
            else:
                yield collection.find(query)[chunks[i-1]:chunks.stop]

