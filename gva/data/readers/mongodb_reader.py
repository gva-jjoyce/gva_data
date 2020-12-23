"""
A MongoDB Reader

This is a light-weight MongoDB reader to fulfil a specific purpose,
it needs some work to make it fully reusable.

collection can have date formatted
"""
from typing import Iterator, Tuple, Optional, List
import datetime
try:
    import pymongo  # type:ignore
except ImportError:
    pass


def _iterate_by_chunks(collection, chunksize=1, start_from=0, query={}):
    chunks = range(start_from, collection.find(query).count(), int(chunksize))
    num_chunks = len(chunks)
    for i in range(1,num_chunks+1):
        if i < num_chunks:
            yield collection.find(query)[chunks[i-1]:chunks[i]]
        else:
            yield collection.find(query)[chunks[i-1]:chunks.stop]


def mongodb_reader(
        path: str = "",
        connection: str = "",
        collection: str = "",
        connectionstring: str = "",
        query: dict = {},
        chunk_size: int = 5000,
        date_range: Tuple[Optional[datetime.date], Optional[datetime.date]] = (None, None)
        ) -> Iterator:

    # if dates aren't provided, use today
    start_date, end_date = date_range
    if not end_date:
        end_date = datetime.date.today()
    if not start_date:
        start_date = datetime.date.today()

    conn = pymongo.MongoClient(connectionstring)
    db = conn[connection]

    # cycle through each day in the range
    for cycle in range(int((end_date - start_date).days) + 1):
        cycle_date = start_date + datetime.timedelta(cycle)

        collection_name = cycle_date.strftime(collection)
        collection_object = db[collection_name]

        mess_chunk_iter = _iterate_by_chunks(collection_object, chunk_size, 0, query=query)
        
        for docs in mess_chunk_iter:
            for doc in docs:
                yield doc
