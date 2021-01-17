"""
Threaded Reader

This wraps the reader classes to read multiple partitions concurrently.

Internally it uses Queues to create the set of partitions to read from
and to communicate what has been read. The number of threads, the number
of items to put on the queues are limited - otherwise the readers are
likely to exhaust the memory available and crash the app.

The threaded reader is opinionated for datasets partitioned on filesystems
or in blobs, where one dataset is split over multiple resources able to be
concurrently read and there is a a delay associated with reading the data,
such as reading over a network or slow storage. Use in other situations
(for example the MongoDB reader or where files are on local SSD storage)
may be detrimental to performance.

The number of Queue slots and the size of the chunks have had various
combinations tried, there may be performance improvements with larger
numbers for each, it's noth within the resolution of my system to be
able to measure them.
"""
import queue
import threading
import sys
import time
from ...formats import dictset

def threaded_reader(items_to_read, reader, max_threads=4):
    """
    Speed up reading sets of files - such as multiple days worth
    of log-per-day files.

    If you care about the order of the records, don't use this.

    Each file is in it's own thread, so reading a single file 
    wouldn't benefit from this approach.
    """
    thread_pool = []

    def thread_process():
        """
        The process inside the threads.

        1) Get any files off the file queue
        2) Read the file in chunks
        3) Put a chunk onto a reply queue
        """
        try:
            source = source_queue.pop(0)
        except IndexError:
            source = None
        while source:
            source_reader = reader.read_from_source(source)
            for chunk in dictset.page_dictset(source_reader, 256):
                reply_queue.put(chunk)  # this will wait until there's a slot
            try:
                source = source_queue.pop(0)
            except IndexError:
                source = None


    source_queue = items_to_read.copy()

    # scale the number of threads, if we have more than the number
    # of files we're reading, will have threads that never complete
    t = min(len(source_queue), max_threads, 8)
    reply_queue = queue.Queue(t * 8)

    # start the threads
    for _ in range(t):
        thread = threading.Thread(target=thread_process)
        thread.daemon = True
        thread.start()
        thread_pool.append(thread)
        time.sleep(0.01)  # offset the start of the threads

    # when the threads are all complete and all the records
    # have been read from the reply queue, we're done
    while any([t.is_alive() for t in thread_pool]) or not(reply_queue.empty()):
        records = reply_queue.get(timeout=10)  # don't wait forever
        yield from records
