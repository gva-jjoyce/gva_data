"""
Reader is about 300% faster (8 processes) than the default serial reader.


Problems

- The .limit method causes this reader to never end ()
- Race condition in determine if the read is complete will loose from records
"""
import queue
import multiprocessing
import sys
import time
from ..formats import dictset
from ...logging import get_logger


def _inner_parse(parser, chunk, condition):
    for item in chunk:
        item = parser(item)
        if condition(item):
            yield item

def inner_process(flag, reader, source_queue, reply_queue, parser, where):

    try:
        source = source_queue.get(timeout=0.1)
    except Exception:
        source = None

    while source is not None:
        source_reader = reader.read_from_source(source)
        for chunk in dictset.page_dictset(source_reader, 256):
            reply_queue.put(list(_inner_parse(parser, chunk, where)))  # wait
        try:
            source = source_queue.get(timeout=0.1)
        except Exception:
            source = None

    #get_logger().debug('close flag')
    flag.value = 0


def processed_reader(items_to_read, reader, parser, where):

    process_pool = []

    send_queue = multiprocessing.Queue()
    for item in items_to_read:
        send_queue.put(item)

    # limit the number of slots
    slots = min(8, len(items_to_read), multiprocessing.cpu_count())
    reply_queue = multiprocessing.Queue(slots * 8)

    for _ in range(slots):
        flag = multiprocessing.Value('i', 1)
        process = multiprocessing.Process(
                target=inner_process, 
                args=(flag, reader, send_queue, reply_queue, parser, where))
        process.daemon = True
        process.start()
        process_pool.append(flag)

    process_pool = set(process_pool)

    # this may end the loop before all of the data is read
    while any({t.value == 1 for t in process_pool}) or not(reply_queue.empty()):
        #get_logger().debug(F"{ {t.value == 1 for t in process_pool} }, {reply_queue.empty()}, {send_queue.empty()}")
        try:
            records = reply_queue.get(timeout=0.1)
            yield from records
        except Exception:  # nosec
            pass
