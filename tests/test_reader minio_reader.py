"""
Test the file reader
"""
import datetime
import time
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.readers import MinioReader, Reader
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


if __name__ == "__main__":

    r = Reader(
        thread_count=4,
        select=['username'],
        from_path='TWITTER/tweets/%datefolders',
        where=lambda r: 'Trump' in r['username'],
        reader_class=MinioReader,
        end_point='10.10.10.30:9000',
        access_key='57BTIM68ETSQ7ZQG',
        secret_key='LXWODW6DSZX9AD9TX9XBTW292KEOATGB',
        start_date=datetime.date(2020,1,31),
        end_date=datetime.date(2020,1,31),
        secure=False
    )

    start = time.time_ns()

    for i, line in enumerate(r):
        pass
    print(i, (time.time_ns() - start) / 1e9)


def end():    
    m = MinioReader(
            end_point='10.10.10.30:9000',
            access_key='57BTIM68ETSQ7ZQG',
            secret_key='LXWODW6DSZX9AD9TX9XBTW292KEOATGB',
            from_path='TWITTER/tweets/%datefolders',
            start_date=datetime.date(2020,1,31),
            end_date=datetime.date(2020,1,31),
            secure=False)

    for item in m.list_of_sources():
        print('item:', item)
        for i, line in enumerate(m.read_from_source(item)):
            pass
        print(i)

    print('okay')
    