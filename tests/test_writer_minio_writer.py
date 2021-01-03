import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.writers import minio_writer, Writer
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass


if __name__ == "__main__":
    
    w = Writer(
            writer=minio_writer,
            end_point=os.getenv('MINIO_END_POINT'),
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False,
            #compress=True,
            to_path='TWITTER/%date/test.jsonl')

    import time

    start = time.time_ns()

    for i in range(100):
        w.append({"tv":i+100})
    w.finalize()

    print('okay', (start - time.time_ns())/1e9)