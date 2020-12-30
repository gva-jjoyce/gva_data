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
            end_point='10.10.10.30:9000',
            access_key='57BTIM68ETSQ7ZQG',
            secret_key='LXWODW6DSZX9AD9TX9XBTW292KEOATGB',
            secure=False,
            #compress=True,
            to_path='TWITTER/%date/test.jsonl')

    for i in range(100):
        w.append({"tv":i+100})
    w.finalize()

    print('okay')