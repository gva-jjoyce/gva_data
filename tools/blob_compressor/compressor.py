"""

"""

# find files which aren't compressed
# compress them
from lzma import LZMAFile
import tempfile
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '../..'))
from gva.data.readers import MinioReader
from gva.data.writers import minio_writer

def _get_temp_file_name():
    """
    Create a tempfile, get the name and then deletes the tempfile.

    The behaviour of tempfiles is inconsistent between operating systems,
    this helps to ensure consistent behaviour.
    """
    file = tempfile.NamedTemporaryFile(
            prefix='gva-',
            delete=True)
    file_name = file.name
    file.close()
    try:
        os.remove(file_name)
    except OSError:
        pass

    return file_name

store = MinioReader(
    end_point=os.getenv('MINIO_END_POINT'),
    access_key=os.getenv('MINIO_ACCESS_KEY'),
    secret_key=os.getenv('MINIO_SECRET_KEY'),
    from_path='TWITTER/tweets/year_2021/month_01/day_02/',
    secure=False
)

for source in store.list_of_sources():

    print(source)

    out_file = _get_temp_file_name()


    with LZMAFile(out_file, 'wb') as lzma_file:
        for line in store.read_from_source(source):
            lzma_file.write(line.encode())

    writer = minio_writer(
        source_file_name=out_file,
        target_path='TWITTER/tweets/year_2021/month_01/day_02_test/twitter_2021-01-02.jsonl',
        end_point=os.getenv('MINIO_END_POINT'),
        access_key=os.getenv('MINIO_ACCESS_KEY'),
        secret_key=os.getenv('MINIO_SECRET_KEY'),
        secure=False,
        add_extension='.lzma')

    print('done', writer)

print('complete')

