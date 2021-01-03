"""

"""

# find files which aren't compressed
# compress them

from gva.data.readers import MinioReader
from gva.data.writers import minio_writer
import lzma
import os

store = MinioReader(
    end_point=os.getenv('MINIO_END_POINT'),
    access_key=os.getenv('MINIO_ACCESS_KEY'),
    secret_key=os.getenv('MINIO_SECRET_KEY'),
    from_path=''
)

for source in store.list_of_sources():
    pass
    # open lzma fila

    for line in store.read_from_source(source):
        pass
        # add a \n to the line
        # add the line to the file

    #writer = minio_writer(
        # source file
    #)

