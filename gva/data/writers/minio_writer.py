from typing import Optional
import datetime
import os
from ...utils import paths
try:
    from minio import Minio  # type:ignore
except ImportError:
    pass


def minio_writer(
        source_file_name: str,
        target_path: str,
        date: Optional[datetime.date] = None,
        add_extention: str = '',
        **kwargs):

    # read key word arguments
    end_point = kwargs.get('end_point')
    access_key = kwargs.get('access_key')
    secret_key = kwargs.get('secret_key')
    secure = kwargs.get('secure', True)

    if date is None:
        date = datetime.datetime.today()

    client = Minio(end_point, access_key, secret_key, secure=secure)
    bucket, path, filename, extention = paths.get_parts(target_path)

    # we need this a few times, so build it once
    date_prefix = paths.date_format(f"{path}{filename}", date)

    # get all of the existing objects once, testig for existance is SLOW
    existing_items = {o.object_name for o in client.list_objects(bucket_name=bucket, prefix=date_prefix)}

    # avoid collisions
    collision_tests = 0
    maybe_colliding_filename = paths.date_format(f"{date_prefix}-{collision_tests:04d}{extention}{add_extention}", date)

    while maybe_colliding_filename in existing_items:
        collision_tests += 1
        maybe_colliding_filename = paths.date_format(f"{date_prefix}-{collision_tests:04d}{extention}{add_extention}", date)

    # put the file using the MinIO API
    with open(source_file_name, 'rb') as file_data:
        file_stat = os.stat(source_file_name)
        client.put_object(
                bucket,
                maybe_colliding_filename,
                file_data,
                file_stat.st_size)

    return maybe_colliding_filename