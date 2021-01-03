"""
Save To Bucket Operator

This is essentially a wrapper around the gva.data.writer/blob_writer
implementation. This will work best when used in conjuction with the
gva.data.reader library.

This works by saving entries to a local cache file and when a volume
of data has been written (default 8Mb) the cache is saved to it's
target destination. These 8Mb blocks are partitions, these are not
intended to be read or accessed directly.

The Writer will automatically LZMA encrypt the data, this can be disabled
by setting the compress parameter to False. If the data is encrypted, the
Reader will automatically decrypt. The need to decrypt is signalled by the
.lzma file extention. LZMA files can be opened by compression tools such
as 7-zip and lzma.exe so is not dependant on the Reader library to be read.
The .lzma extention is added automatically, it will be added twice if
specified as part of the to_path.

The Writer will avoid clashes in filenames by appending a a four digit
number to the end of the filename, starting 0000. If 0000 already exists
it will use 0001, etc to 9999.

If to_path contains date placeholders, (e.g. %date), within a few seconds
of midnight, the Writer will conclude it's current file and create a new
one. Writes within a few seconds of midnight may appear in the previous
day's file.

Writes are optionally validated against a schema, this only works for
dictionaries; the schema definition documentation is in the
gva.validator library.

The Writer will automatically close a partition, even if not full, if
no new records have been added to the partition in 60 seconds.
"""
from .base_operator import BaseOperator
from gva.data import Writer  # type:ignore
from gva.data.validator import Schema  # type:ignore
import datetime


class SaveToBucketOperator(BaseOperator):

    def __init__(
            self,
            *,
            project: str,
            to_path: str,
            schema: Schema = None,
            compress: bool = True,
            date: datetime.date = None,
            **kwargs):
        super().__init__()
        self.writer = Writer(
                project=project,
                to_path=to_path,
                schema=schema,
                compress=compress,
                date=date,
                **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        self.writer.append(data)

    def finalize(self):
        self.writer.finalize()

    def __del__(self):
        try:
            self.writer.finalize()
        except Exception:  # nosec - if this fails, it should be ignored here
            pass
