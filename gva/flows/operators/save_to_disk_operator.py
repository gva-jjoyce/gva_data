from . import BaseOperator
from gva.data.writers import file_writer, Writer # type:ignore
from gva.data.validator import Schema  # type:ignore
import datetime


class SaveToDiskOperator(BaseOperator):

    def __init__(
            self,
            *,
            to_path: str,
            schema: Schema = None,
            compress: bool = True,
            date: datetime.date = None,
            **kwargs):
        super().__init__()
        self.writer = Writer(
                writer=file_writer,
                to_path=to_path,
                schema=schema,
                compress=compress,
                date=date,
                **kwargs)

    def execute(self, data: dict = {}, context: dict = {}):
        self.writer.append(data)
        return data, context

    def finalize(self):
        self.writer.finalize()

    def __del__(self):
        try:
            self.writer.finalize()
        except Exception:  # nosec - if this fails, it should be ignored here
            pass
