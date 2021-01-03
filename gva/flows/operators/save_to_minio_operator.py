from . import BaseOperator
from gva.data.writers import minio_writer, Writer # type:ignore
from gva.data.validator import Schema  # type:ignore
import datetime


class SaveToMinioOperator(BaseOperator):

    def __init__(
            self,
            *,
            end_point: str,
            to_path: str,
            access_key: str,
            secret_key: str,
            schema: Schema = None,
            compress: bool = True,
            date: datetime.date = None,
            secure: bool = True,
            **kwargs):
        super().__init__()
        self.writer = Writer(
                writer=minio_writer,
                to_path=to_path,
                schema=schema,
                compress=compress,
                date=date,
                end_point=end_point,
                access_key=access_key,
                secret_key=secret_key,
                secure=secure,
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
