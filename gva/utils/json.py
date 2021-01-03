"""
Create .serialize and .parse methods to handle json operations

Where orjson is installed, the performance impact is nil,
without orjson, parsing is about as fast as ujson, however
serialization is slower, although still faster than the 
native json library. 
"""
from typing import Any
import datetime
from ..logging import get_logger
try:
    # if orjson is available, use it
    # we will optimize for orjson and adjust ujson to match
    import orjson

    parse = orjson.loads
    serialize = orjson.dumps

except ImportError:  # pragma: no cover
    # orjson doesn't install on 32bit systems so we need a backup plan
    # however, orjson and ujson have functional differences so we can't
    # just swap the references.
    import ujson

    logger = get_logger()
    logger.warning('orjson not installed using ujson')

    def serialize(obj: Any) -> bytes:   # type:ignore

        def fix_fields(dt: Any) -> str:
            """
            orjson and ujson handles some fields differently,
            if one of those fields is detected, fix it. 
            """
            if isinstance(dt, (datetime.date, datetime.datetime)):
                return dt.isoformat()
            return dt

        if isinstance(obj, dict):
            obj_copy = {k:fix_fields(v) for k,v in obj.items()}

        # ujson returns a string, orjson
        return ujson.dumps(obj_copy).encode("utf8")

    parse = ujson.loads  # type:ignore
