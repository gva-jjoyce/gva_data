"""
Create .serialize and .parse methods to handle json operations

Where orjson is installed, the performance impact is nil,
without orjson, parsing is about as fast as ujson, however
serialization is slower, although still faster than the 
native json library. 
"""
from typing import Any
import datetime
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

    print('using backup json parser')

    def serialize(obj: Any) -> bytes:

        def fix_dates(dt):
            if isinstance(dt, (datetime.date, datetime.datetime)):
                return dt.isoformat()
            return dt

        obj_copy = obj.copy()

        if isinstance(obj_copy, dict):
            obj_copy = {k:fix_dates(v) for k,v in obj_copy.items()}

        # ujson returns a string, orjson
        return ujson.dumps(obj_copy).encode("utf8")

    parse = ujson.loads
