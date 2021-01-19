import datetime
from typing import Any

def is_date(value: Any) -> bool:
    try:
        if type(value).__name__ in ("datetime", "date", "time"):
            return True
        datetime.datetime.fromisoformat(value)
        return True
    except (ValueError, TypeError):
        return False