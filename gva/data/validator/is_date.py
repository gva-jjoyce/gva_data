import datetime
from typing import Any

def is_date(value: Any) -> bool:
    """
    Test if a variable is a valid date. Strings should be in iso format.
    """
    try:
        if type(value).__name__ in ("datetime", "date", "time"):
            return True
        datetime.datetime.fromisoformat(value)
        return True
    except (ValueError, TypeError):
        return False