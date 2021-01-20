from typing import Any

def is_list(value: Any) -> bool:
    """
    Test if a variable is a valid list
    """
    return type(value).__name__ == 'list'