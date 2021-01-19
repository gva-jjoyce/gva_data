from typing import Any

def is_list(value: Any) -> bool:
    return type(value).__name__ == 'list'