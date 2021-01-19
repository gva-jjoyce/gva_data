from .is_valid_enum import is_valid_enum
from typing import Any

VALID_BOOLEAN_VALUES = ("true", "false", "on", "off", "yes", "no", "0", "1")

class is_boolean(is_valid_enum):
    def __init__(self, **kwargs):
        """
        is_boolean is a specific case of is_valid_enum
        - it defaults to a set of true/false values
        - the check is case insensitive
        """
        super().__init__()
        if len(self.symbols) == 0:
            self.symbols = VALID_BOOLEAN_VALUES

    def __call__(self, value: Any) -> bool:
        return super().__call__(str(value).lower())