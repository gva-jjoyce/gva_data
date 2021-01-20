"""
Enumerator Test
"""
from typing import Any

class is_valid_enum():
    """
    Test if a variable is on a list of valid values
    """
    __slots__ = ('symbols')

    def __init__(self, **kwargs):
        """
        -> "type": "enum", "symbols": ["up", "down"]

        symbols: list of allowed values (case sensitive)
        """
        self.symbols = kwargs.get('symbols', ())

    def __call__(self, value: Any) -> bool:
        return value and value in self.symbols

    def __str__(self):
        return f'enum {self.symbols}'