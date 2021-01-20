from typing import Any

# 64 bit signed - not a limit, just a default
DEFAULT_MIN = -9223372036854775808
DEFAULT_MAX = 9223372036854775807


class is_numeric():
    """
    Test if a variable is a valid number, optionally between a min and max.
    """
    __slots__ = ('min', 'max')

    def __init__(self, **kwargs):
        
        """
        -> "type": "numeric", "min": 0, "max": 100

        min: low end of valid range
        max: high end of valid range
        """
        self.min = kwargs.get('min') or DEFAULT_MIN
        self.max = kwargs.get('max') or DEFAULT_MAX

    def __call__(self, value: Any) -> bool:
        try:
            n = float(value)
        except (ValueError, TypeError):
            return False
        return self.min <= n <= self.max

    def __str__(self):
        if self.min == DEFAULT_MIN and self.max == DEFAULT_MAX:
            return 'numeric'
        if not self.min == DEFAULT_MIN and not self.max == DEFAULT_MAX:
            return f'numeric ({self.min} - {self.max})'
        if not self.min == DEFAULT_MIN and self.max == DEFAULT_MAX:
            return f'numeric (> {self.min})'
        if self.min == DEFAULT_MIN and not self.max == DEFAULT_MAX:
            return f'numeric (<{self.max})'
