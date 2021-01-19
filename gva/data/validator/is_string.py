from typing import Any
import re

class is_string():
    __slots__ = ('pattern', 'regex')

    def __init__(self, **kwargs):
        self.regex = None
        self.pattern = kwargs.get('format')
        if self.pattern:
            self.regex = re.compile(self.pattern)

    def __call__(self, value: Any) -> bool:
        if self.pattern is None:
            return type(value).__name__ == "str"
        else:
            return self.regex.match(str(value))

    def __str__(self):
        if self.pattern:
            return f'string ({self.pattern})'
        else:
            return 'string'