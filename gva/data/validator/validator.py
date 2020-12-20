"""
Schema Validation

Tests a dictionary against a schema to test for conformity.
Schema definition is similar to - but not the same as - avro schemas

Supported Types:
    - string - a character sequence
    - numeric - a number
    - int - alias for numeric
    - date - a datetime.date or an iso format date or time
    - boolean - a boolean or a binary value (true/false, on/off, yes/no)
    - other - not one of the above, but a required field
    - null - Python Falsy (None, 0, Empty String, etc)

Example Schema:
{
 "name": "Table Name",
 "fields": [
     {"name": "id", "type": "string"},
     {"name": "country",  "type": ["string", "null"]},
     {"name": "followers", "type": ["string", "null"]}
 ]
}
"""
import datetime
import json
from typing import List, Any, Union, Callable
import os


VALID_BOOLEAN_VALUES = ("true", "false", "on", "off", "yes", "no", "0", "1")


def _is_string(value: Any) -> bool:
    return type(value).__name__ == "str"


def _is_boolean(value: Any) -> bool:
    return str(value).lower() in VALID_BOOLEAN_VALUES


class _is_numeric():
    __slots__ = ['min', 'max']
    def __init__(self, min: int, max: int):
        self.min = min
        self.max = max
    def __call__(self, value: Any) -> bool:
        try:
            n = float(value)
        except ValueError:
            return False
        except TypeError:
            return False
        return (n >= self.min) and (n <= self.max)
    def __str__(self):
        if self.min == -9223372036854775808 and self.max == 9223372036854775807:
            return 'numeric'
        if not self.min == -9223372036854775808 and not self.max == 9223372036854775807:
            return f'numeric ({self.min} - {self.max})'
        if not self.min == -9223372036854775808 and self.max == 9223372036854775807:
            return f'numeric ({self.min} - infinity)'
        if self.min == -9223372036854775808 and not self.max == 9223372036854775807:
            return f'numeric (infinity - {self.max})'


def _is_date(value: Any) -> bool:
    try:
        if type(value).__name__ in ["datetime", "date", "time"]:
            return True
        datetime.datetime.fromisoformat(value)
        return True
    except (ValueError, TypeError):
        return False


def _is_null(value: Any) -> bool:
    return not value


def _other_validator(value: Any) -> bool:
    return True


def _not_valid(value: Any) -> bool:
    return False


def _is_list(value: Any) -> bool:
    return isinstance(value, list)


class _is_valid_enum():
    __slots__ = ['symbols']
    def __init__(self, symbols: List[str]):
        self.symbols = symbols
    def __call__(self, value: Any) -> bool:
        return value in self.symbols
    def __str__(self):
        return f'enum {self.symbols}'

"""
Create a dictionary of the validator functions
"""
SIMPLE_VALIDATORS = {
    "string": _is_string,
    "date": _is_date,
    "boolean": _is_boolean,
    "null": _is_null,
    "nullable": _is_null,   # alias
    "not_specified": _not_valid,
    "other": _other_validator,
    "list": _is_list,
    "array": _is_list,
}


def get_validators(
        type_descriptor: Union[List[str], str],
        **kwargs):
    """
    For a given type definition (the ["string", "nullable"] bit), return
    the matching validator functions (the _is_x ones) as a list.
    """
    if not type(type_descriptor).__name__ == 'list':
        type_descriptor = [type_descriptor]  # type:ignore
    validators: List[Callable] = []
    for descriptor in type_descriptor:
        if descriptor == 'enum':
            symbols = kwargs.get('symbols', [])
            validators.append(_is_valid_enum(symbols)) # type:ignore
        elif descriptor in ['int', 'numeric']:
            min_ = kwargs.get('min',  -9223372036854775808)
            max_ = kwargs.get('max', 9223372036854775807)
            validators.append(_is_numeric(min=min_, max=max_))
        else:
            validators.append(SIMPLE_VALIDATORS[descriptor]) # type:ignore
    return validators


def field_validator(value, validators: list) -> bool:
    """
    Execute a set of validator functions (the _is_x) against a value.
    Return True if any of the validators are True.
    """
    return any([validator(value) for validator in validators])


class Schema():

    def __init__(self, definition: Union[dict, str]):
        """
        Compile a validator for a given schema.

        paramaters:
        - definition: a dictionary, text representation of a dictionary (JSON)
          or a JSON file containing a schema definition
        """
        # if we have a schema as a string, load it into a dictionary
        if type(definition).__name__ == 'str':
            if os.path.exists(definition):  # type:ignore
                definition = json.loads(open(definition, mode='r').read())  # type:ignore
            else:
                definition = json.loads(definition)  # type:ignore

        try:
            # read the schema and look up the validators
            self._validators = {
                item.get('name'): get_validators(
                        item['type'], 
                        symbols=item.get('symbols'), 
                        min=item.get('min', -9223372036854775808), # 64bit signed (not a limit, just a default)
                        max=item.get('max', 9223372036854775807))  # 64bit signed (not a limit, just a default)
                for item in definition.get('fields', [])  #type:ignore
            }
        except KeyError:
            raise ValueError("Invalid type specified in schema - valid types are: string, numeric, date, boolean, null, list, enum")
        if len(self._validators) == 0:
            raise ValueError("Invalid schema specification")

    def validate(self, subject: dict = {}, raise_exception=False) -> bool:

        result = True
        self.last_error = ''
 
        for key, value in self._validators.items():
            if not field_validator(subject.get(key), self._validators.get(key, [_other_validator])):
                result = False
                for v in value:
                    if not v(value):
                        self.last_error += f"'{key}' ({subject.get(key)}) did not pass validator {str(v)}.\n"
                        #print(f"'{key}' ({subject.get(key)}) did not pass validator {str(v)}.\n")
        if raise_exception and not result:
            raise ValueError(F"Record does not conform to schema - {self.last_error}. ")
        return result

    def __call__(self, subject: dict = {}, raise_exception=False) -> bool:
        # wrap the validate function
        return self.validate(subject=subject, raise_exception=raise_exception)