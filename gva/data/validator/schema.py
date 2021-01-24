"""
Schema Validation

Tests a dictionary against a schema to test for conformity.
Schema definition is similar to - but not the same as - avro schemas

Example Schema:
{
 "name": "Table Name",
 "fields": [
     {"name": "id", "type": "string"},
     {"name": "country",  "type": ["string", "nullable"]},
     {"name": "followers", "type": ["string", "nullable"]}
 ]
}
"""

from .is_boolean import is_boolean
from .is_cve import is_cve
from .is_date import is_date
from .is_list import is_list
from .is_null import is_null
from .is_numeric import is_numeric
from .is_string import is_string
from .is_valid_enum import is_valid_enum
from .other_validator import other_validator

from typing import Any, Union, List
from ...utils.json import parse
from ...errors import ValidationError
import os

"""
Create dictionaries to look up the type validators
"""
SIMPLE_VALIDATORS = {
    "date": is_date,
    "nullable": is_null,
    "other": other_validator,
    "list": is_list,
    "array": is_list,
}

COMPLEX_VALIDATORS = {
    "enum": is_valid_enum,
    "numeric": is_numeric,
    "string": is_string,
    "boolean": is_boolean,
    "cve": is_cve
}


class Schema():

    def __init__(self, definition: Union[dict, str]):
        """
        Create and compile a validator for a given schema.

        Paramaters:
            definition: dictionary or string
                A dictionary, a JSON string of a dictionary or the name of a 
                JSON file containing a schema definition
        """
        # if we have a schema as a string, load it into a dictionary
        if type(definition).__name__ == 'str':
            if os.path.exists(definition):  # type:ignore
                definition = parse(open(definition, mode='r').read())  # type:ignore
            else:
                definition = parse(definition)  # type:ignore

        try:
            # read the schema and look up the validators
            self._validators = {
                item.get('name'): self._get_validators(
                        item['type'], 
                        symbols=item.get('symbols'), 
                        min=item.get('min'),
                        max=item.get('max'),
                        format=item.get('format'))
                for item in definition.get('fields', [])  #type:ignore
            }
        except KeyError:
            raise ValueError("Invalid type specified in schema - valid types are: string, numeric, date, boolean, nullable, list, enum")
        if len(self._validators) == 0:
            raise ValueError("Invalid schema specification")


    def _get_validators(
            self,
            type_descriptor: Union[List[str], str],
            **kwargs):
        """
        For a given type definition (the ["string", "nullable"] bit), return
        the matching validator functions (the _is_x ones) as a list.
        """
        if not type(type_descriptor).__name__ == 'list':
            type_descriptor = [type_descriptor]  # type:ignore
        validators: List[Any] = []
        for descriptor in type_descriptor:
            if descriptor in COMPLEX_VALIDATORS:
                validators.append(COMPLEX_VALIDATORS[descriptor](**kwargs))
            else:
                validators.append(SIMPLE_VALIDATORS[descriptor])
        return validators


    def _field_validator(
            self,
            value,
            validators: set) -> bool:
        """
        Execute a set of validator functions (the _is_x) against a value.
        Return True if any of the validators are True.
        """
        return any([True for validator in validators if validator(value)])


    def validate(self, subject: dict = {}, raise_exception=False) -> bool:
        """
        Test a dictionary against the Schema

        Parameters:
            subject: dictionary
                The dictionary to test for conformity
            raise_exception: boolean (optional, default False)
                If True, when the subject doesn't conform to the schema a
                ValidationError is raised

        Returns:
            boolean

        Raises:
            ValidationError
        """
        result = True
        self.last_error = ''
 
        for key, value in self._validators.items():
            if not self._field_validator(subject.get(key), self._validators.get(key, [other_validator])):
                result = False
                for v in value:
                    self.last_error += f"'{key}' ({subject.get(key)}) did not pass validator {str(v)}.\n"
        if raise_exception and not result:
            raise ValidationError(F"Record does not conform to schema - {self.last_error}. ")
        return result


    def __call__(self, subject: dict = {}, raise_exception=False) -> bool:
        """
        Alias for validate
        """
        return self.validate(subject=subject, raise_exception=raise_exception)