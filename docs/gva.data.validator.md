# gva.data.validator

### What Is It?

Tests a dictionary for conformity against a schema.

The schema format is based on the schema used by Avro.

## How Do I Use It?

A schema is loaded into a _Schema_ object, data objects are then passed to the 'validate' method. 'validate' will return 'true' if the data conforms, non-conformity returns 'false', or can be set to raise an exception. On failure, the 'last_error' attribute is set to provide more information about the failure.

## Supported Types  

**string** - a character sequence  
Parameters
  - **format** - (optional) A Regular Expression to Match  

**numeric** - a number  
Paramters 
  - **min** - (optional) minimum valid value - if no value is provided an arbitrary very low number is used
  - **max** - (optional) maximum valid value - if no value is provided an arbitrary very high number is used

**date** - a datetime object or a string in iso format    
No Parameters  

**boolean** - boolean value  
Parameters
  - **symbols** - (optional) list of valid values - if no value is provided a default set is used  

**nullable** - an empty (None) or missing value allowed  
No Parameters

**list** - list of values  
No Parameters

**enum** - one of a set of values
Parameters
  -  **symbols** - (optional) list of valid values

**other** - not one of the above, but a required field  
No Parameters

### Example Schema
~~~json
{
 "fields": [
     {"name": "id",    "type": "string"},
     {"name": "name",  "type": "string"},
     {"name": "age",   "type": ["numeric", "null"], "min": 0},
     {"name": "color", "type": "enum", "symbols": ['RED', 'GREEN', 'BLUE']}
 ]
}
~~~

A field can be tested against multiple Types by putting the Types in a list (see _age_ in the Example Schema above). In this case the field is valid if any of the types match; this is most useful for values which are _null_ or a value.

The _null_ checker is valid if `is None` evaluates to `True` for the the value.

### Example Code
~~~python
from gva.data.validator import Schema

schema = Schema({"fields": [{"name": "string_field", "type": "string"}]})
data = {"name":"validator"}

is_valid = schema.validate(data)

if not is_valid:
    print(schema.last_error)
~~~

---
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/typhaon](https://github.com/joocer/typhaon) 
