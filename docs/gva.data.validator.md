# gva.data.validator

## What Is It?

Tests a dictionary for conformity against a schema.

The schema format is based on the schema used by Avro.

## How Do I Use It?

A schema is loaded into a _Schema_ object, data objects are then passed to the 'validate' method. 'validate' will return 'true' if the data conforms, non-conformity returns 'false', or can be set to raise an exception. On failure, the 'last_error' attribute is set to provide more information about the failure.

### Supported Types  

- **string** - a character sequence  
- **numeric** - a number, can optionally define _min_ and _max_ values  
- **int** - alias for _numeric_  
- **date** - an iso format date or time  
- **boolean** - binary value (true/false, on/off, yes/no)  
- **null** - None values allowed  
- **nullable** - alias for _null_  
- **list** - list of values  
- **array** - alias for _list_  
- **enum** - one of a list of values !! enums require the set of valid values to be set in a _symbols_ list.  
- **other** - not one of the above, but a required field  

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

The _null_ checker is valid if the field is _None_ or is a [Python False](https://docs.python.org/2.4/lib/truth.html)


### Example Code
~~~python
schema = Schema({"fields": [{"name": "string_field", "type": "string"}]})
data = {"name":"validator"}

is_valid = schema.validate(data)

if not is_valid:
    print(schema.last_error)
~~~

![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/typhaon](https://github.com/joocer/typhaon) 
