# gva.data.formats.dictset

## What Is It?

**DICT** ionary  data **SET**

A set of methods to help process lists and generators of dictionaries.

These generally create generators to be memory efficient.

## What It Is Not

This is not a replacement for a tool like Pandas, especially if aggregations or other functions across the entire dataset.

Pandas is somewhat memory intensive, if data is in a dictset compatible format dictset can filter and select data before loading into a Pandas dataframe.

## Terminology

**dictset** - an iterable of dictionaries   
**record** - a single dictionary of values (a row)  
**field** - a single data item in a record (a cell)  
**column** - a field across multiple records   

## What Is In It?

`select_record_fields(record, fields)` - Selects a subset of fields from a dictionary  
`order(record)`    
`join(left, right, column, join_type)`  
`union(*args)`  
`create_index(dictset, index_column)`   
`select_from(dictset, columns, condition)`  
`set_column(dictset, column_name, setter)`  
`set_value(record, column_name, setter)`  
`distinct(dictset)`  
`limit(dictset, limit)`  
`dictsets_match(dictset_1, dictset_2)`  
`page_dictset(dictset, page_size)`

## How Do I Use It?

~~~python
from gva.data.formats.dictset import *

dictset = something_which_returns_a_dictset()
dictset = distinct(dictset)
dictset = select_from(dictset, columns['name', 'rank'])
dataframe = pandas.DataFrame(dictset)
~~~