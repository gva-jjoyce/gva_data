# gva.data.formats.dictset

## What Is It?

**DICT** ionary  data **SET**

A set of methods to help process lists and generators of dictionaries.

These generally create and consume generators to be memory efficient.

Handling generators also allows dictset to work with unbounded data (streaming or
infinite datasets); methods like _distinct_ may not operate exactly as expected over huge datasets.

## What It Is Not

This is not a replacement for a tool like Pandas, especially if aggregations or other functions across the entire dataset.

Pandas is somewhat memory intensive, if data is in a dictset compatible format dictset can filter and select data before loading into a Pandas dataframe.

## Terminology

**dictset** - an iterable of dictionaries   
**record** - a single dictionary of values (a row)  
**field** - a single data item in a record (a cell)  
**column** - a field across multiple records   

## What Is In It?

`select_record_fields(record, fields)` - Selects a subset of fields from a record  
`order(record)` - Sort a record by it's keys    
`join(left, right, column, join_type)` - Merge two dictsets on a key  
`union(*args)` - Append dictsets together   
`create_index(dictset, index_column)` - Create a dictionary where the key is the value in a column from a dictset     
`select_from(dictset, columns, condition)` - Select records and columns from a dictset    
`set_column(dictset, column_name, setter)` - Update all the fields of a column    
`set_value(record, column_name, setter)` - Update the value of a field in a record   
`distinct(dictset, cache)` - Deduplicates a dictset **CORRECTNESS IS NOT GUARANTEED**   
`limit(dictset, limit)` - Returns upto a maximum of _limit_ records from a dictset   
`dictsets_match(dictset_1, dictset_2)` - Compare to bounded dictsets for equivilence  
`page_dictset(dictset, page_size)` - Split a dictset into pages of _page_size_  
`sort_dictset(dictset, column, cache_size)` - Order a dictset **CORRECTNESS IS NOT GUARANTEED** 

distinct and sort_dictset work on unbounded datasets so cannot ensure the correctness of their functionality.

## How Do I Use It?

~~~python
from gva.data.formats import dictset

ds = something_which_returns_a_dictset()
ds = dictset.distinct(ds)
ds = dictset.select_from(ds, columns['name', 'rank'])
dataframe = pandas.DataFrame(ds)
~~~