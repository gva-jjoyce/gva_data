# gva.data.formats.dictset

## What Is It?

**DICT** ionary  data **SET**

A set of methods to help process lists and generators of dictionaries.

These methods generally create and consume generators to be memory efficient.

Handling generators also allows dictset to work with unbounded data (streaming or
infinite datasets); methods like _distinct_ may not operate as expected over large
datasets.

## What It Is Not

This is not a replacement for a tool like `Pandas`, especially if aggregations or
other functions across the entire dataset.

Pandas is somewhat memory intensive, if data is in a dictset compatible format
dictset can filter and select data before loading into a Pandas dataframe.

## Terminology

**dictset** - an iterable of dictionaries - usually a list or generator    
**record** - a single dictionary of values (a row)  
**field** - a single data item in a record (a cell)  
**column** - a field across multiple records   

## What Is In It?

`select_from(dictset, columns, where)` - Select records and columns from a dictset    
`set_column(dictset, column_name, setter)` - Update all the fields of a column    
`distinct(dictset, cache_size)` - Deduplicates a dictset **CORRECTNESS IS NOT GUARANTEED**   
`limit(dictset, limit)` - Returns upto a maximum of _limit_ records from a dictset   
`page_dictset(dictset, page_size)` - Split a dictset into pages of _page_size_  
`sort(dictset, column, cache_size)` - Order a dictset **CORRECTNESS IS NOT GUARANTEED**   
`to_pandas(dictset)` - Load the dictset into a Pandas Dataframe  
`join(left, right, column, join_type)` - Merge two dictsets on a key  
`union(*args)` - Append dictsets together   
`dictsets_match(dictset_1, dictset_2)` - Compare two bounded dictsets for equivilence  
`create_index(dictset, index_column)` - Create a dictionary where the key is the value in a column from a dictset     
`order(record)` - Sort a record by it's keys    
`select_record_fields(record, fields)` - Selects a subset of fields from a record   
`set_value(record, column_name, setter)` - Update the value of a field in a record   
`to_html_table(dictset, limit)` - Create a HTML table of the first _limit_ rows  
`to_ascii_table(dictset, limit)` - Create a ASCII table of the first _limit_ rows  
`group_by(dictset, column)` - Create a group_by object, grouping records by the value in _column_

**NOTE** distinct and sort_dictset have been written to work on unbounded (streaming) datasets and 
work on blocks of records so cannot ensure the correctness of the results they create. If these functions
need to be correct, set the _cache_size_ to match the size of the dictset, or use another tool
such as _Pandas_.

## How Do I Use It?

~~~python
from gva.data.formats import dictset

# gva.data.reader return dictsets
ds = something_which_returns_a_dictset()
ds = dictset.distinct(ds, 1000)
ds = dictset.select_from(ds, columns['name', 'rank'])
dataframe = pandas.DataFrame(ds)
~~~

## Groups

**Groups** is in development and its functionality and interface is subject to change - use in systems is not recommended.

The `group_by` function creates a new object holding the grouped data that other operations
can be carried out on. 

~~~python
# create a group_by from a dataset called 'example_dictset'
# group on the value in the column 'country'
groups = dictset.group_by(example_dictset, column='country')

# len() will return the number of groups
print(f'There are {len(groups)} countries')

# aggregate() with apply a function to a column in each group
averages = groups.aggregate('population', sum)

# count() returns the number of items in a specific group
# if no group is provided, it returns an aggregation with len
states_in_oz = groups.count('Australia')
~~~



## A Note On Generators

Most methods in this library delay their execution, which means something like this code:

~~~python
start_time = time.time_ns()
ds = dictset.select_from(ds, columns['name', 'rank'])
print(time.time_ns() - start_time)
~~~

Will print an extremely short time (milliseconds), even for gigabyte sized datasets. This
is because the method returns a `generator` rather than the dataset. The code is executed
when the data is requested. This can be seen with code like this:

~~~python
def demonstration_generator(ds):
    for record in ds:
        print(record)
        # the yield keyword makes the method a generator
        yield record

ds = [{"one":1},{"two":2}]
new_ds = demonstration_generator(ds)
~~~

The above code will not print anything to the screen as the code in the for loop isn't executed.
The result of `type(new_ds).__name__` will show it is a generator rather than a `list`.

The code in the for loop is executed when the records from `new_ds` are stepped through like this:

~~~python
for record in new_ds:
    print(f"record: {record}")
~~~

This code will output:

~~~
{'one':1}
record: {'one':1}
{'two':2}
record: {'two':2}
~~~

The first instance of each row has been written by the `demonstration_generator` whilst the second
is from the for loop which is stepping through the generator.