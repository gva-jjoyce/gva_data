"""
DICT(IONARY) (DATA)SET

A group of functions to assist with handling lists of dictionaries, which is
how JSONL files will tend to be handled.

You would use these methods instead of something like Pandas if you are dealing
with potentially huge datasets and only require to be able to do simple
processes on the dataset, row at a time.

Most methods return generators, which mean they can only be iterated once, to
iterate again they need to be regenerated. If you know you're going
to need to iterate more than once, you can use list() or similar to cache the
values, however this may cause problems if the list is large.
"""
from typing import Iterator, Any, List, Callable
from ...utils.json import serialize, parse
from .group_by import Groups


class JOINS(object):
    INNER_JOIN = 'INNER'
    LEFT_JOIN = 'LEFT'


def select_record_fields(
        record: dict,
        fields: List[str]) -> dict:
    """
    Selects a subset of fields from a dictionary. If the field is not present
    in the dictionary it defaults to None.

    Parameters:
        record: dictionary
            The dictionary to select from
        fields: list of strings
            The list of the field names to select

    Returns:
        dictionary
    """
    return {k: record.get(k, None) for k in fields}


def order(
        record: dict) -> dict:
    """
    Sort a dictionary by its keys.

    Parameters:
        record: dictionary
            The dictionary to sort

    Returns:
        dictionary
    """
    return dict(sorted(record.items()))


def join(
        left: Iterator[dict],
        right: Iterator[dict],
        column: str,
        join_type=JOINS.INNER_JOIN) -> Iterator[dict]:
    """
    Iterates over the left dictset, matching records fron the right dictset. 
    Dictsets provided to this method are expected to be bounded.

    INNER_JOIN, the default, will discard records unless they appear in both
    tables, LEFT_JOIN will keep all the records fron the left table and add
    records for the right table if a match is found.

    It is recommended that the left table be the larger of the two tables as
    the right table is loaded into memory to perform the matching and look ups.

    Parameters:
        left: iterable of dictionaries 
            The 'left' dictset
        right: iterable of dictionaries
            The 'right' dictset
        column: string
            The name of column shared by both dictsets to join on
        join_type: dictset.JOINS (optional, default INNER_JOIN)
            The type of join, INNER or LEFT

    Yields:
        dictionary
    """
    index = create_index(right, column)
    for record in left:
        value = record.get(column)
        if index.get(value):
            yield {**record, **index[value]}
        elif join_type == JOINS.LEFT_JOIN:
            yield record


def union(*args) -> Iterator[dict]:
    """
    Append the records from a set of lists together, doesn't ensure columns
    align.

    Parameters:
        args: list of iterables of dictionaries
            The lists of dictionaries to concatenate

    Yields:
        dictionary
    """
    for dictset in args:
        yield from dictset


def create_index(
        dictset: Iterator[dict],
        index_column: str) -> dict:
    """
    Create an index of a file to speed up look-ups, it is expected that the
    value in the index_column is unique but this is not enforced.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        index_column: string
            the column in the dictset to index on

    Returns:
        dictionary
    """
    index = {}
    for record in dictset:
        index_value = record[index_column]
        index[index_value] = record
    return index


def select_from(
        dictset: Iterator[dict],
        columns: List[str] = ['*'],
        where: Callable = None) -> Iterator[dict]:
    """
    Scan a dictset, filtering rows and selecting columns.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        columns: list of strings 
            The list of column names to return
        where: callable (optional) 
            The function to apply to filter records, we return the rows that 
            evaluate to True, default returns all records

    Yields:
        dictionary
    """
    def _select_columns(dictset, columns):
        for record in dictset:
            record = select_record_fields(record, columns)
            yield record

    if where is not None:
        dictset = filter(where, dictset)
    if columns != ['*']:
        dictset = _select_columns(dictset, columns)
    yield from dictset


def set_column(
        dictset: Iterator[dict],
        column_name: str,
        setter: Callable) -> Iterator[dict]:
    """
    Performs set_value on each row in a set.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        column_name: string 
            The column to create or update
        setter: callable or any
            A function or constant to update the column with

    Yields:
        dictionary
    """
    for record in dictset:
        yield set_value(record, column_name, setter)


def set_value(
        record: dict,
        field_name: str,
        setter: Callable) -> dict:
    """
    Sets the value of a column to either a fixed value or as the result of a
    function which recieves the row as a parameter.

    Paramters:
        record: dictionary
            The dictionary to update
        field_name: string 
            The field to create or update
        setter: callable or any
            A function or constant to update the field with

    Returns:
        dictionary
    """
    copy = record.copy()
    if callable(setter):
        copy[field_name] = setter(copy)
    else:
        copy[field_name] = setter
    return copy


def distinct(
        dictset: Iterator[dict],
        cache_size: int = 10000):
    """
    THIS MAY NOT DO WHAT YOU EXPECT IT TO.

    Removes duplicate records from a dictset, as it able to run against
    an unbounded (infinite) set, it may not fully deduplicate a set. The
    larger the cache_size the more likely the duplication will be correct
    however, this is at the expense of memory.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        cache_size: integer (optional, default 10,000)
            the number of records to cache

    Yields:
        dictionary
    """
    from ...utils import LRU_Index
    lru = LRU_Index(size=cache_size)

    for record in dictset:
        entry = serialize(record)
        if lru(entry):
            continue
        yield record


def limit(
        dictset: Iterator[dict],
        limit: int):
    """
    Returns up to 'limit' number of records.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        limit: integer 
            the maximum number of records to return

    Yields:
        dictionary
    """
    counter = limit
    for record in dictset:
        if counter == 0:
            return None
        counter -= 1
        yield record


def dictsets_match(
        dictset_1: Iterator[dict],
        dictset_2: Iterator[dict]):
    """
    Tests if two dictsets match regardless of the order of the order of the
    records in the dictset. Return is True if the sets match.
    
    Note that this will exhaust a generator.

    Parameter:
        dictset_1: iterable of dictionaries
            The first dictset to match
        dictset_2: iterable of dictionaries
            The second dictset to match

    Returns:
        boolean
    """
    def _hash_set(dictset: Iterator[dict]):
        xor = 0
        for record in dictset:
            entry = order(record)
            entry = serialize(entry)  # type:ignore
            _hash = hash(entry)
            xor = xor ^ _hash
        return xor

    return _hash_set(dictset_1) == _hash_set(dictset_2)


def page_dictset(
        dictset: Iterator[dict],
        page_size: int) -> Iterator:
    """
    Enables paging through a dictset by returning a page of records at a time.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        page_size: integer
            The number of records per page

    Yields:
        dictionary
    """
    chunk: list = []
    for item in dictset:
        if len(chunk) >= page_size:
            yield chunk
            chunk = [item]
        else:
            chunk.append(item)
    if chunk:
        yield chunk


def sort(
        dictset: Iterator[dict],
        column: str,
        cache_size: int,
        descending: bool = True):
    """
    THIS MAY NOT DO WHAT YOU EXPECT IT TO.

    Sorts a dictset by a column. As it able to run an unbounded dataset it
    may not correctly order a set completely. The larger the cache_size the
    more likely the set will be ordered correctly, at the cost of memory.

    This method works best with partially sorted data, for randomized data
    and a small cache, the effect of sorting is poor; for partially sorted
    data, and/or a large cache, the performance is better.

    Note that if this method is placed in a pipeline, it will need to process
    cache_size number of records before it will emit any records.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to process
        column: string
            The field to order by
        cache_size: integer
            The number of records to cache
        descending: boolean (optional, True)
            Reverse the order of the dictset

    Yields:
        dictionary
    """

    def _sort_key(key):
        """
        called like this: _sort_key(key)(row)
        """
        k = key

        def _inner_sort_key(row):
            return row.get(k)

        return _inner_sort_key

    # cache_size is the high water mark, 3/4 is the low water mark. 
    # We fill the cache to the high water mark, sort it and yield 
    # the top 1/4 before filling again.
    # This reduces the number of times we execute the sorted function
    # which is the slowest part of this method.
    # A cache_size of 1000 has a neglible impact on performance, a 
    # cache_size of 50000 introduces a performance hit of about 15%.
    quarter_cache = max(cache_size // 4, 1)
    cache = []
    for item in dictset:
        cache.append(item)
        if len(cache) > cache_size:
            cache = sorted(cache, key=_sort_key(column), reverse=descending) 
            if descending:
                yield from reversed(cache[:quarter_cache])
            else:
                yield from cache[:quarter_cache]
            del cache[:quarter_cache]
    cache = sorted(cache, key=_sort_key(column), reverse=descending) 
    yield from cache


def to_pandas(
        dictset: Iterator[dict]):
    """
    Load an iterable of dictionaries into a pandas dataframe.

    Parameters:
        dictset: iterable of dictionaries
            The dictset to load

    Returns:
        pandas dataframe
    """
    import pandas  # type:ignore
    return pandas.DataFrame(dictset)


def extract_column(
        dictset: Iterator[dict],
        column: str) -> list:
    """
    Extract the values from column into a list 

    Parameters:
        dictset: iterable of dictionaries
            The dictset to extract values from
        column: string
            The name of the column to extract the values of

    Returns:
        list
    """  
    return [record.get(column) for record in dictset]


def group_by(
        dictset: Iterator[dict],
        column: str) -> Groups:
    """
    Create a Groups object 

    Parameters:
        dictset: iterable of dictionaries
            The dictset to group
        column: string
            The name of the column to group by

    Returns:
        gva.formats.Groups
    """  
    return Groups(dictset, column)


def jsonify(
        list_of_json_strings: Iterator[dict]):
    """
    Convert a list of strings to a list of dictionaries

    Parameters:
        list_of_json_strings: iterable of strings
            The JSON formatted strings to parse to dictionaries

    Yields:
        dictionary
    """  
    for row in list_of_json_strings:
        yield parse(row)  # type:ignore
