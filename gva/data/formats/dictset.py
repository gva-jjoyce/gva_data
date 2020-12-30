"""
DICT(IONARY) (DATA)SET

A group of functions to assist with handling lists of dictionaries, which is
how JSONL files will tend to be handled.

You would use these methods instead of something like Pandas if you are dealing
with potentially huge datasets and only require to be able to do simple
processes on the dataset, row at a time.

Methods provide a limited approximation of SQL functionality:

SELECT   - select_from
UNION    - union
WHERE    - select_from
JOIN     - join
DISTINCT - disctinct
LIMIT    - limit

Most methods return generators, which mean they can only be iterated once, to
iterate again they need to be regenerated. If you know you're going
to need to iterate more than once, you can use list() or similar to cache the
values, however this may cause problems if the list is large.


Example usage:

# filter the two lists
filtered_list1 = select_from_dictset(list1, ['id', 'name'], lambda x: x['height'] > 2)
filtered_list2 = select_from_dictset(list2, ['id', 'last_seen'])

# join the two lists on id
for record in join_dictsets(filtered_list1, filtered_list2, 'id'):
    print(record)

"""
from typing import Iterator, Any, List, Union, Callable
import orjson as json


class JOINS(object):
    INNER_JOIN = 'INNER'
    LEFT_JOIN = 'LEFT'


def select_all(dummy: Any) -> bool:
    """
    Returns True.
    - dummy: anything, it's ignored
    """
    return True


def select_record_fields(
        record: dict,
        fields: List[str]) -> dict:
    """
    Selects a subset of fields from a dictionary. If the field is not present
    in the dictionary it defaults to None.

    Parameters:
    - dictset: an iterable of dictionaries
    - fields: a list of the fields to select
    """
    return {k: record.get(k, None) for k in fields}


def order(record: dict) -> dict:
    """
    Sort a dictionary by its keys.

    Parameters:
    - record: a record 
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

    NOTES:
    - where columns are in both tables - I don't know what happens.
    - resultant records may have inconsistent columns (same as 
      source lists)

    Parameters:
    - left: the 'left' dictset
    - right: the 'right' dictset
    - column: the column shared by both dictsets to join on
    - join_type: the type of join, INNER or LEFT

    Approximate SQL:
    SELECT * FROM left JOIN right ON left.column = right.column
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
    - Any number of dictsets

    Approximate SQL:

    SELECT * FROM dictset_1
    UNION
    SELECT * FROM dictset_2
    """
    for dictset in args:
        for record in dictset:
            yield record


def create_index(
        dictset: Iterator[dict],
        index_column: str) -> dict:
    """
    Create an index of a file to speed up look-ups.

    Parameters:
    - dictset: an iterable of dictionaries
    - index_column: the column in the dictset to index on, it is expected to be
      unique but this is not enforced.
    """
    index = {}
    for record in dictset:
        index_value = record[index_column]
        index[index_value] = record
    return index


def select_from(
        dictset: Iterator[dict],
        columns: List[str] = ['*'],
        where: Callable = select_all) -> Iterator[dict]:
    """
    Scan a dictset, filtering rows and selecting columns.

    Parameters:
    - dictset: an iterable of dictionaries
    - columns: a list of column names to return
    - where: a function to apply to filter records (keep rows that evaluate to True)

    Approximate SQL:
    SELECT columns FROM dictset WHERE where
    """
    for record in dictset:
        if where(record):
            if columns != ['*']:
                record = select_record_fields(record, columns)
            yield record


def set_column(
        dictset: Iterator[dict],
        column_name: str,
        setter: Callable) -> Iterator[dict]:
    """
    Performs set_value on each row in a set.

    Parameters:
    - dictset: an iterable of dictionaries
    - column_name: the column to create or update
    - setter: a method or constant to update the column
    """
    for record in dictset:
        yield set_value(record, column_name, setter)


def set_value(
        record: dict,
        column_name: str,
        setter: Callable) -> dict:
    """
    Sets the value of a column to either a fixed value or as the result of a
    function which recieves the row as a parameter.

    This method is of limited use by itself, it is used internally by
    `set_column`.

    Paramters:
    - record: a record
    - column_name: the name of the field
    - setter: a method or constant to update the field
    """
    if callable(setter):
        record[column_name] = setter(record)
    else:
        record[column_name] = setter
    return record


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
    - dictset: an iterable of dictionaries
    - cache_size: the number of records to remember (optional, default 10,000)
    """
    from ...utils import LRU_Index
    lru = LRU_Index(size=cache_size)

    for record in dictset:
        entry = json.dumps(record)
        if lru(entry):
            continue
        yield record


def limit(
        dictset: Iterator[dict],
        limit: int):
    """
    Returns up to 'limit' number of records.

    Paramters:
    - dictset: an iterable of dictionaries
    - limit: the maximum number of records to return
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
    Tests if two dictsets match.
    
    Note that this will exhaust a generator.

    Parameter:
    - dictset_1: the first dictset
    - dictset_2: the second dictset
    """
    def _hash_set(dictset: Iterator[dict]):
        xor = 0
        for record in dictset:
            entry = order(record)
            entry = json.dumps(entry)  # type:ignore
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
    - dictset: an iterable of dictionaries
    - page_size: the number of records per page
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

    Parameters:
    - dictset: an iterable of dictionaries
    - column: the field to order by
    - cache_size: the number of records to cache to order

    This method works best with partially sorted data, for randomized data
    and a small cache, the effect of sorting is poor; for partially sorted
    data, and/or a large cache, the performance is better.

    Note that running this multiple times is not memory efficient as each
    run is concurrent. You are better off doubling the cache size rather 
    than running this function twice.

    Also note that if this method is placed in a pipeline, it will need
    to collect cache_size number of records before it will emit any records.
    """
    # cache_size is the high water mark, 3/4 is the low water mark. 
    # We fill the cache to the high water mark, sort it and yield 
    # the top 1/4 before filling again.
    # This reduces the number of times we execute the sorted function
    # which is the slowest part of this method.
    # A cache_size of 1000 has a neglible impact on performance, a 
    # cache_size of 50000 introduces a performance hit of about 15%.
    quarter_cache = cache_size // 4
    cache = []
    for item in dictset:
        cache.append(item)
        if len(cache) > cache_size:
            cache = sorted(cache, key=lambda x: x[column], reverse=descending) 
            if descending:
                yield from reversed(cache[:quarter_cache])
            else:
                yield from cache[:quarter_cache]
            del cache[:quarter_cache]
    cache = sorted(cache, key=lambda x: x[column], reverse=descending) 
    yield from cache


def to_pandas(
        dictset: Iterator[dict]):
    import pandas  # type:ignore
    return pandas.DataFrame(dictset)
