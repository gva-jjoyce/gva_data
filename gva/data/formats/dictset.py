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
    Selects a subset of fields from a dictionary. If the field
    is not present in the dictionary it defaults to None.

    Parameters:
    - dictset: an iterable of dictionaries
    - fields: a list of the fields to select
    """
    return {k: record.get(k, None) for k in fields}


def order(record: dict) -> dict:
    """
    Sort a dictionary by it's keys.

    Parameters:
    - records: a 
    """
    return dict(sorted(record.items()))

def join(
        left: Iterator[dict],
        right: Iterator[dict],
        column: str,
        join_type=JOINS.INNER_JOIN) -> Iterator[dict]:
    """
    Iterates over the left table, matching records fron the right table.

    INNER_JOIN, the default, will discard records unless they appear in both
    tables, LEFT_JOIN will keep all the records fron the left table and add
    records for the right table if a match is found.

    It is recommended that the left table be the larger of the two tables as
    the right table is loaded into memory to perform the matching and look ups.

    NOTES:
    - where columns are in both tables - I don't know what happens.
    - resultant records may have inconsistent columns (same as 
      source lists)

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
    """
    index = {}
    for record in dictset:
        index_value = record[index_column]
        index[index_value] = record
    return index


def select_from(
        dictset: Iterator[dict],
        columns: List[str] = ['*'],
        condition: Callable = select_all) -> Iterator[dict]:
    """
    Scan a dictset, filtering rows and selecting columns.

    Basic implementation of SQL SELECT statement for a single table

    Approximate SQL:

    SELECT columns FROM dictset WHERE condition
    """
    for record in dictset:
        if condition(record):
            if columns != ['*']:
                record = select_record_fields(record, columns)
            yield record


def set_column(
        dictset: Iterator[dict],
        column_name: str,
        setter: Callable) -> Iterator[dict]:
    """
    Performs set_value on each row in a set
    """
    for record in dictset:
        yield set_value(record, column_name, setter)


def set_value(
        record: dict,
        column_name: str,
        setter: Callable) -> dict:
    """
    Sets the value of a column to either a fixed value or as the
    result of a function which recieves the row as a parameter
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
    Removes duplicate records from a dictset

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
    Tests if two sets match - this terminates generators
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
    Enables paging through a dictset by returning a page
    of records at a time.
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
