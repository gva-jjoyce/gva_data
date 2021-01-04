# gva.data.readers

## What Is It?

A data reader library.

The reader can read across multiple blobs in a GCS bucket with filter, select and and name pattern matching functionality.

## How Do I Use It?

Instantiate a _Reader_, the default functionality is to read Google Cloud Storage buckets, reading the local filesystem
requires the _reader_ parameter to be set to _file_reader_. The default behaviour is for the reader to find all of the 
files/blobs that start with the _from_path_ and have the extention provided (default _.jsonl_), to read each file
line-by-line, convert the line to a python dict by parsing as a JSON string and yielding the result.

The _Reader_ can be configured to just return the text and to look for a different file extention and to limit searches
based on a date range (requires folders to have date parts in their names). The default will only look for today.

The _Reader_ can filter records and limit the columns returned - the storage format is unindexed and row-based so this
does not affect read performance - the dataset is scanned to find matching records. However, the reader is lazy and only
scans until it finds a matching record to return, it will repeat this until either it has exhausted the records to look 
for or until records are no longer asked for.

The parameter names on the _Reader_ have been chosen to be similar to a SQL select statement.

~~~python
from gva.data import Reader

critical_errors = Reader(
        select=['server', 'error_level'],
        from_path="error_logs/year_%Y/month_%m/day_%d/",
        where=lambda r: r['error_level'] == 'critical',
        date_range=(datetime.date(2020, 1, 1), None)
    )
critical_errors.to_pandas()
~~~

## Available Readers

**blob_reader** - the default reader  
**file_reader** - read from files and folders on the filesystem  
**mongodb_reader** - read from a MongoDB    
**minio_reader** - read from MinIo (may work with S3)  

## Usage Recommendations

**Date Filtering**  
There are two ways to read from files from particular dates - filter at a folder level and use a _where_ clause. It is 
recommended that folder-level filtering is used as a gross filter and the _where_ as a fine filter. Folder filtering 
will reduce the number of files that need to be read, improving performance.

---
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/orwell](https://github.com/joocer/orwell) 
