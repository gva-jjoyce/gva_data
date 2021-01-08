# gva.data.readers

## What Is It?

A data reader library.

The reader can read across multiple files in a GCS bucket with filter, select and and name pattern matching functionality.

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

**GoogleCloudStorageReader** - (default) GCP Cloud Storage reader  
**FileReader** - read from files and folders on the local filesystem  
**MongoDbReader** - read from a MongoDB    
**MinioReader** - read from MinIO (may work with S3)  

## Parameters

**from_path**: str  
>The path to the data, see below for more detailed notes (required)   

**project**: str, required for `GoogleCloudStorageReader`
>The name of the GCP project where the data is stored - only used by the `GoogleCloudStorageReader`

**select**: list of field names, optional  
> A list of the nanes of the columns to return from the dataset (default is all columns)  

**where**: Callable, optional  
>A method (a function or a lambda expression) to filter the returned records, where the function returns `True` the record is returned, `False` the record is skipped (default is all records)

**reader**: BaseReader class name, optional
> The reader class to perform the data access tasks (default `GoogleCloudStorageReader`)  

**data_format**: str, either '_json_' or '_text_', optional
>Controls how the data is interpretted. '_json_' will parse to a `dict` before 'select' or 'where', '_text_' will just return the line that has been read (default is 'json') 

**date_range**: A tuple of dates, optional
>The dates to search for data between, the first value is the start date, the second is the end date (default is today)

**start_date**: date, optional
>The starting date of the range to read over - if used with 'date_range', this value will be preferred (default is today)

**end_date**: date, optional
>The end date of the range to read over - if used with 'date_range', this value will be preferred (default is today)

**project**: str, required for `GoogleCloudStorageReader`
>The name of the GCP project where the data is stored - only used by the `GoogleCloudStorageReader`

**extention**: str, optional
>The file extention to filter files by - only used by the `FileReader` (default is '.jsonl')

**chunk_size**: int, optional
>Limit the number of bytes read from a file at at time - only used by the `FileReader` (default is 16Mb, the default partition size) 

**delimiter**: str, optional
>The character(s) used to split between records - only used by the `FileReader` (default is '\n')

**encoding**: str, optional
>The encoding to apply to the file as it is read - only used by the `FileReader` (default is 'utf8')

**step_back_days**: int, optional
>The number of days to step back if there is no data available for day (default is 0) - _**in development**_

**thread_count**: int, optional
>The number of threads to use to read data, provides performance improvement at the cost of record ordering (default is not use any threading) 

**fork_processes**: bool, optional
>Fork the read over multiple processes to speed up reading _**in development**_

**NOTE** The `MongoDbReader` and `MinioReader` have additional parameters not listed above.

## From Path

The 'from_path' parameter is a string which describes where the data should be read from, the path is similar to file and directory paths, with some additional notes:

It's recommended that the 'from_path' just specify the folder/directory to read from and not the filenames. When the path is a folder, it should end with a '/'. This removes the need to know the file names before reading the data. 

For the `GoogleCloudStorageReader`, the bucket name is at the start of the 'from_path' - this aligns to how the paths are shown in the UI.

The 'from_path' can contain date formatting placeholders and for each date between the 'start_date' and the 'end_date' the placeholders are replaced with the appropriate strings.

There are two additional date formatting placeholders, representing common exhanges:
- %date = %Y_%m_%d
- %datefolders = year_%Y/month_%m/day_%d

## Recommendations

**Date Filtering**  
There are two ways to read from files from particular dates - filter at a folder level and use a _where_ clause. It is 
recommended that folder-level filtering is used as a gross filter and the _where_ as a fine filter. Folder filtering 
will reduce the number of files that need to be read, improving performance.

**Data Filtering**   
Converting to json takes time, if the 'where' clause is essentially a text search consider using 'text' as the 'data_format', doing the text search in the 'where' and converting the filtered records to `dict`s.

---
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/orwell](https://github.com/joocer/orwell) 
