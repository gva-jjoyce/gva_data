# gva.data.writer

## What Is It?

A data writer helper library.

The writer writes individual records to files and buckets, with caching, partitioning and data validation.

The writer has features for stream processing. By default the _Writer_ will write to GCS storage buckets,
this can be changed by updating the _writer_ parameter to the _file_writer_ function.

## How Do I Use It?

1) Instantiate the Writer
2) Append records to the Writer

~~~python
from gva.data import Writer

writer = Writer(
        to_path="error_logs/%date/errors.jsonl"
)
writer.append({"server": "files", "error_level": "debug", "message", "power on"})
~~~

## Partitions

The default behaviour of the writer is to create partitions. Partitions are chunks of data upto 16Mb in size, 
partitions are also closed no new records have been appended to the writer for 30 seconds. 

Partitions have four digit suffixes added to filenames. Compressed files have _.lzma_ added as an extention.
Writer will replace datetime placeholders, including macros to covert `%date` to `%Y-%m-%d` and `%datefolders` 
to `year_%Y/month_%m/day_%d`.

For example:

`error_logs/%Y/%m/%d/errors_%date.jsonl` => `error_logs/2020/12/25/errors_2020-12-25-0000.jsonl.lzma`

Partitions help with stream processing by:
- Partitioning gives a finite bound to writing and saving data

Partitions help with reading data by:
- Partitions can be loaded into memory one at a time, enabling reading of huge data sets without huge memory
- Partitions can be read in parallel, speeding up scanning and searching of data

The smaller files which are created by partitions also help activities like out-of-band compression, which
can be used to reduce storage costs.

## Usage Recommendations

**to_path**  
The _to_path_ is recommended to be in this form:

source/year_%Y/month_%m/day_%d/source_%date.jsonl

General guidance is that the number of items in each folder should be minimized, this generally looks like folders
creating folders for each unit of time and writing files to those folders. This can be monthly, weekly, daily, hourly
etc, depending on the frequency and volume of the data. The Writer can create up to 9999 partitions before the partition
numbers start to collide - this is:

- over a rate of one parition every 10 seconds
- over 156Gb of data

If these limits are being approached, the partition numbering logic should be rewritten or the resolution of the data
changed.

**Compression**  
Compression reduces filesizes to approximately 25% of their original size, but is very expensive (between 
4 and 20 times longer to write). On smaller data sets this is unlikely to be a problem but can increase a
job which tool a few minutes to over an hour for larger datasets. It's recommended in this scenario that
compression be deferred. A _tool_ to support this is planned.


---
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/orwell](https://github.com/joocer/orwell)  
