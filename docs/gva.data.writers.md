# gva.data.writer

## What Is It?

A data writer helper library.

The writer writes to buckets, with tempfile caching, and data validation functionality.

## How Do I Use It?

~~~python
from gva.data import Writer

writer = Writer(
        to_path="error_logs/"
)
writer.append({"server": "files", "error_level": "debug", "message", "power on"})
~~~

## Usage Recommendations

**Compression**  
Compression reduces filesizes to approximately 25% of their original size, but is very expensive (between 
4 and 20 times longer to write). On smaller data sets this is unlikely to be a problem but can increase a
job which tool a few minutes to over an hour for larger datasets. It's recommended in this scenario that
compression be deferred.


---
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/orwell](https://github.com/joocer/orwell)  
