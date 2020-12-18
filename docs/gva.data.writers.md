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

![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/orwell](https://github.com/joocer/orwell) 