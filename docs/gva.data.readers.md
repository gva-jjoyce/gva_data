# gva.data.readers

## What Is It?

A data reader library.

The reader can read across multiple blobs in a GCS bucket with filter, select and and name pattern matching functionality.

## How Do I Use It?

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

---
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/orwell](https://github.com/joocer/orwell) 
