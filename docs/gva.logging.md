# gva.logging

## What Is It?

A logging helper library using Python's logging module

## How Do I Use It?

1 - Import library  
2 - Fetch the logger  
3 - Start Logging

~~~python
from gva.logging import get_logger
logger = get_logger()
logger.debug("this is a debug message")
~~~

The logger implements a new log level TRACE. Trace is level 100 (higer than any of the default levels) and is accessed as per other log levels:

~~~python
logger.trace("This is a trace message")
~~~
