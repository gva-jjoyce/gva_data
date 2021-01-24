# gva.logging

# What Is It?

A logging helper library using Python's logging module

# How Do I Use It?

1) Import library  
2) Fetch the logger  
3) Start Logging

~~~python
# import the library
from gva.logging import get_logger

# fetch the logger
logger = get_logger()

# start logging
logger.debug("this is a debug message")
~~~

The logger implements a new log levels **TRACE**, **AUDIT** and **ALERT**. 
These are accessed the same as other logging levels:

~~~python
logger.trace("This is a trace message")
logger.audit("This is an audit message")
logger.alert("This is an alert message")
~~~

# Log Levels

 Level        | Priority | Example Use
--------------|----------|-------------------------------
 **DEBUG**    | 10       | Low-level information to assist with debugging
 **INFO**     | 20       | Reporting of status information
 **WARNING**  | 30       | Features which will be deprecated or experimental features
 **ERROR**    | 40       | An error which is recoverable
 **CRITICAL** | 50       | An unrecoverable error
 **TRACE**    | 100      | Message tracing records
 **AUDIT**    | 110      | Information to be recorded for audit purposes
 **ALERT**    | 120      | A situation which requires immediate attention has occurred