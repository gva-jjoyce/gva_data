# Advanced Topics

## Bin Writers

Bin Writers provide a secondary logging mechanism intended to be used in
addition to standard logging. Writers can log larger blocks of text, and
the writers have two scenarios they are used:

- Error Reporting
- Message Tracing

### Error Reporting

In addition to notifications about errors being reported using the logging
module, functionality exists to write full stack trace and state information
to a separate sink.

Each separate error event is saved to a separate file; a message is logged
to the standard logger with the error type and the name of the file created
by the Bin Writer.

There is no Error Bin Writer associated with flows by default; one needs to
be attached to the flow; three Writers are part of the library:

`FileBin(bin_name, path)` - writes to the file system  
`GoogleCloudStorageBin(bin_name, project, bucket, path)` - writes to Google Cloud Storage  
`MinioBin(bin_name,end_point, bucket, path, access_key, secret_key)` - writes to MinIO server  

### Message Tracing

Tracing provides functionality to log the journey messages take through and
between flows. Traces rely on the `context` information passed alongside
data into and through flows. 

The default behavior is 1/1000 runs are traced, this rate can be changed
when the _flow_ is run using the `trace_sample_rate` parameter. Setting to `1`
will trace every run, setting to `0` will not trace any runs. When the value
is between 0 and 1, runs are randomly sampled at the rate provided. Selection
is random, like dice rolls, selection is independent between samples.

`context` is a dictionary, this can contain many pieces of arbitrary data,
fields relating to tracing are:

- trace - a boolean indicating if the _run_ is being traced 
- execution_trace - the detailed record of the trace (see Block Trace)
- uuid - unique identifier for the run

The _uuid_ is created at the start of the _run_, if one does not already exist.

There is a default *TRACE* level logger, set at a higher priority than 
*CRITICAL*, so will log if any logging is enabled, this only logs very summary
information, additional information is available via the _execution_trace_.

The _execution_trace_ requires a Bin Writer to be attached in order for 
records to be written, a separate instantiation on a Writer must be made
and attached to the flow in order for the _execution_trace_s to be written.

### Block Trace

The block trace creates a record of the state of data at each _operation_ in
the flow. When one record separates into multiple messages, they inherit the
_trace_ status and the _uuid_ from the source message, to enable tracking of 
all states of the data.

The trace uses hashing to provide guarantees the data it records is an
accurate record of what was processed.
