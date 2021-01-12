# gva.flows

## What Is It?

A set of modules to define and execute data flows.

## What It Is Not

`gva.flows` does not schedule the execution of data flows, it only orchestrates the execution of a flow once it has been triggered.

## Terminology

**flow** - describes the steps and order of **how** data is going to be processed - this is an implementation of a [DAG](https://en.wikipedia.org/wiki/Directed_acyclic_graph#Data_processing_networks)   

**run** - a specific execution of a flow

**operator** - describe **what** work is being going to be done to, or with, data  

**data** - usually a python `dict`, but any piece of information which is acted on   

**context** - information separate to the data, but accompanies it through the pipeline to provide more information about what has happened to the data or to configure the operations  

## What Is In It?

Flows are sets of operators chained together. The library is a set of flows, with helper functions for co-ordination and logging.

The library contains the following set of operators:

**FilterOperator** - Uses a function to filter the _data_ payload    
**NoOpOperator** - No Operation, does nothing  
**PrintOperator** - Prints the _data_ payload to the screen  
**SaveToBucketOperator** - Save the _data_ payload to a json lines blob on GCS  
**SaveToMinioOperator** - Save the _data_ payload to a json lines file on MinIo (may be S3 compatible)    
**SaveToDiskOperator** - Save the _data_ payload to a json lines file
**ValidatorOperator** - Tests the _data_ payload for conformity to a Schema  
**SplitTextOperator** - Splits a text payload to _data_ into multiple messages
**EndOperator** - Flow end marker  

## How Do I Write My Own Operator?

The `BaseOperator` exists to allow creation of new operators. A minimum implementation only needs to override the `execute` method.

If initialization is required (for example opening a file or acquiring reference data), this should be done in the `__init __` method, to ensure the BaseOperator also performs initialization, `super().__init__()` must also be called. `__init__` can take any number of parameters and has no return.

The `execute` method is the only method that must be overridded, it is called for every record which reaches the operator. This method takes three parameters:

- _self_ - as per all class methods
- _data_ - a dictionary holding the data to be operated on
- _context_ - a dictionary holding information about the flow execution - such as scope and trace information

The return from `execute` must be one of the following:

- a tuple of the data and context - `data, context`
- a list, or generator, of data and context tuples
- `None` - to signify the next operator should not be executed

The optional `finalize` method is where any closure activities are executed, for example closing a database connection or file. This method takes no parameters and has no return.

### Example New Operator
~~~python
from gva.data.flows import BaseOperator

# inherit from the BaseOperator
class ExampleOperator(BaseOperator):
 
  def __init__(self):
    super().__init__()
    do_operator_init()

  def execute(self, data, context):
    data = do_something_to_the_data(data)
    return data, context

  def finalize(self):
    do_operator_close()
~~~


## How Do I Use It?

1) Define the operations that are to be applied to the data using _Operators_
2) Chain the operators together to create a _flow_
3) Run data through the _flow_.

~~~python
from gva.data.flows import BaseOperator

# Define the operations
class DoubleOperator(BaseOperator)
  def execute(data, context):
    return data * 2, context
    
class PrintOperator(BaseOperator)
  def execute(data, context):
    print(data)
    return data, context
    
# Chain them together
flow = DoubleOperator() > PrintOperator() > EndOperator()

# Run data through the flow
flow.run(data="22")
~~~

## Bin Writers

Bins are locations where logging information is written, separate to the `logging` sink;
there are two types of bins implemented:

- Error Bins - where more information about errors are written, such as stack traces
- Trace Bins - where information relating to message tracing is written

Bins are separate to logs for a few key reasons:

- The amount of information can be large, especially for a long flow or large data record
- The data in the stack or trace may include information not appropriate to store in logs
  which have less restrictive access permissions

Three Bin implementations have been written:

- `GoogleCloudStorageBin`
- `FileBin`
- `MinioBin`

---  
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/newidydd](https://github.com/joocer/newidydd) 