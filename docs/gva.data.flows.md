# gva.data.flows

## What Is It?

A set of modules to define and execute data flows.

Data Flows are designed to process Python dictionaries through a series of ordered steps such as validation, transformation and storage. These steps are called _operations_ implemented by _operators_. A series of _operators_ are called a _flow_.

## What Is In It?

Flows are sets of operators chained together. The library is a set of flows, with helper functions for co-ordination and logging.

The library contains the following set of operators:

**FilterOperator** - Uses a function to filter the _data_ payload    
**NoOpOperator** - No Operation, does nothing  
**PrintOperator** - Prints the _data_ payload to the screen  
**SaveToBucketOperator** - Save the _data_ payload to a json lines file  
**ValidatorOperator** - Tests the _data_ payload for conformity to a Schema  
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

---  
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/newidydd](https://github.com/joocer/newidydd) 