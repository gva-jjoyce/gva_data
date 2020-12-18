# gva.data.flows
 
![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)  
Forked from [joocer/newidydd](https://github.com/joocer/newidydd) 

## What Is It?

A set of modules to define and execute data flows.

Data Flows are designed to process Python dictionaries through a series of ordered steps such as validation, transformation and storage. These steps are called _operations_ implemented by _operators_. A series of _operators_ are referred to as _flows_.


## What Is In It?

Data Flows are sets of operators chained together. The library contains the following base set of operators:

**FilterOperator** - Uses a function to filter the _data_ payload    
**NoOpOperator** - No Operation, does nothing  
**PrintOperator** - Prints the _data_ payload to the screen  
**SaveToBucketOperator** - Save the _data_ payload to a json lines file  
**ValidatorOperator** - Tests the _data_ payload for conformity to a Schema  
**EndOperator** - Flow end marker  

## How Do I Use It?

Define the operations that are to be applied to the data using Operator Classes

Chain those operations together

run data through the flow.

~~~python
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
flow.go(data="22")
~~~

