# flows: data flows

## What Is It?
A data flow definition and execution library

## How Do I Use It?
Define the operations that are to be applied to the data using Operator Classes

Chain those operations together

run data through the flow.

~~~
# Define the operations
class DoubleOperator(BaseOperator)
  def execute(data, context):
    return data * 2, context
    
class PrintOperator(BaseOperator)
  def execute(data, context):
    print(data)
    return data, context
    
# Chain them together
flow = DoubleOperator() > PrintOperator()

# Run data through
runner.go(flow=flow, data="22")
~~~

## How Do I Get It?
~~~
pip install --upgrade git+https://github.com/gva-jjoyce/gva_data_flows
~~~
or in your requirements.txt
~~~
git+https://github.com/gva-jjoyce/gva_data_flows
~~~
