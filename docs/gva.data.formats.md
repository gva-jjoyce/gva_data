# gva.data.formats

## What Is It?

A set of methods for handling complex data types.

## What Is In It?

[**dictset**](gva.data.formats.dictset.md) for handling lists or generators of dictionaries  
**graphs** for handling networkx graphs

## How Do I Use It?

### dictset
~~~python
from gva.data.formats.dictset import *

dictset = something_which_returns_a_dictset()
dictset = distinct(dictset)
dictset = select_from(dictset, columns['name', 'rank'])
dataframe = pandas.DataFrame(dictset)
~~~

### graphs
~~~python
from gva.data.formats.graphs import *

graph = something_which_returns_a_graph()
graph = search_nodes(graph, {'type', 'person'})
show_graph(graph)
~~~