# gva.data.formats

## What Is It?

A set of methods for handling complex data types.

## What Is In It?

[**dictset**](gva.data.formats.dictset.md) for handling lists or generators of dictionaries  
**graphs** for handling networkx graphs
[**display**](gva.data.formats.display.md) for formatting data for display purposes

## How Do I Use It?

### dictset
~~~python
from gva.data.formats import dictset

ds = something_which_returns_a_dictset()
ds = dictset.distinct(ds)
ds = dictset.select_from(ds, columns=['name', 'rank'])
dataframe = pandas.DataFrame(ds)
~~~

### graphs
~~~python
from gva.data.formats import graphs

graph = something_which_returns_a_graph()
graph = graphs.search_nodes(graph, {'type', 'person'})
graphs.show_graph(graph)
~~~