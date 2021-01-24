# gva.data.formats.display

# What Is It?

A library to help display data - this is not intended as a reporting interface.

# What Is In It?
  
`html_table(dictset, limit)` - Create a HTML table of the first _limit_ rows  
`ascii_table(dictset, limit)` - Create a ASCII table of the first _limit_ rows  

# How Do I Use It?

~~~python
from gva.data.formats import display

ds = [
    {'key': 1, 'value': 'one', 'plus1': 2},
    {'key': 2, 'value': 'two', 'plus1': 3},
    {'key': 3, 'value': 'three', 'plus1': 4}
]
print(display.ascii_table(ds))
~~~

will display:
~~~
┌─────┬───────┬───────┐
│ key │ value │ plus1 │
├─────┼───────┼───────┤
│  1  │  one  │   2   │
│  2  │  two  │   3   │
│  3  │ three │   4   │
└─────┴───────┴───────┘
~~~