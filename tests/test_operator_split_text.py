"""
Test Split Text Operator
"""
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.data.flows.operators import SplitTextOperator


def test_split_text_default():

    TEXT = "line 1\nline 2\nline 3"
    op = SplitTextOperator()
    # the operator returns a generator
    result = op(TEXT)  

    # we need to enumerate the generator to get the data fields
    data = []
    for d, c in result:
        data.append(d)

    assert data == ['line 1', 'line 2', 'line 3']


def test_split_text_non_default():

    TEXT = "line 1,line 2,line 3"
    op = SplitTextOperator(separator=',')
    # the operator returns a generator
    result = op(TEXT)  

    # we need to enumerate the generator to get the data fields
    data = []
    for d, c in result:
        data.append(d)

    assert data == ['line 1', 'line 2', 'line 3']


def test_split_text_not_splittable():

    TEXT = "I have no instance of the separator"
    op = SplitTextOperator()
    # the operator returns a generator
    result = op(TEXT)  

    # we need to enumerate the generator to get the data fields
    data = []
    for d, c in result:
        data.append(d)

    assert data == ['I have no instance of the separator']


def test_split_text_only_separator():

    # the text is just instances of the separator
    TEXT = "\n\n\n\n\n\n"
    op = SplitTextOperator()
    # the operator returns a generator
    result = op(TEXT)  

    # we need to enumerate the generator to get the data fields
    data = []
    for d, c in result:
        data.append(d)

    assert data == ['', '', '', '', '', '', '']


if __name__ == "__main__":
    test_split_text_default()
    test_split_text_non_default()
    test_split_text_not_splittable()
    test_split_text_only_separator()