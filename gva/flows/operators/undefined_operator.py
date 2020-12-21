"""
Undefined Operator

Used as a default operator when one needs to be defined but isn't.
As that would be an error situation, this operator raises an
exception.
"""
from .base_operator import BaseOperator


class UndefinedOperator(BaseOperator):

    def execute(self, data={}, context={}):
        raise NotImplementedError()
