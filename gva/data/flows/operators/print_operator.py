"""
Print Operator

Prints the data object to the console.
"""
from .base_operator import BaseOperator


class PrintOperator(BaseOperator):
    """
    Prints the data to the console
    """
    def execute(self, data={}, context={}):
        print(data)
        return data, context
