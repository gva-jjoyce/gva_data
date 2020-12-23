"""
Split Text Operator

Splits a text payload into multiple messages but a given separator.
"""
from . import BaseOperator


class SplitTextOperator(BaseOperator):

    def __init__(self, separator='\n'):
        self.separator = separator
        super().__init__()

    def execute(self, data, context):
        
        split = data.split(self.separator)
        for item in split:
            yield item, context
