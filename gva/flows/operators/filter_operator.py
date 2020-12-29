"""
Filter Operator

Filters records, returns the record for matching records and returns 'None'
for non-matching records.

The Filter Operator takes one configuration item at creation - a Callable
(function) which takes the data as a dictionary as it's only parameter and
returns 'true' to retain the record, or 'false' to not pass the record 
through to the next operator.

Example Instantiation:

filter_ = FilterOperator(condition=lambda r: r.get('severity') == 'high')

The condition does not need to be lambda, it can be any Callable including 
methods.
"""
from .base_operator import BaseOperator

def match_all(data):
    return True

class FilterOperator(BaseOperator):

    def __init__(self, condition=match_all):
        self.condition = condition
        super().__init__()

    def execute(self, data={}, context={}):
        if self.condition(data):
            return data, context
        return None
