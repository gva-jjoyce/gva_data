"""
Filter Operator

Filters records, returns the record for matching records and
returns None for unmatching records.
"""
from .base_operator import BaseOperator


class FilterOperator(BaseOperator):

    @staticmethod
    def match_all(data):
        return True

    def __init__(self, condition=match_all):
        self.condition = condition
        super().__init__()

    def execute(self, data={}, context={}):
        if self.condition(data):
            return data, context
        return None
