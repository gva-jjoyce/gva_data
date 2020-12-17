"""
End Operator

Explicit marker for the end of a pipeline.

This helps to prevent problems with the DAG builder
"""
from .base_operator import BaseOperator


class EndOperator(BaseOperator):

    def execute(self, data={}, context={}):
        pass
