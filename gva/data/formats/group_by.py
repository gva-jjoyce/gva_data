"""
Group By functionality for Iterables of Dictionaries
"""
from ...logging import get_logger
from typing import Callable

class Groups():
    """
    group_by
    
    Parameters:
    - dictset: an iterable of dictionaries
    - column: the field to group by
    
    Returns a 'group_by' object. The 'group_by' object holds the dataset in
    memory so is unsuitable for large datasets.
    """
    __slots__ = ('groups')

    def __init__(self, dictset, column):
        get_logger().warning('dictset.group_by is alpha functionality and subject to significant change - do not use in systems')
        groups = {}
        for item in dictset:
            my_item = item.copy()
            key = my_item.get(column)
            if groups.get(key) is None:
                groups[my_item.get(column)] = []
            del my_item[column]
            groups[key].append(my_item)
        self.groups = groups

    def count(self, value=None):
        """
        Count the number of items in groups
        
        Paramters:
        - value: (optional) if provided, return the count of just this group
        """
        if value is None:
            return {x:len(y) for x,y in self.groups.items()}
        else:
            try:
                return [len(y) for x,y in self.groups.items() if x == value].pop()
            except:
                return 0

    def aggregate(self, column, method):
        """
        Applies an aggregation function by group.
        
        Parameters:
        - column: the name of the field to aggregate
        - method: the function to aggregate with
        
        Examples:
        - maxes = grouped.aggregate('age', max)
        - means = grouped.aggregate('age', maths.mean)  
        """
        response = {}
        for key, items in self.groups.items():
            for item in items:
                values = [item.get(column) for item in items if item.get(column) is not None]
            response[key] = method(values)
        return response

    def apply(self, method: Callable):
        """
        Apply a function to all groups
        """
        return {key:method(items) for key, items in self.groups.items()}
            
    def __len__(self):
        """
        Returns the number of groups in the set.
        """
        return len(self.groups)

    def __repr__(self):
        """
        Returns the group names
        """
        return str(list(self.groups.keys()))
