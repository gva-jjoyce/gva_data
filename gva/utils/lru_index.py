"""
LRU Index

Implements an arbitrary length LRU Index
"""
from typing import Any, List, Optional

class LRU_Index(object):

    __slots__ = ['hash_list']

    def __init__(self, size: int = 1000):
        self.hash_list: List[Optional[int]] = [None] * size

    def test(self, item: Any):
        item_hash = hash(item)
        if item_hash in self.hash_list:
            item_index = self.hash_list.index(item_hash)
            self.hash_list[:] = [item_hash] + self.hash_list[:item_index] + self.hash_list[item_index+1:]  # type:ignore
            self.hash_list.append(self.hash_list.pop(0))
            return True
        self.hash_list[0] = item_hash
        self.hash_list.append(self.hash_list.pop(0))
        return False

    def __call__(self, item: Any):
        return self.test(item)
