
import os
import sys
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva.utils import LRU_Index
try:
    from rich import traceback
    traceback.install()
except ImportError:
    pass

item_1 = 'one'
item_2 = 'two'
item_3 = 'three'
item_4 = 'four'
item_5 = 'five'

def test_lru():

    lru = LRU_Index(size=3)

    assert not lru(item_1), item_1
    assert not lru(item_2), item_2
    assert lru(item_2), item_2
    assert not lru(item_3), item_3
    assert lru(item_1), item_1
    assert not lru(item_4), item_4

    assert lru(item_1), item_1
    assert lru(item_3), item_3
    assert lru(item_4), item_4

    assert not lru(item_5), item_5
    assert lru(item_5), item_5
    assert lru(item_3), item_3
    assert lru(item_4), item_4

if __name__ == "__main__":
    test_lru()
