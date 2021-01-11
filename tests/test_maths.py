"""
Tests for paths to ensure the split and join methods
of paths return the expected values for various
stimulus.
"""
import datetime
import sys
import os
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from gva import maths
try:
    from rich import traceback
    traceback.install()
except ImportError:   # pragma: no cover
    pass


def test_maths():

    series = list(range(1, 101))

    # values cross-referenced against:
    # https://www.calculators.org/math/standard-deviation.php
    assert maths.mean(series) == 50.5
    assert maths.standard_deviation(series) == 29.011491975882016
    assert maths.variance(series) == 841.6666666666666

    series = []
    assert maths.mean(series) is None
    assert maths.standard_deviation(series) is None
    assert maths.variance(series) is None


if __name__ == "__main__":
    test_maths()

    print('okay')
