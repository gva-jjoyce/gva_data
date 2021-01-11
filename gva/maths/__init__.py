"""
Maths

Adapted From:
https://github.com/joocer/timeseries/blob/master/timeseries/general.py

Licenced Under:
http://www.apache.org/licenses/LICENSE-2.0
"""
import math

# average of the series
def mean(series: list) -> float:
    s = [s for s in series if s is not None and not math.isnan(s)]
    if len(s) == 0:
        return None  # type:ignore
    return sum(s) / float(len(s))

# standard deviateion of the series
def standard_deviation(series: list) -> float:
    var = variance(series)
    if var is None:
        return None  # type:ignore
    return variance(series) ** (0.5)

# statistal variance of the series
def variance(series: list) -> float:
    s = [s for s in series if s is not None and not math.isnan(s)]
    if len(s) == 0:
        return None  # type:ignore
    series_mean = mean(s)
    return sum((x - series_mean) ** 2.0 for x in s) / (len(s) - 1)
