"""
Base Reader
"""
import abc
from typing import Iterable
import datetime

class BaseReader(abc.ABC):

    def __init__(self, **kwargs):
        self.from_path = kwargs.get('from_path')

        date_range = kwargs.get('date_range')
        self.start_date = datetime.date.today()
        self.end_date = datetime.date.today()
        if isinstance(date_range, tuple):
            self.start_date, self.end_date = date_range
        self.start_date = kwargs.get('start_date', self.start_date)
        self.end_date = kwargs.get('end_date', self.end_date)

    def __del__(self):
        pass

    @abc.abstractmethod
    def list_of_sources(self) -> Iterable:
        pass 

    @abc.abstractmethod
    def read_from_source(self, item_name:str) -> Iterable:
        pass
