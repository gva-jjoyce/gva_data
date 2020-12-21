"""
Base Bin

Implements common functions for each of the bins.
"""
import time
import random
import abc


class BaseBin(abc.ABC):

    def __init__(self, bin_name: str):
        self.name = bin_name

    def __str__(self) -> str:
        return self.name

    @abc.abstractmethod
    def __call__(self, record: str):
        raise NotImplementedError()

    def __ror__(self, flow):
        # set a attribute on the flow which calls this class
        setattr(flow, str(self), self)