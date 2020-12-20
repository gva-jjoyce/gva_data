"""
File Bin
"""
import time
import random
from .base_bin import BaseBin
import os

class FileBin(BaseBin):

    def __init__(
            self,
            bin_name: str,
            path: str):
        self.path = path
        self.name = bin_name
        os.makedirs(self.path, exist_ok=True)

    # does as little as possible to commit the record
    def __call__(
            self,
            record: str):
        # to reduce collisions we get the time in nanoseconds
        # and a random number between 1 and 1000
        file_name = F"{self.path}/{time.time_ns()}-{random.randrange(0,9999):04d}.txt"  # nosec - not crypto
        with open(file_name, 'w') as file:
            file.write(record)
