"""
Trace Blocks

As data moves between the flows, Trace Blocks is used to create a record of
operation being run. This should provide assurance that the data has not been
tampered with as it passes through the flow.

It uses an approach similar to a block-chain in that each block includes a
hash of the previous block.

The block contains a hash of the data, the name of the operation, a 
programatically determined version of the code that was run, a timestamp and
a hash of the last block.

This isn't distributed, but the intention is that the trace log writes the
block hash at the time the data is processed which this Class creating an
independant representation of the trace. In order to bypass this control,
the user must update the trace log and this trace block.
"""
import datetime
import hashlib
import os
from .json import parse, serialize
from ..data.formats import dictset

EMPTY_HASH = ['0'] * 64

def random_int() -> int:
    """
    Select a random integer (64bit)
    """
    return bytes_to_int(os.urandom(8))

def bytes_to_int(bytes: bytes) -> int:
    """
    Helper function, convert set of bytes to an integer
    """
    result = 0
    for b in bytes:
        result = result * 256 + int(b)
    return result

class TraceBlocks():

    __slots__ = ['blocks']

    def __init__(self, uuid="00000000-0000-0000-0000-000000000000"):
        """
        Create block chain and seed with the UUID.
        """
        self.blocks = []
        self.blocks.append({
            "block": 1,
            "timestamp": datetime.datetime.now().isoformat(),
            "uuid": uuid
        })

    def add_block(self,
                  data_hash=EMPTY_HASH,
                  operator="Not Specified",
                  version="-",
                  execution_ns=0):
        """
        Add a new block to the chain.
        """
        previous_block = self.blocks[-1]
        previous_block_hash = self.hash(previous_block)

        # proof is what makes mining for bitcoin so hard, we're setting a low
        # target of the last character being a 0, 4, 8, 12 (1/4 chance)
        # if you wanted to make this harder, set a different rule to exit
        # while loop
        proof = str(random_int())
        while self.hash(''.join([proof, previous_block_hash]))[-1] not in ['0', '4', '8', '12']:
            proof = str(random_int())

        block = {
            "block": len(self.blocks) + 1,
            "timestamp": datetime.datetime.now().isoformat(),
            "data_hash": data_hash,
            "previous_block_hash": previous_block_hash,
            "operator": operator,
            "version": version,
            "execution_ns": execution_ns,
            "proof": proof
        }
        self.blocks.append(block)

    def __str__(self):
        return serialize(self.blocks).decode()

    def hash(self, block):
        try:
            bytes_object = serialize(dictset.order(block))
        except:
            bytes_object = str(block).encode()
        raw_hash = hashlib.sha256(bytes_object)
        hex_hash = raw_hash.hexdigest()
        return hex_hash
