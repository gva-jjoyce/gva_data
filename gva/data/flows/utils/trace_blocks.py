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
import json
import datetime
import hashlib

EMPTY_HASH = ['0'] * 64


class TraceBlocks():

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
                  version="-"):
        """
        Add a new block to the chain.
        """
        previous_block = self.blocks[-1]
        previous_block_hash = self.hash(previous_block)

        block = {
            "block": len(self.blocks) + 1,
            "timestamp": datetime.datetime.now().isoformat(),
            "data_hash": data_hash,
            "previous_block_hash": previous_block_hash,
            "operator": operator,
            "version": version
        }
        self.blocks.append(block)

    def __str__(self):
        return json.dumps(self.blocks)

    def hash(self, block):
        string_object = json.dumps(block, sort_keys=True)
        block_string = string_object.encode()
        raw_hash = hashlib.sha256(block_string)
        hex_hash = raw_hash.hexdigest()
        return hex_hash
