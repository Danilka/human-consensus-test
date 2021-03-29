import time
from typing import final, Union


@final
class Block:

    # Block ID.
    block_id: int

    # Previous Block ID. Shadow parameter
    prev_block_id: int

    # ID of the node that created the block.
    # node_id = None if the block is blank.
    node_id: Union[int, None]

    # Main block payload.
    body: str

    # Time created.
    created: float

    def __init__(self, block_id: int, node_id: Union[int, None], body: str = ""):
        """
        Constructor.
        :param block_id: Sequential block number in the main chain.
        :param node_id: ID of the node that created this block. Set to None for a blank block.
        """
        self.block_id = block_id
        self.node_id = node_id
        self.body = body
        self.created = time.time()

    def __getattr__(self, item):
        # Generate self.prev_block_id.
        if item == "prev_block_id":
            return self.block_id-1 if self.block_id >= 0 else None
        return self.__getattribute__(item)

    def __eq__(self, other) -> bool:
        """Is another object equal to self?"""
        if not isinstance(other, self.__class__):
            return NotImplemented

        # TODO: We should probably match hash and bodies.
        #  However this doesn't work for blank blocks that have different bodies, but still need to match at the moment.
        return self.block_id == other.block_id and self.node_id == other.node_id

    def __ne__(self, other) -> bool:
        """Is another object not equal to self?"""
        return not self.__eq__(other)

    def __str__(self) -> str:
        """Represent instance of the class as a string."""
        return "B{}byN{}{}".format(
            self.block_id,
            self.node_id if self.node_id is not None else "_",
            ": {}".format(self.body) if self.body else "",
        )

    def __repr__(self):
        return self.__str__()
