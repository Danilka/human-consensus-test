import time


class Block:

    # Block ID.
    block_id: int

    # ID of the node that created the block.
    node_id: int

    # Time created.
    created: float

    def __init__(self, block_id: int, node_id: int):
        """Constructor"""
        self.block_id = block_id
        self.node_id = node_id
        self.created = time.time()

    def __eq__(self, other) -> bool:
        """Is another object is equal to self?"""
        if not isinstance(other, self.__class__):
            raise Exception("Cannot compare objects Block and {}".format(type(other)))
        return self.block_id == other.block_id and self.node_id == other.node_id

    def __ne__(self, other) -> bool:
        """Is another object is not equal to self?"""
        return not self.__eq__(other)

    def __str__(self) -> str:
        """Represent instance of the class as a string."""
        return "B{}byN{}".format(
            self.block_id,
            self.node_id,
        )
