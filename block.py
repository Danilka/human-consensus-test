import time
from typing import final, Union
from colored import bg, fg, attr


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
        """
        Represent block as a colored string.
        [4] <- example of a block with ID=4.
        ID is in green if the block is created by a node.
        ID is gray if it's a blank block.
        """

        return "{reset}{bg}{fg}{block_id}{reset}".format(
            bg=bg('green') if self.node_id is not None else bg('grey_30'),
            fg=fg('white'),
            reset=attr('reset'),
            block_id=self.block_id,
        )

    def __repr__(self) -> str:
        """
        Plain text representation of the block.
        B4byN7: Block body text. <- example of a block with ID 4, created by the node with ID 7.
        B4byN_: Blank block. <- example of a blank block ID 4
        """

        return "B{}byN{}{}".format(
            self.block_id,
            self.node_id if self.node_id is not None else "_",
            ": {}".format(self.body) if self.body else "",
        )
