from __future__ import annotations
from block import Block
from candidate import Candidate
from message import Message
from .node_message import NodeMessage


class NodeCommit(NodeMessage):
    """Commit related Node functionality."""

    def gen_commit(self) -> bool:
        if self.node_id != self.get_next_block_node_id():
            # logging.error("Node #{} tried to generate a commit, while it should have been generated by {}".format(
            #     self.node_id,
            #     self.get_next_block_node_id(),
            # ))
            return False

        # Create a new block.
        block = Block(
            block_id=self.get_next_block_id(),
            node_id=self.node_id,
            body="Block is generated by N{}.".format(self.node_id),     # TODO: Replace with something meaningful.
        )

        self.add_candidate(Candidate(block))

        # Gen broadcast message.
        message = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_COMMIT,
            blocks=[block],
        )

        self.broadcast(message)
        return True

    def receive_commit(self, message_in: Message) -> bool:
        """Receive a commit message."""
        # This logic is already handled by block validation in self.receive()
        return True
