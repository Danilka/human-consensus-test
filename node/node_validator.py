from __future__ import annotations
import logging
from block import Block
from message import Message
from .node_base import NodeBase


class NodeValidator(NodeBase):
    """Validation methods for the BaseNode class."""

    def validate_block(self, block: Block) -> bool:
        """
        Verifies if a block is real.
        :param block: Instance of a Block.
        :return: True - block is valid. False - block is invalid.
        """

        # TODO Add block hash verification here.
        if block.node_id is None or (block.block_id % len(self.nodes) == block.node_id and block.block_id >= 0):
            return True
        return False

    def validate_message(self, message: Message) -> bool:
        """
        Check if a message is legitimate.
        :param message: Instance of a Message.
        :return: True -> message can be trusted. False -> message should not be trusted.
        :raises: self.NodeNotEnoughData if there is not enough data in self.chain and self.candidates to validate a message.
        """

        # Check message type.
        if not isinstance(message, Message):
            return False

        # Validate blocks in the chain if there are any.
        for block in message.chain:
            if not self.validate_block(block):
                logging.error(
                    "N{} received a message from N{} and discarded it because a block in a chain is invalid.".format(
                        self.node_id,
                        message.node_id
                    )
                )
                return False

        if message.message_type == Message.TYPE_CHAIN_UPDATE:
            # If it's a chain update, validation stops here.
            return True
        elif not message.block:
            # Otherwise check if the message has a block.
            logging.error(
                "N{} received a message from N{} and discarded because there are no blocks attached.".format(
                    self.node_id,
                    message.node_id,
                )
            )
            return False

        # Verify sent blocks in the message.
        if not self.validate_block(message.block):
            logging.error(
                "N{} received a message from N{} and discarded it because the block is invalid.".format(
                    self.node_id,
                    message.node_id
                )
            )
            return False

        # TODO: Add real signature message validation here.
        return True

    def validate_chain(self, chain=None) -> bool:
        """
        Validates passed or self.chain.
        :param chain: Ordered list of Block objects.
        :return: True - Chain is valid. False - chain is invalid.
        """
        # TODO Add chain signature verification.

        # Pick the chain that we are validating.
        if chain is None:
            chain = self.chain

        # Validate chain object type.
        if not isinstance(chain, list):
            return False

        # Check every block.
        last_block_id = None
        for block in chain:

            # Validate block's type.
            if not isinstance(block, Block):
                return False

            # Check previous block ID.
            if block.prev_block_id != last_block_id:
                return False
            last_block_id = block.prev_block_id

            # Validate the block itself.
            if not self.validate_block(block):
                return False

        # If everything passed before, we assume that the chain is valid.
        return True
