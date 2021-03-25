from __future__ import annotations
from typing import Union

from block import Block


class Message:

    # Possible message types. e.g. Use Message.TYPE_APPROVE to send an approve.
    TYPE_COMMIT = "commit"
    TYPE_APPROVE = "approve"
    TYPE_VOTE = "vote"
    TYPE_APPROVE_STATUS_UPDATE = "approve_status_update"
    TYPE_VOTE_STATUS_UPDATE = "vote_status_update"
    TYPE_CHAIN_UPDATE_REQUEST = "chain_update_request"
    TYPE_CHAIN_UPDATE = "chain_update"

    node_id: int
    message_type: str
    block: Block
    messages_chain: Union[list, dict, None]

    def __init__(
        self,
        node_id: int,
        message_type: str,
        block: Union[None, Block] = None,
        messages_chain: Union[list, dict, None] = None,
    ):
        """
        Constructor
        :param node_id: ID of the node sending the message.
        :param message_type: One of the self.TYPE_***
        :param block: block that is attached to the message.
        :param messages_chain: Messages that prove the block.
        """
        self.node_id = node_id
        self.message_type = message_type
        self.block = block
        self.messages_chain = messages_chain

    def __eq__(self, other) -> bool:
        """Is another object is equal to self?"""
        if not isinstance(other, self.__class__):
            raise Exception("Cannot compare objects Message and {}".format(type(other)))
        if (self.node_id, self.message_type, self.block) != (other.node_id, other.message_type, other.block):
            return False
        if not self.messages_chain and not other.messages_chain:
            return True
        if len(self.messages_chain) != len(other.messages_chain):
            return False
        for key, val in self.messages_chain.items():
            if val != other.messages_chain[key]:
                return False
        return True

    def __ne__(self, other) -> bool:
        """Is another object is not equal to self?"""
        return not self.__eq__(other)

    def __str__(self) -> str:
        """Represent instance of the class as a string."""
        return "[{message_type}] B{block_id} messages_chain size = {messages_chain_size}".format(
            message_type=self.message_type,
            block_id=self.block.block_id if self.block else "-",
            messages_chain_size=len(self.messages_chain) if self.messages_chain else 0,
        )
