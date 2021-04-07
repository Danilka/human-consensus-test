from __future__ import annotations
from typing import List, Union
from message import Message
from .node_block import NodeBlock


class NodeMessage(NodeBlock):
    """Messages related Node functionality."""

    def send_message(self, message: Message, node_id: int):
        """ Send a message to a specific node. """

        self.transport.send(message, to_id=node_id)

    def broadcast(self, message: Message, exclude_node_ids: Union[None, List[int]] = None):
        """ Send message to everyone. """

        if isinstance(exclude_node_ids, list):
            exclude_node_ids.append(self.node_id)
        else:
            exclude_node_ids = [self.node_id]

        for i in range(len(self.nodes)):
            # Skipp sending messages excluded list.
            if i in exclude_node_ids:
                continue
            self.send_message(message, i)

    def delay_message(self, message: Message):
        """
        Saves a message to be processed later.
        :param message: Instance of a Message.
        """

        # Save message into the buffer.
        self.messages_buffer.append(message)

        # Sort the buffer.
        self.messages_buffer = sorted(self.messages_buffer, key=lambda x: x.blocks[0].block_id, reverse=True)

    def get_delayed_message(self) -> Message:
        """
        Pull a delayed message from self.messages_buffer if it is ready to be processed.
        :return: Instance of a Message.
        :raises: self.NodeValueError if there are no messages to be processed.
        """

        # Make sure there are messages waiting in the buffer.
        if not self.messages_buffer:
            raise self.NodeValueError("self.messages_buffer is empty. No messages to pull.")

        # Make sure the node is ready to process this message.
        next_block_id = self.get_next_block_id()
        if self.messages_buffer[-1].blocks[0].block_id > next_block_id:
            raise self.NodeValueError(
                "The node is not ready to process next message in from self.messages_buffer.\n"
                "Next block to be processed by this node is B{}. The message requires B{}".format(
                    next_block_id,
                    self.messages_buffer[-1].blocks[0].block_id,
                )
            )

        # Pull and return the message.
        return self.messages_buffer.pop()
