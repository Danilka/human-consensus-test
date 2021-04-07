from __future__ import annotations
import logging
from abc import abstractmethod
from typing import List, Union
from candidate import Candidate
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
        self.messages_buffer = sorted(self.messages_buffer, key=lambda x: x.block.block_id, reverse=True)

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
        if self.messages_buffer[-1].block.block_id > next_block_id:
            raise self.NodeValueError(
                "The node is not ready to process next message in from self.messages_buffer.\n"
                "Next block to be processed by this node is B{}. The message requires B{}".format(
                    next_block_id,
                    self.messages_buffer[-1].block.block_id,
                )
            )

        # Pull and return the message.
        return self.messages_buffer.pop()

    def message_to_candidate(self, message_in: Message) -> bool:
        """
        Pull candidate from a message_in.block and add or pick current candidate.
        :param message_in:
        :return: True - if the candidate was successfully pulled. False - if not.
        """

        # If there is no block, we cannot do anything.
        if not message_in.block:
            return False

        # Check if the block has already been forged.
        if message_in.block.block_id < len(self.chain):
            # That node is behind. Send a chain update to it.
            self.send_chain_update(
                node_id=message_in.node_id,
                starting_block_id=message_in.block.block_id-1,
            )
            logging.debug(
                "N{} received a message from N{} and discarded it because this block is already forged.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return False

        next_block_id = self.get_next_block_id()

        # Check if the block is from the future.
        if message_in.block.block_id > next_block_id:
            # Request chain update from that node.
            self.request_chain_update(node_ids=[message_in.node_id])
            return False

        # Sanity check. At this point the block should be the next one.
        if next_block_id != message_in.block.block_id:
            raise ValueError(
                "N{} received a message from N{} with a block #{} "
                "that is not forged, not a future block, "
                "and not the next in line {}. This should never happen.".format(
                    self.node_id,
                    message_in.node_id,
                    message_in.block.block_id,
                    next_block_id,
                )
            )

        # Try finding passed block in the existing candidates.
        if message_in.block.block_id in self.candidates\
                and self.candidates[message_in.block.block_id].find(message_in.block) is not None:
            try:
                self.set_active_candidate(message_in.block)
                return True
            except self.NodeValueError as e:
                # This should not happen.
                logging.error(
                    "N{} received a message {} and tried to set active candidate from it, but it didn't work."
                    " Error message: {}".format(
                        self.node_id,
                        message_in,
                        e,
                    )
                )
                # This is critical, so we stop the program for now.
                # TODO: Remove the raise.
                raise e

        # Create a new candidate out of the received block.
        self.add_candidate(Candidate(block=message_in.block))

        return True

    @abstractmethod
    def send_chain_update(self, node_id, starting_block_id: int = 0):
        pass

    @abstractmethod
    def request_chain_update(self, node_ids: Union[None, List[int]] = None):
        pass
