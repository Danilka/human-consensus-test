from __future__ import annotations
import logging
import time
from typing import List, Union
from candidate import Candidate
from message import Message
from .node_vote import NodeVote


class NodeChainUpdate(NodeVote):
    """Chain update related Node functionality."""

    def send_chain_update(self, node_id, starting_block_id: int = 0):
        """Send chain update to a specific node starting from a starting_block_id."""

        # Blocks that need to be included into the message.
        # Note that indexes in this list are not block IDs!
        if starting_block_id:
            chain_out = self.chain
        else:
            chain_out = []
            if starting_block_id < len(self.chain)-1:
                # Pick a diff between received block and the last block we have.
                for i in range(starting_block_id, len(self.chain)):
                    chain_out.append(self.chain[i])

        # Message
        next_block_id = self.get_next_block_id()
        message_out = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_CHAIN_UPDATE,
            chain=chain_out,
            candidates={
                next_block_id: self.candidates[next_block_id]
            } if next_block_id in self.candidates else None,
        )

        # Send
        self.send_message(message_out, node_id)
        return True

    def receive_chain_update(self, message_in: Message) -> bool:
        """Receive a chain update."""

        # Update chain.
        if message_in.chain:

            # Create searchable index.
            block_index = {}
            for block in message_in.chain:
                block_index[block.block_id] = block

            # Iterate and update self.chain for only the blocks we need.
            next_block_id = self.get_next_block_id()
            while next_block_id in block_index:
                if self.validate_block(block_index[next_block_id]):
                    self.chain.insert(next_block_id, block_index[next_block_id])
                else:
                    logging.warning(
                        "N{} received a chain update from N{}"
                        " and block {} is not valid. The block was discarded".format(
                            self.node_id,
                            message_in.node_id,
                            block_index[next_block_id]
                        )
                    )
                del block_index[next_block_id]
                next_block_id = self.get_next_block_id()

        # Update candidates.
        if message_in.candidates:
            candidate_id = self.get_next_block_id()  # To make sure we are getting the right candidate.
            if candidate_id not in message_in.candidates:
                # This is either because the update is from a node that is behind, or something is wrong.
                logging.warning(
                    "N{} received a chain update from N{}"
                    " and the candidate in the message is not for the next block in line.".format(
                        self.node_id,
                        message_in.node_id,
                    )
                )
                # nothing else to do at this point.
                return False

            candidate: Candidate
            for candidate in message_in.candidates[candidate_id]:
                local_candidate_id = self.candidates[candidate_id].find(candidate.block)
                if local_candidate_id is not None:
                    # We have a local candidate for the same block. Pick the best.
                    if self.candidates[candidate_id][local_candidate_id] < candidate:
                        self.candidates[candidate_id][local_candidate_id] = candidate
                else:
                    # We did not have a local candidate for the same block. Save received one.
                    self.candidates[candidate_id].append(candidate)

        return True

    def request_chain_update(self, node_ids: Union[None, List[int]] = None):
        # Get the last block in the chain.

        try:
            last_block = self.get_last_block()
        except self.NodeValueError:
            last_block = None

        # Prepare the request message.
        message = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_CHAIN_UPDATE_REQUEST,
            block=last_block,   # Signifies the last block we have.
        )

        # Send or broadcast the message.
        if not node_ids:
            self.broadcast(message)
        else:
            for node_id in node_ids:
                self.send_message(message, node_id)

        # Update the timer.
        self.time_update_requested = time.time()

    def try_requesting_chain_update(self):
        """Try to request a chain update from other nodes if there is enough of standby time."""

        # Check if it is time to request an update.
        if self.last_activity_time() + self.chain_update_timeout > time.time():
            return

        self.request_chain_update()

    def receive_chain_update_request(self, message_in: Message) -> bool:
        """Receive a chain update request."""

        # Blocks that need to be included into the message.
        # Note that indexes in this list are not block IDs!
        chain_out = []
        if not message_in.block:
            # The whole chain should be sent.
            chain_out = self.chain
        elif message_in.block.block_id < len(self.chain)-1:
            # Pick a diff between received block and the last block we have.
            for i in range(message_in.block.block_id+1, len(self.chain)):
                chain_out.append(self.chain[i])

        # Message
        next_block_id = self.get_next_block_id()
        message_out = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_CHAIN_UPDATE,
            chain=chain_out,
            candidates={
                next_block_id: self.candidates[next_block_id]
            } if next_block_id in self.candidates else None,
        )

        # Send
        self.send_message(message_out, message_in.node_id)
        return True
