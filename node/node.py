from __future__ import annotations
import logging
from typing import Union, final
from candidate import Candidate
from message import Message
from .node_forge import NodeForge


@final
class Node(NodeForge):
    """
    Main Node class with final functionality.

    Inheritance:
    Node - Main loop and logic methods
    |-NodeForge - Block forging methods
      |-NodeChainUpdate - Chain sync related methods
        |-NodeVote - Vote related send and receive methods
          |-NodeApprove - Approval related send and receive methods
            |-NodeCommit - Initial commit generation
              |-NodeMessage - Messages transport methods
                |-NodeBlock - Block, chain, candidate functions and text representation
                  |-NodeValidator - Validators
                    |-NodeBase - Variables, constructor
                      |-NodeExceptions - Exceptions
    """

    def run(self, message: Union[Message, None] = None):
        """
        Main loop method.
        :param message: Message object to be received by this node.
        :return:
        """

        # Process an incoming message if we got one.
        if message:
            self.receive(message_in=message)

        # Process delayed messages.
        try:
            while self.messages_buffer:
                self.receive(message_in=self.get_delayed_message())
        except self.NodeValueError:
            # If there are still messages in self.messages_buffer, but they are not ready to be processed we keep going.
            pass

        # Pick the best candidate to work with.
        try:
            self.set_active_candidate()

            # Try sending an approve.
            self.send_approve_once()

            # Try sending an approve status update.
            self.send_approve_status_update_once()

            # Try sending a vote.
            self.send_vote_once()

            # Try sending a vote status update.
            self.send_vote_status_update_once()

            # Try forging the candidate.
            self.try_forging_candidate_block()
        except self.NodeValueError:
            # No candidates.

            # Try generating the block if we are the appropriate node for it.
            self.gen_commit()

            # Try voting for a blank block if there are no candidates.
            self.try_approving_blank_block()

        # Try requesting chain update.
        self.try_requesting_chain_update()

    def receive(self, message_in: Message):
        """Receive a message from another node."""

        # TYPE_CHAIN_UPDATE_REQUEST - Does not require message validation.
        if message_in.message_type == Message.TYPE_CHAIN_UPDATE_REQUEST:
            return self.receive_chain_update_request(message_in)
        # TYPE_CHAIN_UPDATE
        elif message_in.message_type == Message.TYPE_CHAIN_UPDATE:
            return self.receive_chain_update(message_in)

        # Validate an incoming message.
        try:
            if not self.validate_message(message_in):
                # If the message cannot be trusted, we simply disregard it.
                return
        except self.NodeNotEnoughData:
            # If there is not enough data to validate the message we save it for later.
            return self.delay_message(message_in)

        # Check if the block has already been forged.
        if message_in.blocks[0] in self.chain:
            # TODO: We should probably send the proof message to the requesting node, so it can forge the block as well.
            logging.debug(
                "N{} received a message from N{} and discarded it because this block is already forged.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return

        # Try finding passed block in the existing candidates.
        if message_in.blocks[0].block_id in self.candidates\
                and self.candidates[message_in.blocks[0].block_id].find(message_in.blocks[0]) is not None:
            try:
                # Check if the block is next to be processed.
                next_block_if = self.get_next_block_id()
                if next_block_if == message_in.blocks[0].block_id:
                    self.set_active_candidate(message_in.blocks[0])
                else:
                    # Means that the block is from the future, so we request a chain update from that block.
                    # We've already checked if this block was in the chain before.
                    self.request_chain_update(node_ids=[message_in.node_id])

                    # We used to delay this message.
                    # However, since the update is requested, we will get it as part of the update.
                    # So we just discard the message.
                    # self.delay_message(message_in)
                    return
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
        else:
            # Check if the block is from the future.
            if self.get_next_block_id() < message_in.blocks[0].block_id:
                # Request chain update from this node instead of processing this message.
                return self.request_chain_update(node_ids=[message_in.node_id])
            elif message_in.blocks[0].block_id < len(self.chain):
                # Check if the block has already been forged. Then send a chain update to that node.
                return self.send_chain_update(message_in.node_id, message_in.blocks[0].block_id-1)
            elif self.get_next_block_id() != message_in.blocks[0].block_id:
                raise ValueError(
                    "N{} received a message from N{} with a block #{} "
                    "that is not forged, not a future block, "
                    "and not the next in line {}. This should never happen.".format(
                        self.node_id,
                        message_in.node_id,
                        message_in.blocks[0].block_id,
                        self.get_next_block_id(),
                    )
                )

            # Create a candidate out of this block.
            self.add_candidate(Candidate(block=message_in.blocks[0]))

        # COMMIT
        if message_in.message_type == Message.TYPE_COMMIT:
            # Commit message is fully handled by the block validation. There is no need to do anything else really.
            return self.receive_commit(message_in)

        # APPROVE
        elif message_in.message_type == Message.TYPE_APPROVE:
            return self.receive_approve(message_in)

        # APPROVE_STATUS_UPDATE
        elif message_in.message_type == Message.TYPE_APPROVE_STATUS_UPDATE:
            return self.receive_approve_status_update(message_in)

        # VOTE
        elif message_in.message_type == Message.TYPE_VOTE:
            return self.receive_vote(message_in)

        # VOTE_STATUS_UPDATE
        elif message_in.message_type == Message.TYPE_VOTE_STATUS_UPDATE:
            return self.receive_vote_status_update(message_in)
