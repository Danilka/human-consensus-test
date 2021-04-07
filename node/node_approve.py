from __future__ import annotations
import logging
import time
from block import Block
from candidate import Candidate
from message import Message
from .node_commit import NodeCommit


class NodeApprove(NodeCommit):
    """Approve related Node functions."""

    def send_approve_once(self):
        """Send approval message to everyone once."""

        # Check if we sent an approve for any candidate.
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_APPROVE):
            return False

        # Save the action we are taking.
        self.active_candidate.take_action(Candidate.ACTION_APPROVE)

        # Prepare the message.
        message_out = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_APPROVE,
            block=self.active_candidate.block,
        )

        # Save approve message into our own log as well.
        self.active_candidate.messages_approve[self.node_id] = message_out
        self.broadcast(message_out)
        return True

    def enough_approves(self, message_chain=None) -> bool:
        """
        Check if we have enough approves for the current block_candidate.
        :return: True - there is enough approves, False - not enough approves.
        """
        if message_chain is None:
            if len(self.active_candidate.messages_approve) > len(self.nodes) / 2.0:
                return True
            return False
        else:
            if len(message_chain) > len(self.nodes) / 2.0:
                return True
            return False

    def send_approve_status_update_once(self):
        """Send approve status update once if we have enough approves."""

        # Check if we sent an approve status update for any candidate.
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_APPROVE_STATUS_UPDATE):
            return

        if not self.enough_approves():
            return

        # Set a flag that we have sent this update out.
        self.active_candidate.take_action(Candidate.ACTION_APPROVE_STATUS_UPDATE)

        # Increment the vote_status update counter with our own info.
        self.active_candidate.approve_status_updates.add(self.node_id)

        message_out = Message(
            node_id=self.node_id,
            block=self.active_candidate.block,
            message_type=Message.TYPE_APPROVE_STATUS_UPDATE,
            # TODO: This should have a separate diff for each node with only messages
            # that they need to reach approval.
            messages_chain=self.active_candidate.messages_approve,
        )
        self.broadcast(message_out)

    def enough_approve_status_updates(self) -> bool:
        """
        Check if we have enough approve status updates in the current candidate to vote for it.
        :return: True - enough approve status updates, False, not enough approve status updates.
        """
        return len(self.active_candidate.approve_status_updates) > len(self.nodes) / 2.0

    def receive_approve(self, message_in: Message):
        """Receive an approve message."""

        # If we already sent approve status update, we do not need extra approves.
        if not self.keep_excessive_messages and self.candidates[self.active_candidate.block.block_id].check_action(
            Candidate.ACTION_APPROVE_STATUS_UPDATE
        ):
            return

        if message_in.node_id in self.active_candidate.messages_approve:
            # We already have this message, so we disregard it.
            logging.debug(
                "N{} received an approve from N{}, but already had it.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return

        # Save incoming approve.
        self.active_candidate.messages_approve[message_in.node_id] = message_in

    def receive_approve_status_update(self, message_in: Message):
        """Receive an approve status update message."""

        # Verify message chain.
        for _, message_in_chain in message_in.messages_chain.items():
            if not self.validate_block(message_in_chain.block):
                # Got a message with a wrong block.
                logging.error("N{} received an approve status update from N{} with a wrong block".format(
                    self.node_id,
                    message_in.node_id,
                ))
                return

        if not self.enough_approves(message_in.messages_chain):
            # This means that there is not enough votes for approval.
            logging.error(
                "N{} received an approve status update from N{} with not enough votes in it".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return

        if self.active_candidate and self.active_candidate.block != message_in.block:
            # This means that my block is different from the one that is being approved.
            # TODO: We need to find the difference and update our chain up to this block.
            logging.error(
                "N{} received an approve status update from N{} with a block ({}) "
                "that differs from my candidate ({}).".format(
                    self.node_id,
                    message_in.node_id,
                    message_in.block,
                    self.active_candidate.block,
                ))
            return

        ### At this point the blocks match and the messages chan from that node is correct. ###

        # Update our messages_approve with the new info from the message.
        if not self.active_candidate.check_action(Candidate.ACTION_VOTE) or self.keep_excessive_messages:
            self.active_candidate.messages_approve.update(message_in.messages_chain)

        # Increment the approve_status_update counter with the info we got.
        self.active_candidate.approve_status_updates.add(message_in.node_id)

        # Save the whole approve message chain for that node.
        # TODO: Comment this out as the other node's approval chain could still fill up and be updated.
        # self.messages_vote[message_in.node_id] = message_in.messages_chain

    def try_approving_blank_block(self):
        """Try to nominate a blank block as the next block in the chain if no candidate was received."""

        # We assume that the active candidate is the next valid block.
        # It is only blank when there are no valid candidates.
        if self.active_candidate:
            return

        # Check if it is time to nominate a blank block.
        if self.time_forged + self.blank_block_timeout > time.time():
            return

        # Create blank block.
        blank_block = Block(
            block_id=self.get_next_block_id(),
            node_id=None,   # Set to None to signify that this is a blank block.
            body="Blank block.",    # TODO: Replace with something meaningful.
        )

        self.add_candidate(Candidate(blank_block))

        # Approve the blank block.
        self.send_approve_once()
