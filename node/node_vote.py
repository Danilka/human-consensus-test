from __future__ import annotations
import logging
from candidate import Candidate
from message import Message
from .node_approve import NodeApprove


class NodeVote(NodeApprove):
    """Vote related Node functions."""

    def enough_votes(self, message_chain=None) -> bool:
        """
        Check if we have enough votes for the current block_candidate.
        :return: True - there is enough votes, False - not enough votes.
        """
        if message_chain is None:
            return len(self.active_candidate.messages_vote) > len(self.nodes) / 2.0
        else:
            return len(message_chain) > len(self.nodes) / 2.0

    def enough_vote_status_updates(self) -> bool:
        """
        Check if we have enough vote status updates in the current candidate to try forging a new block.
        :return: True - enough votes, False, not enough votes.
        """
        return len(self.active_candidate.vote_status_updates) > len(self.nodes) / 2.0

    def send_vote_once(self):
        """Send vote message to everyone once."""

        # Check if we sent a vote for any candidate.
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_VOTE):
            return False

        # Check if we have enough approve status updates, we send a status update.
        if not self.enough_approve_status_updates():
            return False

        # Save the action we are taking.
        self.active_candidate.take_action(Candidate.ACTION_VOTE)

        # Save our own vote.
        self.active_candidate.messages_vote[self.node_id] = self.active_candidate.messages_approve

        # Prepare the message.
        message_out = Message(
            node_id=self.node_id,
            block=self.active_candidate.block,
            message_type=Message.TYPE_VOTE,
            # TODO: This should have a separate diff for each node with only messages that they need to reach approval.
            # messages_chain={**self.messages_vote, **{self.node_id: self.messages_approve}}
            messages_chain=self.active_candidate.messages_vote[self.node_id],
        )

        # Send.
        self.broadcast(message_out)
        return True

    def send_vote_status_update_once(self):
        """Send vote status update once if we have enough votes."""

        # Check if we sent a vote status update for any candidate.
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_VOTE_STATUS_UPDATE):
            return

        if not self.enough_votes():
            return

        # Prepare the message.
        message_out = Message(
            node_id=self.node_id,
            block=self.active_candidate.block,
            message_type=Message.TYPE_VOTE_STATUS_UPDATE,
            # TODO: This should have a separate diff for each node with only messages
            # that they need to reach votes.
            messages_chain=self.active_candidate.messages_vote,
        )

        # Set a flag that we have sent this update out.
        self.active_candidate.take_action(Candidate.ACTION_VOTE_STATUS_UPDATE)

        # Increment the vote_status update counter with our own info.
        self.active_candidate.vote_status_updates.add(self.node_id)

        # Broadcast the message.
        self.broadcast(message_out)

    def receive_vote(self, message_in: Message):
        """Receive a vote message."""

        # If we already have this message, so we disregard it.
        if message_in.node_id in self.active_candidate.messages_vote:
            logging.info(
                "N{} received a vote from N{}, but already had it.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return

        # Save the vote.
        self.active_candidate.messages_vote[message_in.node_id] = message_in.messages_chain

    def receive_vote_status_update(self, message_in: Message):
        """Receive a vote status update message."""
        # If we are getting this, it means that the block is forged by others, but not us.
        # So we update the info and try to forge ourselves.

        # Update our vote messages chain.
        for node_id_in in message_in.messages_chain.keys():
            if node_id_in not in self.active_candidate.messages_vote:
                self.active_candidate.messages_vote[node_id_in] = message_in.messages_chain[node_id_in]
            # TODO: We do not need to update chains for the votes that we already have. They should be the exact same.
            else:
                if self.active_candidate.messages_vote[node_id_in] != message_in.messages_chain[node_id_in]:
                    logging.debug(
                        "N{} received a vote status update from N{}. "
                        "In the payload there was a vote proof for N{}'s vote. "
                        "N{} had a local copy that differs from the received proof.\n"
                        "Local proof: {}\n"
                        "Received proof: {}".format(
                            self.node_id,
                            message_in.node_id,
                            node_id_in,
                            self.node_id,
                            self.active_candidate.messages_vote[node_id_in],
                            message_in.messages_chain[node_id_in],
                        )
                    )

        # Increment the vote_status update counter with the info we got.
        self.active_candidate.vote_status_updates.add(message_in.node_id)
