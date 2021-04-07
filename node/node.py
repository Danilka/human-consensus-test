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
        :param Message message: Message object to be received by this node.
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

        # Validate an incoming message.
        if not self.validate_message(message_in):
            # If the message cannot be trusted, we simply disregard it.
            return

        # Not currently used.
        # return self.delay_message(message_in)

        # TYPE_CHAIN_UPDATE_REQUEST - Does not require message validation.
        if message_in.message_type == Message.TYPE_CHAIN_UPDATE_REQUEST:
            return self.receive_chain_update_request(message_in)

        # TYPE_CHAIN_UPDATE
        elif message_in.message_type == Message.TYPE_CHAIN_UPDATE:
            return self.receive_chain_update(message_in)

        # Pull candidate from the incoming message.
        if not self.message_to_candidate(message_in):
            # If we cannot pull a candidate, we cannot do anything else with this message.
            return

        # COMMIT
        if message_in.message_type == Message.TYPE_COMMIT:
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
