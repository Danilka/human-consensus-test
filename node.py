from __future__ import annotations
import logging
import time
from collections import defaultdict
from typing import List, Union
from block import Block
from candidate import Candidate
from message import Message
from transport import Transport


class Node:

    # ID of the node.
    node_id: int

    # Pointer to the list of all nodes.
    nodes: List[Node]

    # Current chain. Blocks must be sorted. Chain has to start with block #0.
    # TODO Add hash support to start from mid chain.
    chain: List[Block]

    # Dict of lists of Candidates.
    # Use self.add_candidate to write.
    candidates: dict    # self.candidates[BLOCK_ID] -> [Candidate, Candidate, ...]

    # Pointer to the current active candidate.
    active_candidate: Union[Candidate, None]

    # Link to a global Transport object
    transport: Transport

    # Buffer for messages that the node is not ready to process.
    # Ordered by item.block.block_id - Last is the lowest block number.
    # Should only be written by self.delay_message() method.
    messages_buffer: List[Message]

    # Should we keep extra messages after a proof was achieved.
    keep_excessive_messages: bool

    # Time control variables.
    time_forged: float
    time_approved: float
    blank_block_timeout: float

    def __init__(
        self,
        nodes: List[Node],
        node_id: int,
        transport: Transport,
        chain: Union[List, None] = None,
        keep_excessive_messages: bool = False,
        blank_block_timeout: float = 2.0,
    ):
        """
        Constructor
        :param nodes: Pointer to a list of already instantiated nodes. This node will add itself there.
        :param node_id: ID of this node.
        :param transport: Pointer to an instance of the Transport that handles message transfer between nodes.
        :param chain: Chain of forged blocks.
        :param keep_excessive_messages: Keep saving update messages inside self.candidates after a state has been proven.
        :param blank_block_timeout: Timeout before a blank block would be nominated as a next block.
        """

        # Validate and set the chain.
        if not self.validate_chain(chain):
            raise ValueError("Passed chain is invalid. Cannot instantiate a new Node object with this chain.")
        self.chain = chain if chain else []

        self.node_id = node_id
        self.candidates = defaultdict(list)
        self.active_candidate = None
        self.transport = transport

        # Set nodes pull.
        nodes.append(self)
        self.nodes = nodes

        # Messages related settings.
        self.keep_excessive_messages = keep_excessive_messages
        self.blank_block_timeout = blank_block_timeout
        self.messages_buffer = []

        # Start all timers at node's declaration.
        # We assume that there are either no blocks in the chain or the last one was forged and approved right now.
        self.time_forged = time.time()
        self.time_approved = time.time()

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

        # Try generating the block if we are the appropriate node for it.
        if not self.active_candidate:
            self.gen_commit()

        # Try voting for a blank block if there are no candidates.
        if not self.active_candidate:
            self.try_voting_blank_block()

    def receive(self, message_in: Message) -> bool:
        """Receive a message from another node."""

        # Validate an incoming message.
        try:
            if not self.validate_message(message_in):
                # If the message cannot be trusted, we simply disregard it.
                return False
        except self.NodeNotEnoughData:
            # If there is not enough data to validate the message we save it for later.
            self.delay_message(message_in)
            return False

        # Check if the block has already been forged.
        if message_in.block in self.chain:
            # TODO: We should probably send the proof message to the requesting node, so it can forge the block as well.
            logging.debug(
                "N{} received a message from N{} and discarded it because this block is already forged.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return False

        # Save new block as a candidate if we do not already have it.
        if message_in.block.block_id not in self.candidates:
            # Create a candidate out of this block.
            if self.add_candidate(Candidate(block=message_in.block)):
                # Since we just got a new block and verified it to be good, we broadcast an approval for it.
                self.send_approve_once()
            else:
                # Sanity check, this should not trigger.
                logging.error(
                    "N{} received a message {} and tried to add a candidate for it, but it didn't work.".format(
                        self.node_id,
                        message_in,
                    )
                )

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

    def receive_commit(self, message_in: Message) -> bool:
        """Receive a commit message."""
        # This logic is already handled by block validation in self.receive()
        return True

    def receive_approve(self, message_in: Message) -> bool:
        """Receive an approve message."""

        # If we already sent approve status update, we do not need extra approves.
        approve_status_update_sent = False
        for candidate in self.candidates[self.active_candidate.block.block_id]:
            if candidate.check_action(Candidate.ACTION_APPROVE_STATUS_UPDATE):
                approve_status_update_sent = True

        if not self.keep_excessive_messages and approve_status_update_sent:
            return False

        if message_in.node_id in self.active_candidate.messages_approve:
            # We already have this message, so we disregard it.
            logging.debug(
                "N{} received an approve from N{}, but already had it.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return True

        # Save incoming approve.
        self.active_candidate.messages_approve[message_in.node_id] = message_in

        # Send approve status update if we need to.
        self.send_approve_status_update_once()

        return True

    def receive_approve_status_update(self, message_in: Message) -> bool:
        """Receive an approve status update message."""

        # Verify message chain.
        for _, message_in_chain in message_in.messages_chain.items():
            if not self.verify_block(message_in_chain.block):
                # Got a message with a wrong block.
                logging.error("N{} received an approve status update from N{} with a wrong block".format(
                    self.node_id,
                    message_in.node_id,
                ))
                return False

        if not self.enough_approves(message_in.messages_chain):
            # This means that there is not enough votes for approval.
            logging.error(
                "N{} received an approve status update from N{} with not enough votes in it".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return False

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
            return False

        ### At this point the blocks match and the messages chan from that node is correct. ###

        # Update our messages_approve with the new info from the message.
        if not self.active_candidate.check_action(Candidate.ACTION_VOTE) or self.keep_excessive_messages:
            self.active_candidate.messages_approve.update(message_in.messages_chain)

        # Increment the approve_status_update counter with the info we got.
        self.active_candidate.approve_status_updates.add(message_in.node_id)

        # Save the whole approve message chain for that node.
        # TODO: Comment this out as the other node's approval chain could still fill up and be updated.
        # self.messages_vote[message_in.node_id] = message_in.messages_chain

        # We have enough approves, now we also send out our vote.
        self.send_vote_once()

        return True

    def receive_vote(self, message_in: Message) -> bool:
        """Receive a vote message."""

        # If we already have this message, so we disregard it.
        if message_in.node_id in self.active_candidate.messages_vote:
            logging.info(
                "N{} received a vote from N{}, but already had it.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return False

        # Save the vote.
        self.active_candidate.messages_vote[message_in.node_id] = message_in.messages_chain

        # Try sending vote status update if we have enough votes.
        self.send_vote_status_update_once()

        # Try forging the candidate block.
        self.try_forging_candidate_block()

        return True

    def receive_vote_status_update(self, message_in: Message) -> bool:
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

        # The rest, in theory, should always pass.
        # Since we just got a vote status update that has complete info for this block.

        # Send vote status update if we need to.
        self.send_vote_status_update_once()

        # Try forging the candidate block.
        self.try_forging_candidate_block()

        return True

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
            block=block,
        )

        self.broadcast(message)
        return True

    def get_next_block_id(self) -> int:
        if not self.chain:
            return 0
        return len(self.chain)

    def get_next_block_node_id(self) -> int:
        # Get next block master node ID
        # This assumes that nodes never change after initiating the group.
        return self.get_next_block_id() % len(self.nodes)

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
            self.transport.send(message, to_id=i)

    def delay_message(self, message: Message):
        """
        Saves a message to be processed later.
        :param message: Instance of a Message.
        :return:
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

    def try_voting_blank_block(self) -> bool:
        """
        Try to nominate a blank block as the next block in the chain if no candidate was received.
        :return: True - Blank block ws nominated. False - there is no need to nominate blank block.
        """

        # We assume that the active candidate is the next valid block.
        # It is only blank when there is no valid candidate.
        if self.active_candidate:
            return True

        # Check if it is time to nominate a blank block.
        if self.time_forged + self.blank_block_timeout > time.time():
            return False

        # Create blank block.
        blank_block = Block(
            block_id=self.get_next_block_id(),
            node_id=None,   # Set to None to signify that this is a blank block.
            body="Blank block.",    # TODO: Replace with something meaningful.
        )

        self.add_candidate(Candidate(blank_block))

        # Approve the blank block.
        self.send_approve_once()

        return True

    def send_approve_once(self):
        """Send approval message to everyone once."""

        # Check if we sent an approve for any candidate.
        for candidate in self.candidates[self.active_candidate.block.block_id]:
            if candidate.check_action(Candidate.ACTION_APPROVE):
                return False

        # Save the action we are taking.
        self.active_candidate.actions_taken.add(Candidate.ACTION_APPROVE)

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

    def send_approve_status_update_once(self) -> bool:
        """
        Send approve status update once if we have enough approves.
        :return: True - update was sent. False - update was not sent.
        """

        # Check if we sent an approve status update for any candidate.
        for candidate in self.candidates[self.active_candidate.block.block_id]:
            if candidate.check_action(Candidate.ACTION_APPROVE_STATUS_UPDATE):
                return False

        if not self.enough_approves():
            return False

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

        return True

    def send_vote_once(self):
        """Send vote message to everyone once."""

        # Check if we sent a vote for any candidate.
        for candidate in self.candidates[self.active_candidate.block.block_id]:
            if candidate.check_action(Candidate.ACTION_VOTE):
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

    def send_vote_status_update_once(self) -> bool:
        """
        Send vote status update once if we have enough votes.
        :return: True - update was sent. False - update was not sent.
        """

        # Check if we sent a vote status update for any candidate.
        for candidate in self.candidates[self.active_candidate.block.block_id]:
            if candidate.check_action(Candidate.ACTION_VOTE_STATUS_UPDATE):
                return False

        if not self.enough_votes():
            return False

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

        return True

    def try_forging_candidate_block(self) -> bool:
        """
        Attempt to forge a candidate block and send vote status update.
        :return: True - Block is forged, False - block is not forged.
        """

        # Block validation
        if not self.active_candidate:
            logging.info("N{} Unsuccessful forge attempt, there is no candidate block.".format(self.node_id))
            return False

        # Check if we have enough vote status updates.
        if not self.enough_vote_status_updates():
            return False

        # Just in case validating votes, but this should pass if the vote status update has passed.
        if not self.enough_votes():
            return False

        # Log successful forge attempt.
        logging.info("N{} B{} is forged.".format(self.node_id, self.active_candidate.block.block_id))

        # Forge the block and add it to the chain.
        self.chain.append(self.active_candidate.block)
        self.active_candidate.forged = True
        self.active_candidate = None

        # Reset timer since the last forged block.
        self.time_forged = time.time()

        # Moved to self.run() method.
        # # Try generating the next block if we are the appropriate node.
        # self.gen_commit()

        return True

    def enough_approve_status_updates(self) -> bool:
        """
        Check if we have enough approve status updates in the current candidate to vote for it.
        :return: True - enough approve status updates, False, not enough approve status updates.
        """
        return len(self.active_candidate.approve_status_updates) > len(self.nodes) / 2.0

    def enough_vote_status_updates(self) -> bool:
        """
        Check if we have enough vote status updates in the current candidate to try forging a new block.
        :return: True - enough votes, False, not enough votes.
        """
        return len(self.active_candidate.vote_status_updates) > len(self.nodes) / 2.0

    def add_candidate(self, candidate: Candidate):
        """
        Adds a candidate to self.candidates chain and set new self.active_candidate.
        :param candidate: Instance of a Candidate.
        :return: True - candidate is added, False - not added.
        """

        # Check if there is a candidate with the same block already in there.
        existing_candidate: Candidate
        for existing_candidate in self.candidates[candidate.block.block_id]:
            if existing_candidate.block == candidate.block:
                if existing_candidate == existing_candidate:
                    # if the same candidate is already there, it's ok.
                    return False
                else:
                    # This is not ok when there is a different candidate with the same block.
                    logging.error(
                        "N{} is trying to add a candidate to self.candidates "
                        "that already has another candidate with the same block. B{}".format(
                            self.node_id,
                            candidate.block.block_id,
                        )
                    )
                    return False

        self.candidates[candidate.block.block_id].append(candidate)

        # Set new active_candidate after adding a new one in the mix.
        self.set_active_candidate()

        return True

    def set_active_candidate(self):
        """Sets self.active_candidate to the current candidate."""
        self.active_candidate = self.get_candidate()

    def get_candidate(self) -> Candidate:
        """
        Get a candidate that is next in line after the last forged block.
        :raises: NodeValueError if there is no candidate to return.
        :return: Pointer to a Candidate instance in self.candidates.
        """

        # Get the next block ID.
        next_block_id = self.get_next_block_id()

        # Raise if there is no candidate.
        if next_block_id not in self.candidates:
            raise self.NodeValueError("There is no candidate available.")

        # Return if there is only one candidate.
        if len(self.candidates[next_block_id]) == 1:
            return self.candidates[next_block_id][0]

        # Return if there is a candidate that is the most fitting.
        candidate = sorted(  # We sort the list of candidates.
            self.candidates[next_block_id],     # For the given block.
            key=lambda x: (
                not x.forged,   # Forged goes first. Reversed for True to go first.
                not x.check_action(Candidate.ACTION_VOTE_STATUS_UPDATE),    # Vote status update goes second.
                not x.check_action(Candidate.ACTION_VOTE),  # Vote goes second.
                not x.check_action(Candidate.ACTION_APPROVE_STATUS_UPDATE),   # Approved status update goes third.
                not x.check_action(Candidate.ACTION_APPROVE),   # Approved goes forth.
            ),
        )[0]    # Return the first in the list.

        # This should not trigger. Just a sanity check.
        if candidate.forged:
            logging.error(
                "N{} is trying to retrieve it's current candidate "
                "and there is one that is already forged for B{}".format(
                    self.node_id,
                    candidate.block.block_id,
                )
            )

        return candidate

    def enough_approves(self, message_chain=None) -> bool:
        """
        Check if we have enough approves for the current block_candidate.
        :return: True - there is enough approves, False - not enough approves.
        """
        if message_chain is None:
            if len(self.active_candidate.messages_approve) > (len(self.nodes) - 1) / 2.0:
                return True
            return False
        else:
            if len(message_chain) > (len(self.nodes) - 1) / 2.0:
                return True
            return False

    def enough_votes(self, message_chain=None) -> bool:
        """
        Check if we have enough votes for the current block_candidate.
        :return: True - there is enough votes, False - not enough votes.
        """
        if message_chain is None:
            return len(self.active_candidate.messages_vote) > len(self.nodes) / 2.0
        else:
            return len(message_chain) > len(self.nodes) / 2.0

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

        # Check if the message has a block.
        if not message.block:
            logging.error(
                "N{} received a message from N{} and discarded because there is no block attached.".format(
                    self.node_id,
                    message.node_id,
                )
            )
            return False

        # Verify sent block in the message.
        if not self.verify_block(message.block):
            logging.error(
                "N{} received a message from N{} and discarded it because the block is invalid.".format(
                    self.node_id,
                    message.node_id
                )
            )
            return False

        # TODO: Add real signature message validation here.
        return True

    def verify_block(self, block: Block) -> bool:
        """
        Verifies if a block is real.
        :param block: Instance of a Block.
        :return: True - block is valid. False - block is invalid.
        """
        # TODO Add block hash verification here.
        if block.block_id % len(self.nodes) == block.node_id and block.block_id >= 0 or block.block_id is None:
            return True
        return False

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
            if not self.verify_block(block):
                return False

        # If everything passed before, we assume that the chain is valid.
        return True

    class NodeValueError(ValueError):
        """Custom value exception for Node class."""
        pass

    class NodeNotEnoughData(ValueError):
        """Exception that is raised when there is not enough information to do something."""
        pass
