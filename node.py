from __future__ import annotations
import logging
import time
from collections import defaultdict
from typing import List, Union
from block import Block
from candidate import Candidate, CandidateManager
from message import Message
from transport import Transport
from colored import fg, bg, attr


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
    time_update_requested: float
    blank_block_timeout: float
    chain_update_timeout: float

    def __init__(
        self,
        nodes: List[Node],
        node_id: int,
        transport: Transport,
        chain: Union[List, None] = None,
        keep_excessive_messages: bool = False,
        blank_block_timeout: float = 2.0,
        chain_update_timeout: float = 5.0,
    ):
        """
        Constructor
        :param nodes: Pointer to a list of already instantiated nodes. This node will add itself there.
        :param node_id: ID of this node.
        :param transport: Pointer to an instance of the Transport that handles message transfer between nodes.
        :param chain: Chain of forged blocks.
        :param keep_excessive_messages: Keep saving update messages inside self.candidates after a state has been proven.
        :param blank_block_timeout: Timeout before a blank block would be nominated as a next block.
        :param chain_update_timeout: Timeout before the node requests a chain update from other nodes.
        """

        # Validate and set the chain.
        if not self.validate_chain(chain):
            raise ValueError("Passed chain is invalid. Cannot instantiate a new Node object with this chain.")
        self.chain = chain if chain else []

        self.node_id = node_id
        self.candidates = defaultdict(CandidateManager)
        self.active_candidate = None
        self.transport = transport

        # Set nodes pull.
        nodes.append(self)
        self.nodes = nodes

        # Messages related settings.
        self.keep_excessive_messages = keep_excessive_messages
        self.blank_block_timeout = blank_block_timeout
        self.chain_update_timeout = chain_update_timeout
        self.messages_buffer = []

        # Start all timers at node's declaration.
        # We assume that there are either no blocks in the chain or the last one was forged and approved right now.
        self.time_forged = time.time()
        self.time_approved = time.time()
        self.time_update_requested = time.time()

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
        except self.NodeValueError:
            # No candidates.

            # Try generating the block if we are the appropriate node for it.
            self.gen_commit()

            # Try voting for a blank block if there are no candidates.
            self.try_approving_blank_block()

        # Try requesting chain update.
        self.try_requesting_chain_update()

    def receive(self, message_in: Message) -> bool:
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
                return False
        except self.NodeNotEnoughData:
            # If there is not enough data to validate the message we save it for later.
            self.delay_message(message_in)
            return False

        # Check if the block has already been forged.
        if message_in.blocks[0] in self.chain:
            # TODO: We should probably send the proof message to the requesting node, so it can forge the block as well.
            logging.debug(
                "N{} received a message from N{} and discarded it because this block is already forged.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return False

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

                    return False
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
                self.request_chain_update(node_ids=[message_in.node_id])
                return False
            elif message_in.blocks[0].block_id < len(self.chain):
                # Check if the block has already been forged. Then send a chain update to that node.
                self.send_chain_update(message_in.node_id, message_in.blocks[0].block_id-1)
                return False
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
            if self.add_candidate(Candidate(block=message_in.blocks[0])):
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
            blocks=[last_block],   # Signifies the last block we have.
        )

        # Send or broadcast the message.
        if not node_ids:
            self.broadcast(message)
        else:
            for node_id in node_ids:
                self.send_message(message, node_id)

        # Update the timer.
        self.time_update_requested = time.time()

    def receive_commit(self, message_in: Message) -> bool:
        """Receive a commit message."""
        # This logic is already handled by block validation in self.receive()
        return True

    def receive_approve(self, message_in: Message) -> bool:
        """Receive an approve message."""

        # If we already sent approve status update, we do not need extra approves.
        if not self.keep_excessive_messages and self.candidates[self.active_candidate.block.block_id].check_action(
            Candidate.ACTION_APPROVE_STATUS_UPDATE
        ):
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
            if not self.verify_block(message_in_chain.blocks[0]):
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

        if self.active_candidate and self.active_candidate.block != message_in.blocks[0]:
            # This means that my block is different from the one that is being approved.
            # TODO: We need to find the difference and update our chain up to this block.
            logging.error(
                "N{} received an approve status update from N{} with a block ({}) "
                "that differs from my candidate ({}).".format(
                    self.node_id,
                    message_in.node_id,
                    message_in.blocks[0],
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

    def receive_chain_update_request(self, message_in: Message) -> bool:
        """Receive a chain update request."""

        # Blocks that need to be included into the message.
        # Note that indexes in this list are not block IDs!
        blocks = []
        if not message_in.blocks:
            # The whole chain should be sent.
            blocks = self.chain
        elif message_in.blocks[0].block_id < len(self.chain)-1:
            # Pick a diff between received block and the last block we have.
            for i in range(message_in.blocks[0].block_id+1, len(self.chain)):
                blocks.append(self.chain[i])

        # Message
        next_block_id = self.get_next_block_id()
        message_out = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_CHAIN_UPDATE,
            blocks=blocks,
            candidates={
                next_block_id: self.candidates[next_block_id]
            } if next_block_id in self.candidates else None,
        )

        # Send
        self.send_message(message_out, message_in.node_id)
        return True

    def send_chain_update(self, node_id, starting_block_id: int = 0):
        """Send chain update to a specific node starting from a starting_block_id."""

        # Blocks that need to be included into the message.
        # Note that indexes in this list are not block IDs!
        blocks = []
        if starting_block_id < len(self.chain)-1:
            # Pick a diff between received block and the last block we have.
            for i in range(starting_block_id, len(self.chain)):
                blocks.append(self.chain[i])

        # Message
        next_block_id = self.get_next_block_id()
        message_out = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_CHAIN_UPDATE,
            blocks=blocks,
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
        if message_in.blocks:

            # Create searchable index.
            block_index = {}
            for block in message_in.blocks:
                block_index[block.block_id] = block

            # Iterate and update self.chain for only the blocks we need.
            next_block_id = self.get_next_block_id()
            while next_block_id in block_index:
                if self.verify_block(block_index[next_block_id]):
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
            blocks=[block],
        )

        self.broadcast(message)
        return True

    def get_last_block(self) -> Block:
        """
        Get the last forged block in self.chain.
        :return: Block instance of the last block.
        :raises: Node.NodeValueError if self.chain is empty.
        """
        if not self.chain:
            raise self.NodeValueError("Chain is empty. There is no last blocks.")

        return self.chain[-1]

    def get_next_block_id(self) -> int:
        """
        Get the next block ID that has to be generated.
        :return: Block ID
        """
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
            self.send_message(message, i)

    def send_message(self, message: Message, node_id: int):
        """ Send a message to a specific node. """
        self.transport.send(message, to_id=node_id)

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

    def try_approving_blank_block(self) -> bool:
        """
        Try to nominate a blank block as the next block in the chain if no candidate was received.
        :return: True - Blank block ws nominated. False - there is no need to nominate blank block.
        """

        # We assume that the active candidate is the next valid block.
        # It is only blank when there are no valid candidates.
        if self.active_candidate:
            return False

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

    def try_requesting_chain_update(self):
        """Try to request a chain update from other nodes if there is enough of standby time."""

        # Check if it is time to request an update.
        if self.last_activity_time() + self.chain_update_timeout > time.time():
            return

        self.request_chain_update()

    def send_approve_once(self):
        """Send approval message to everyone once."""

        # Check if we sent an approve for any candidate.
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_APPROVE):
            return False

        # Save the action we are taking.
        self.active_candidate.actions_taken.add(Candidate.ACTION_APPROVE)

        # Prepare the message.
        message_out = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_APPROVE,
            blocks=[self.active_candidate.block],
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
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_APPROVE_STATUS_UPDATE):
            return False

        if not self.enough_approves():
            return False

        # Set a flag that we have sent this update out.
        self.active_candidate.take_action(Candidate.ACTION_APPROVE_STATUS_UPDATE)

        # Increment the vote_status update counter with our own info.
        self.active_candidate.approve_status_updates.add(self.node_id)

        message_out = Message(
            node_id=self.node_id,
            blocks=[self.active_candidate.block],
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
            blocks=[self.active_candidate.block],
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
        if self.candidates[self.active_candidate.block.block_id].check_action(Candidate.ACTION_VOTE_STATUS_UPDATE):
            return False

        if not self.enough_votes():
            return False

        # Prepare the message.
        message_out = Message(
            node_id=self.node_id,
            blocks=[self.active_candidate.block],
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
        if self.candidates[candidate.block.block_id].find(candidate.block) is not None:
            self.set_active_candidate(block=candidate.block)
            return False

        self.candidates[candidate.block.block_id].append(candidate)

        # Set new active_candidate after adding a new one in the mix.
        self.set_active_candidate(block=candidate.block)

        return True

    def set_active_candidate(self, block: Union[Block, None] = None):
        """
        Sets self.active_candidate to the best current candidate or a passed block.
        :param block: Instance of a Block or None
        :raises: NodeValueError if the candidate was not set.
        :return:
        """
        if block is None:
            # If the block was not passed we set active candidate automatically.
            self.active_candidate = self.get_candidate()
            return

        # Check if this block is in candidates.
        if block.block_id not in self.candidates:   # or len(self.candidates[block.block_id]) == 0: <- Would prevent the last raise.
            raise self.NodeValueError("Passed block is not found in node's self.candidates dictionary.")

        # Check if this block is next in line.
        if self.get_next_block_id() != block.block_id:
            raise self.NodeValueError(
                "Passed block is not the next block to be processed.\n"
                "Expected block #{}, got #{}".format(
                    self.get_next_block_id(),
                    block.block_id,
                )
            )

        # Check if the same block is in candidates.
        for i in range(len(self.candidates[block.block_id])):
            if self.candidates[block.block_id][i].block == block:
                # Matching candidate is found, set the candidate and exit.
                self.active_candidate = self.candidates[block.block_id][i]
                return

        # This means that the previous loop did not find a matching block.
        raise self.NodeValueError("Passed block is not found in node's self.candidates dictionary.")

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

        # Return if there is a candidate that is the most fitting.
        try:
            return self.candidates[next_block_id].best_candidate()
        except ValueError:
            self.NodeValueError("There is no candidate available.")

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
        if not message.blocks:
            logging.error(
                "N{} received a message from N{} and discarded because there is no block attached.".format(
                    self.node_id,
                    message.node_id,
                )
            )
            return False

        # Verify sent blocks in the message.
        for block in message.blocks:
            if not self.verify_block(block):
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
        if block.node_id is None or (block.block_id % len(self.nodes) == block.node_id and block.block_id >= 0):
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

    def last_activity_time(self):
        """Get the time of last activity in the node."""
        activity_times = [
            self.time_forged,
            self.time_approved,
            self.time_update_requested,
        ]
        active_candidate = self.active_candidate
        if active_candidate:
            activity_times.append(active_candidate.block.created)

        # Return the lates of all times.
        return sorted(activity_times, reverse=True)[0]

    chain_annotation_str = "N7 <- Node number 7.\n" \
        "B: <- This is followed by node's blocks.\n" \
        "{bg_forged}{fg}[3]{reset} <- Forged block number 3\n" \
        "{bg_blank}{fg}[4]{reset} <- forged blank block number 4" \
        "".format(
            bg_forged=bg('green'),
            bg_blank=bg('grey_30'),
            fg=fg('white'),
            reset=attr('reset'),
        )

    def chain_str(self):
        """Get printable string representing node's current chain."""
        r = attr('reset')
        for block in self.chain:
            r += "{bg}{fg}[{block_id}]".format(
                bg=bg('green') if block.node_id is not None else bg('grey_30'),
                fg=fg('white'),
                block_id=block.block_id,
            )
        r += attr('reset')
        return r

    def candidates_str(self, starting_id: int = 0):
        """
        Get printable string representing node's current candidates.
        :param starting_id: Output should start from candidate #. Default=0.
        :return: Printable string representing current chain's candidates.
        """
        if starting_id not in self.candidates:
            return ""

        r = ""
        for i in range(starting_id, max(self.candidates.keys())+1):
            r += str(self.candidates[i])     # Getting current candidates
        return r

    def __str__(self):
        """Returns current node stage in a text format."""

        return 'N{node_id:02d} B:{chain}{next_candidates}'.format(
            node_id=self.node_id,
            chain=self.chain_str(),
            next_candidates=self.candidates_str(self.get_next_block_id()),
        )

    class NodeValueError(ValueError):
        """Custom value exception for Node class."""
        pass

    class NodeNotEnoughData(ValueError):
        """Exception that is raised when there is not enough information to do something."""
        pass
