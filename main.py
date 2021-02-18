from __future__ import annotations
from typing import Tuple, List, Union, Set
import math
import random
import logging
import numpy as np
import time
import copy

# Choose between logging.INFO, logging.ERROR, logging.DEBUG
LOGGING_LEVEL = logging.DEBUG

# Should nodes store extra profs and votes.
KEEP_EXCESSIVE_MESSAGES = False

# Maximum number of main loop cycles.
MAX_LOOP_ITERATIONS = 10**7

# Maximum X, Y distance.
MAX_DISTANCE = 10.0

# Number of nodes.
NODE_COUNT = 16

# Number of blocks to generate.
GENERATE_BLOCKS = 10

# Lost Message % [0, 100) on send
LOST_MESSAGES_PERCENTAGE = 40.0

# Distance between nodes gets multiplied by this factor and converted to seconds.
DELAY_MULTIPLIER = 0.000001


class Block:

    # Block ID.
    block_id: int

    # ID of the node that created the block.
    node_id: int

    # Time created.
    created: float

    def __init__(self, block_id: int, node_id: int):
        """Constructor"""
        self.block_id = block_id
        self.node_id = node_id
        self.created = time.time()

    def __eq__(self, other) -> bool:
        """Is another object is equal to self?"""
        if not isinstance(other, self.__class__):
            raise Exception("Cannot compare objects Block and {}".format(type(other)))
        return self.block_id == other.block_id and self.node_id == other.node_id

    def __ne__(self, other) -> bool:
        """Is another object is not equal to self?"""
        return not self.__eq__(other)

    def __str__(self) -> str:
        """Represent instance of the class as a string."""
        return "B{}byN{}".format(
            self.block_id,
            self.node_id,
        )


class Message:

    # Possible message types. e.g. Use Message.TYPE_APPROVE to send an approve.
    TYPE_COMMIT = "commit"
    TYPE_APPROVE = "approve"
    TYPE_VOTE = "vote"
    TYPE_APPROVE_STATUS_UPDATE = "approve_status_update"
    TYPE_VOTE_STATUS_UPDATE = "vote_status_update"

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
        """Constructor"""
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


class Transport:

    # Messages pool.
    # Always kept reverse sorted by "delivery_time" key in each element.
    # First that needs to be delivered is the last element.
    pool: List[MessageWrapper]

    # Nodes map
    nodes_map = List[dict]
    """
    [{
        node_id: {
            "x": 123,   # X coordinate on a plane.
            "y": 456,   # Y coordinate on a plane.
            "drop_rate": 5, # In percent [0, 100]
            "connection_speed": 0.7, # how fast are the messages transferred (0, 1]
        }
    },...]
    """

    def __init__(self, nodes_count: int, max_distance: int = MAX_DISTANCE):
        self.pool = []
        self.nodes_map = []
        for i in range(nodes_count):
            self.nodes_map.append(
                {
                    "x": random.uniform(0, max_distance),
                    "y": random.uniform(0, max_distance),
                    "drop_rate": LOST_MESSAGES_PERCENTAGE,
                    "connection_speed": random.uniform(np.nextafter(0, 1), np.nextafter(1, 2)),
                }
            )

    def _pool_set(self, message_wrapper: MessageWrapper):
        """Save a message into the pool."""
        self.pool.append(message_wrapper)
        self.pool = sorted(self.pool, key=lambda x: x.time_deliver, reverse=True)

    def get_distance(self, from_node_id: int, to_node_id: int) -> float:
        """Get distance between two nodes by block_id."""
        return math.sqrt(
            (self.nodes_map[from_node_id]['x'] - self.nodes_map[to_node_id]['x']) ** 2 +
            (self.nodes_map[from_node_id]['y'] - self.nodes_map[to_node_id]['y']) ** 2
        )

    def connection_delay(self, from_node_id: int, to_node_id: int) -> float:
        """Get connection delay between two nodes in seconds."""
        distance = self.get_distance(from_node_id, to_node_id)
        avg_connection_speed = (
                self.nodes_map[from_node_id]['connection_speed']
                + self.nodes_map[to_node_id]['connection_speed']
        ) / 2.0
        return distance * avg_connection_speed * DELAY_MULTIPLIER

    def send(self, message: Message, to_id: int) -> bool:
        """
        Add a message to the send pool.
        :param message: Message object.
        :param to_id: Node block_id to send to.
        :return: True if added, False if the connection was broken.
        """

        # Get sender node block_id.
        from_id = message.node_id

        # Randomly drop this message.
        if random.randint(0, 100) < (self.nodes_map[from_id]['drop_rate']+self.nodes_map[to_id]['drop_rate'])/2.0:
            # Log
            logging.debug("Message N{}->N{} {} was dropped due to the random drop rule.".format(
                from_id,
                to_id,
                message,
            ))
            return False

        # Calculate delivery times.
        time_now = time.time()
        time_deliver = time_now + self.connection_delay(from_id, to_id)

        # Prepare the message wrapper.
        message_wrapper = self.MessageWrapper(
            message=message,
            to_id=to_id,
            time_send=time_now,
            time_deliver=time_deliver,
        )

        # Save to the pool.
        self._pool_set(message_wrapper)

        # Log.
        logging.debug("Send {}".format(message_wrapper))

        return True

    def receive(self) -> Union[List[Tuple[Message, int]]]:
        """
        Receive messages that are due.
        :return:    List of tuples with (Message, to_node_id) or an empty list if there are no messages left.
                    Ordered by first message is first to be delivered.
        """

        messages_to_deliver = []
        time_now = time.time()
        try:
            while self.pool[-1].time_deliver <= time_now:
                # Pull the message.
                message_wrapper = self.pool.pop()
                # Log.
                logging.debug("Receive {}".format(message_wrapper))
                # Save to the output.
                messages_to_deliver.append((message_wrapper.message, message_wrapper.to_id))
        except IndexError:
            pass

        return messages_to_deliver

    class MessageWrapper:

        message: Message
        time_deliver: float
        time_send: float
        to_id: int
        from_id: int    # Shadow parameter that is proxied from the message body.

        def __init__(
            self,
            message: Message,
            to_id: int,
            time_deliver: Union[float, None] = None,
            time_send: Union[float, None] = None,
        ):
            self.time_deliver = time_deliver if time_deliver else time.time()
            self.time_send = time_send if time_send else time.time()
            self.to_id = to_id
            self.message = copy.deepcopy(message)

        def __getattr__(self, item):
            """Proxies from_id from the message body."""
            if item == 'from_id':
                return self.message.node_id
            else:
                return self.__getattribute__(item)

        def __str__(self):
            """String representation of self."""
            return "Message N{from_id}->N{to_id}: {message}".format(
                from_id=self.from_id,
                to_id=self.to_id,
                message=self.message,
            )


class Candidate:

    # Possible actions that a node could take
    ACTION_APPROVE = "approve"
    ACTION_APPROVE_STATUS_UPDATE = "approve_status_update"
    ACTION_VOTE = "vote"
    ACTION_VOTE_STATUS_UPDATE = "vote_status_update"

    POSSIBLE_ACTIONS = (
        ACTION_APPROVE,
        ACTION_APPROVE_STATUS_UPDATE,
        ACTION_VOTE,
        ACTION_VOTE_STATUS_UPDATE,
    )

    # Shadow argument. Points to the self.block.block_id
    block_id: int

    # Candidate block.
    block: Block

    # Incoming approve messages.
    messages_approve: dict

    # Incoming vote messages.
    messages_vote: dict

    # Set of node_ids that we received vote status updates from.
    vote_status_updates: Set[int]

    # Running list of unique taken actions by this node.
    actions_taken: Set[str]

    # Defines if the block was forged or not.
    forged: bool

    def __init__(self, block: Block):
        self.block = block
        self.messages_approve = {}
        self.messages_vote = {}
        # {
        #   node_id: [list of node_ids that approved this vote]
        # }
        self.vote_status_updates = set()
        self.actions_taken = set()
        self.forged = False

    def take_action(self, action: str):
        """
        Check if the action was already taken on this candidate.
        :param action: One of the actions from Candidate.POSSIBLE_ACTIONS
        """
        if action not in self.POSSIBLE_ACTIONS:
            raise Exception("Trying to take an action ({}) that is not allowed. Possible actions: {}".format(
                action,
                self.POSSIBLE_ACTIONS,
            ))
        self.actions_taken.add(action)

    def check_action(self, action: str) -> bool:
        """
        Check if the action was already taken on this candidate.
        :param action: One of the actions from Candidate.POSSIBLE_ACTIONS
        :return: True if action has already been taken, False if the action has not yet been taken.
        """
        if action not in self.POSSIBLE_ACTIONS:
            raise Exception("Trying to check an action ({}) that is not allowed. Possible actions: {}".format(
                action,
                self.POSSIBLE_ACTIONS,
            ))
        return action in self.actions_taken

    def __getattr__(self, item):
        """Proxies block_id from the block.block_id."""
        if item == 'block_id':
            return self.block.block_id
        else:
            return self.__getattribute__(item)


class Node:

    # ID of the node.
    node_id: int

    # Pointer to the list of all nodes.
    nodes: List[Node]

    # Current chain.
    chain: List[Block]

    # Dict of candidate blocks and their proofs.
    candidates: dict
    """
        {
            block_id: Candidate,
            ...
        }
    """

    # Pointer to the current active candidate.
    active_candidate: Union[Candidate, None]

    # Link to a global Transport object
    transport: Transport

    def __init__(self, nodes: List[Node], node_id: int, transport: Transport, chain: Union[List, None] = None):
        """Constructor"""
        # Assignment
        self.node_id = node_id
        self.chain = chain if chain else []
        self.candidates = {}
        self.active_candidate = None
        self.transport = transport
        nodes.append(self)
        self.nodes = nodes

    def get_next_block_id(self) -> int:
        return len(self.chain) + 1

    def get_next_block_master_id(self) -> int:
        # Get next block master node ID
        # This assumes that nodes never change after initiating the group.
        return self.get_next_block_id() % len(self.nodes)

    def verify_block(self, block: Block) -> bool:
        # Verifies if a block is real.
        if block.block_id % len(self.nodes) == block.node_id:
            return True
        return False

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

    def enough_vote_status_updates(self) -> bool:
        """
        Check if we have enough vote status updates in the current candidate to try forging a new block.
        :return: True - enough votes, False, not enough votes.
        """
        return len(self.active_candidate.vote_status_updates) > len(self.nodes) / 2.0

    def gen_commit(self) -> bool:
        if self.node_id != self.get_next_block_master_id():
            # logging.error("Node #{} tried to generate a commit, while it should have been generated by {}".format(
            #     self.node_id,
            #     self.get_next_block_master_id(),
            # ))
            return False

        # Gen new block.
        block = Block(
            block_id=self.get_next_block_id(),
            node_id=self.node_id,
        )

        self.candidates[block.block_id] = Candidate(block)
        self.active_candidate = self.candidates[block.block_id]

        # Gen broadcast message.
        message = Message(
            node_id=self.node_id,
            message_type=Message.TYPE_COMMIT,
            block=block,
        )

        self.broadcast(message)
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
        logging.info("N{} B{} is forged.".format(self.node_id, self.active_candidate.block_id))

        # Forge the block and add it to the chain.
        self.chain.append(self.active_candidate.block)
        self.active_candidate.forged = True
        self.active_candidate = None

        # Moved to self.run() method.
        # # Try generating the next block if we are the appropriate node.
        # self.gen_commit()

        return True

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

    def send_approve_once(self):
        """Send approval message to everyone once."""
        if Candidate.ACTION_APPROVE in self.active_candidate.actions_taken:
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

    def send_vote_once(self):
        """Send vote message to everyone once."""
        if self.active_candidate.check_action(Candidate.ACTION_VOTE):
            return False

        # Check if we have enough approve votes, we send a status update.
        if not self.enough_approves():
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

    def send_approve_status_update_once(self) -> bool:
        """
        Send approve status update once if we have enough approves.
        :return: True - update was sent. False - update was not sent.
        """
        if self.active_candidate.check_action(Candidate.ACTION_APPROVE_STATUS_UPDATE):
            return False

        if not self.enough_approves():
            return False

        self.active_candidate.take_action(Candidate.ACTION_APPROVE_STATUS_UPDATE)

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

    def send_vote_status_update_once(self) -> bool:
        """
        Send vote status update once if we have enough votes.
        :return: True - update was sent. False - update was not sent.
        """

        if self.active_candidate.check_action(Candidate.ACTION_VOTE_STATUS_UPDATE):
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

    def receive_commit(self, message_in: Message) -> bool:
        """Receive a commit message."""
        # This logic is already handled by block validation in self.receive()
        return True

    def receive_approve(self, message_in: Message) -> bool:
        """Receive an approve message."""

        # If we already voted, we do not need extra approves.
        if not KEEP_EXCESSIVE_MESSAGES and self.active_candidate.check_action(Candidate.ACTION_VOTE):
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

        self.active_candidate.messages_approve[message_in.node_id] = message_in

        # Send approve status update if we need to.
        self.send_approve_status_update_once()

        # We have enough approves, now we also send out our vote.
        self.send_vote_once()

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
        if not self.active_candidate.check_action(Candidate.ACTION_VOTE) or KEEP_EXCESSIVE_MESSAGES:
            self.active_candidate.messages_approve.update(message_in.messages_chain)

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

    def receive(self, message_in: Message) -> bool:
        """Receive a message from another node."""

        # Check if the message has a block.
        if not message_in.block:
            logging.error(
                "N{} received a message from N{} and discarded because there is no block attached.".format(
                    self.node_id,
                    message_in.node_id,
                )
            )
            return False

        # First we verify sent block in the message. If it's bad, we disregard the message.
        if not self.verify_block(message_in.block):
            logging.error(
                "N{} received a message from N{} and discarded it because the block is invalid.".format(
                    self.node_id,
                    message_in.node_id
                )
            )
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
            self.candidates[message_in.block.block_id] = Candidate(block=message_in.block)
            # Set current active candidate.
            self.active_candidate = self.candidates[message_in.block.block_id]
            # Since we just got a new block and verified it to be good, we broadcast an approval for it.
            self.send_approve_once()
        else:
            # Set current active candidate.
            self.active_candidate = self.candidates[message_in.block.block_id]

        # COMMIT
        if message_in.message_type == Message.TYPE_COMMIT:
            # Commit message is fully handled by the block validation. There is no need to do anything else really.
            return self.receive_commit(message_in)

        # APPROVE
        # TODO
        elif message_in.message_type == Message.TYPE_APPROVE:
            return self.receive_approve(message_in)

        # APPROVE_STATUS_UPDATE
        # TODO
        elif message_in.message_type == Message.TYPE_APPROVE_STATUS_UPDATE:
            return self.receive_approve_status_update(message_in)

        # VOTE
        # TODO
        elif message_in.message_type == Message.TYPE_VOTE:
            return self.receive_vote(message_in)

        # VOTE_STATUS_UPDATE
        # TODO
        elif message_in.message_type == Message.TYPE_VOTE_STATUS_UPDATE:
            return self.receive_vote_status_update(message_in)

    def run(self, message: Union[Message, None] = None):
        """
        Main loop method.
        :param message: Message object to be received by this node.
        :return:
        """

        # Process the message if we got one.
        if message:
            self.receive(message_in=message)

        # Try generating the block if we are the appropriate node for it.
        if not self.active_candidate:
            self.gen_commit()


def main():
    # Setup logging.
    logging.basicConfig(format='%(levelname)s: %(message)s', level=LOGGING_LEVEL)

    # Start a transport.
    transport = Transport(nodes_count=NODE_COUNT)

    # Generate nodes.
    nodes: List[Node] = []
    for i in range(NODE_COUNT):
        Node(
            nodes=nodes,
            node_id=i,
            transport=transport,
            chain=[],
        )
    logging.info("Nodes generated.")

    # Main loop.
    nodes_with_required_number_of_blocks = 0
    cycles = 0
    while True:
        cycles += 1

        # IDs of all the nodes that need to be ran.
        nodes_to_run = {i for i in range(NODE_COUNT)}

        # Get possible message.
        messages_to_deliver = transport.receive()

        # Deliver the messages.
        for message, to_node_id in messages_to_deliver:

            # Run the node and deliver it's message.
            nodes[to_node_id].run(message)

            # Remove ID of the nodes that have been ran.
            try:
                nodes_to_run.remove(to_node_id)
            except KeyError:
                pass

        # Run the rest of the nodes that did not get a message.
        for i in nodes_to_run:
            nodes[i].run()

        # Exit the main loop if GENERATE_BLOCKS was forged on the majority of the nodes.
        nodes_with_required_number_of_blocks = 0
        for i in range(NODE_COUNT):
            if len(nodes[i].chain) >= GENERATE_BLOCKS:
                nodes_with_required_number_of_blocks += 1
        if nodes_with_required_number_of_blocks > NODE_COUNT/2.0:
            logging.info("--- All the blocks we requested were forged, stopping gracefully. ---")
            break

        if cycles > MAX_LOOP_ITERATIONS:
            logging.error("--- Premature termination. We ran out of allowed cycles by MAX_LOOP_ITERATIONS. ---")
            break

    # Gather stats on generated blocks.
    blocks_generated = {}
    for i in range(NODE_COUNT):
        for block in nodes[i].chain:
            try:
                blocks_generated[block.block_id].append(i)
            except KeyError:
                blocks_generated[block.block_id] = [i]

    # Log the generated blocks stats.
    for block_id, nodes_confirmed in blocks_generated.items():
        logging.info("B{} confirmed by {}/{} nodes.".format(block_id, len(nodes_confirmed), NODE_COUNT))
    if not blocks_generated:
        logging.error("No blocks were generated.")

    logging.info("{} cycles were executed.".format(cycles))
    logging.info("THE END")


if __name__ == "__main__":
    main()
