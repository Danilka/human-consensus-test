import math
import random
import logging
import asyncio
import numpy as np
import time
import copy

# Maximum X, Y distance.
MAX_DISTANCE = 1000.0

# Number of nodes.
NODE_COUNT = 3

# Lost Message % [0, 100) on send
LOST_MESSAGES_PERCENTAGE = 0.0

# Distance between nodes gets multiplied by this factor and converted to seconds.
DELAY_MULTIPLIER = 0.0001


class Block:
    id = None
    node_id = None

    def __init__(self, id, node_id):
        """Constructor"""
        self.id = id
        self.node_id = node_id

    def __eq__(self, other):
        """Is another object is equal to self?"""
        if not isinstance(other, self.__class__):
            raise Exception("Cannot compare objects Block and {}".format(type(other)))
        return self.id == other.id and self.node_id == other.node_id

    def __ne__(self, other):
        """Is another object is not equal to self?"""
        return not self.__eq__(other)

    def __str__(self):
        """Represent instance of the class as a string."""
        return "B{}byN{}".format(
            self.id,
            self.node_id,
        )


class Message:
    # Possible message types. Use Message.TYPE_APPROVE to send an approve.
    TYPE_COMMIT = "commit"
    TYPE_APPROVE = "approve"
    TYPE_VOTE = "vote"
    TYPE_APPROVE_STATUS_UPDATE = "approve_status_update"
    TYPE_VOTE_STATUS_UPDATE = "vote_status_update"

    node_id = None
    message_type = None
    block = None
    messages_chain = None

    def __init__(self, node_id, message_type, block=None, messages_chain=None):
        """Constructor"""
        self.node_id = node_id
        self.message_type = message_type
        self.block = block
        self.messages_chain = messages_chain

    def __str__(self):
        """Represent instance of the class as a string."""
        return "[{message_type}] B{block_id} messages_chain size = {messages_chain_size}".format(
            message_type=self.message_type,
            block_id=self.block.id if self.block else "-",
            messages_chain_size=len(self.messages_chain) if self.messages_chain else 0,
        )


class Transport:

    # Messages pool.
    pool = None

    # Nodes map
    nodes_map = None
    """
    {
        node_id: {
            "x": 123,   # X coordinate on a plane.
            "y": 456,   # Y coordinate on a plane.
            "drop_rate": 5, # In percent [0, 100]
            "connection_speed": 0.7, # how fast are the messages transferred (0, 1]
        }
    }
    """

    def __init__(self, nodes_count, max_distance=MAX_DISTANCE):
        self.pool = []
        for i in range(nodes_count):
            self.nodes_map[i] = {
                "x": random.uniform(0, MAX_DISTANCE),
                "y": random.uniform(0, MAX_DISTANCE),
                "drop_rate": LOST_MESSAGES_PERCENTAGE,
                "connection_speed": random.uniform(np.nextafter(0, 1), np.nextafter(1, 2)),
            }

    def get_distance(self, from_node_id, to_node_id):
        # Get distance between two nodes by id.
        return math.sqrt(
            (self.nodes_map[from_node_id]['x'] - self.nodes_map[to_node_id]['x']) ** 2 +
            (self.nodes_map[from_node_id]['y'] - self.nodes_map[to_node_id]['y']) ** 2
        )

    def connection_delay(self, from_node_id, to_node_id):
        """Get connection delay between two nodes in seconds."""
        distance = self.get_distance(from_node_id, to_node_id)
        avg_connection_speed = (
                self.nodes_map[from_node_id]['connection_speed']
                + self.nodes_map[to_node_id]['connection_speed']
        ) / 2.0
        return distance * avg_connection_speed * DELAY_MULTIPLIER

    def send(self, message: Message, from_id: int, to_id: int) -> bool:

        # Randomly drop this message.
        if random.randint(0, 100) <= (self.nodes_map[from_id]['drop_rate']+self.nodes_map[to_id]['drop_rate'])/2.0:
            logging.info("Message N{}->N{} {} was dropped due to the random drop rule.".format(
                from_id,
                to_id,
                message,
            ))
            return False

        # Calculate delivery times.
        time_now = time.time()
        time_deliver = time_now + self.connection_delay(from_id, to_id)

        self.pool.append(
            self.MessageWrapper(
                message=message,
                from_id=from_id,
                to_id=to_id,
                time_send=time_now,
                time_deliver=time_deliver,
            )
        )

    class MessageWrapper:

        message = None
        time_deliver = None
        time_send = None

        def __init__(self, message, from_id, to_id, time_deliver=None, time_send=None):
            self.time_deliver = time_deliver if time_deliver else time.time()
            self.time_send = time_send if time_send else time.time()
            self.from_id = from_id
            self.to_id = to_id
            self.message = copy.deepcopy(message)


class Node:
    # Possible actions that a node could take
    ACTION_APPROVE = "approve"
    ACTION_VOTE = "vote"

    # Running list of taken actions by this node.
    actions_taken = None

    # ID of the node.
    id = None

    # Coordinates of the node.
    x, y = None, None

    # Pointer to the list of all nodes.
    nodes = None

    # received messages log - not used yet
    messages_in = []

    # sent messages log - not used yet
    messages_out = []

    # Speed of connection between nodes (0,1]
    connection_speed = None

    # Current chain.
    chain = None

    # Current next block candidate.
    block_candidate = None

    # Incoming approve messages.
    messages_approve = None

    # Incoming vote messages.
    messages_vote = None

    def __init__(self, nodes, id, x=0.0, y=0.0, connection_speed=0.5, chain=None):
        """Constructor"""

        # Validation
        if not 0 < connection_speed <= 1:
            raise Exception("connection_speed must be between 0 and 1")
        if not isinstance(chain, list):
            raise Exception("chain must be a list, not {}".format(type(chain)))

        # Assignment
        self.id = id
        self.x, self.y = x * 1.0, y * 1.0
        self.connection_speed = connection_speed
        self.chain = chain
        self.messages_approve = {}
        self.messages_vote = {}
        self.actions_taken = []
        self.block_candidate = None
        # self.messages_approve_status_update = {}
        nodes.append(self)
        self.nodes = nodes

    def get_distance_to(self, node_id):
        # Get distance between self and passed node_id.
        return math.sqrt((self.x - self.nodes[node_id].x) ** 2 + (self.y - self.nodes[node_id].y) ** 2)

    async def connection_delay(self, node_id):
        """Delay the program by distance * connection_speed in miliseconds."""
        distance = self.get_distance_to(node_id)
        await asyncio.sleep(distance * self.connection_speed * DELAY_MULTIPLIER)

    def get_next_block_id(self):
        return len(self.chain) + 1

    def get_next_block_master_id(self):
        # Get next block master node ID
        # This assumes that nodes never change after initiating the group.
        return self.get_next_block_id() % len(self.nodes)

    def verify_block(self, block: Block) -> bool:
        # Verifies if a block is real.
        if block.id % len(self.nodes) == block.node_id:
            return True
        return False

    async def gen_commit(self):
        if self.id != self.get_next_block_master_id():
            # logging.error("Node #{} tried to generate a commit, while it should have been generated by {}".format(
            #     self.id,
            #     self.get_next_block_master_id(self),
            # ))
            return None

        # Gen new block.
        block = Block(
            id=self.get_next_block_id(),
            node_id=self.id,
        )

        self.block_candidate = block

        # Gen broadcast message.
        message = Message(
            node_id=self.id,
            message_type=Message.TYPE_COMMIT,
            block=block,
        )

        await self.broadcast(message)

    def forge_candidate_block(self):
        """ Moves candidate block onto the main chain and cleans supporting vars. """
        if not self.block_candidate:
            raise Exception("Trying to forge a block candidate while it doesn't exist. Node #{}".format(self.id))

        logging.info("N{} B{} is forged.".format(self.id, self.block_candidate.id))

        self.chain.append(self.block_candidate)
        self.block_candidate = None
        self.messages_approve = {}
        self.messages_vote = {}
        self.actions_taken = []

    async def try_forging_candidate_block(self):
        if not self.block_candidate:
            logging.info("N{} Unsuccessful forge attempt, there is no candidate block.".format(self.id))
            return False

        if len(self.messages_vote) <= len(self.nodes) / 2.0:
            logging.info(
                "N{} B{} Unsuccessful forge attempt, not enough votes.".format(self.id, self.block_candidate.id))
            return False

        # This means that we have enough votes for Approve Status Update, so we broadcast everything:
        await self.send_vote_once()

        # Forge the block and add it to the chain.
        self.forge_candidate_block()

        return True

    async def send_message(self, node_id, message):
        """Send a message to a specific node."""

        logging.info("Message send N{}->N{}: {}".format(
            self.id,
            node_id,
            message,
        ))

        # Randomly drop connection.
        if random.randint(0, 100) < LOST_MESSAGES_PERCENTAGE:
            return
        # Introduce the delay.
        await self.connection_delay(node_id)
        await self.nodes[node_id].receive(copy.deepcopy(message))

    async def broadcast(self, message: Message, exclude_node_ids=None):
        """ Send message to everyone. """
        if isinstance(exclude_node_ids, list):
            exclude_node_ids.append(self.id)
        else:
            exclude_node_ids = [self.id]

        for i in range(len(self.nodes)):
            # Skipp sending messages to self.
            if i in exclude_node_ids:
                continue
            await self.send_message(node_id=i, message=message)

    async def send_approve_once(self):
        """Send approval message to everyone once."""
        if self.ACTION_APPROVE in self.actions_taken:
            return False

        # Save the action we are taking.
        self.actions_taken.append(self.ACTION_APPROVE)

        message_out = Message(
            node_id=self.id,
            message_type=Message.TYPE_APPROVE,
            block=self.block_candidate,
        )
        # Save approve message into our own log as well.
        self.messages_approve[self.id] = message_out
        await self.broadcast(message_out)
        return True

    async def send_vote_once(self):
        """Send vote message to everyone once."""
        if self.ACTION_VOTE in self.actions_taken:
            return False

        # Save the action we are taking.
        self.actions_taken.append(self.ACTION_VOTE)

        message_out = Message(
            node_id=self.id,
            block=self.block_candidate,
            message_type=Message.TYPE_VOTE,
            # TODO: This should have a separate diff for each node with only messagess that they need to reach approval.
            messages_chain={**self.messages_vote, **{self.id: self.messages_approve}}
        )
        await self.broadcast(message_out)
        return True

    async def receive_commit(self, message_in: Message) -> bool:
        """Receive a commit message."""
        if self.block_candidate:
            # TODO: I already have a candidate, this is phishy.
            logging.info(
                "N{} received a commit from N{} and discarted it because there is alredy a candidate.".format(
                    self.id,
                    message_in.node_id,
                ))
            return False
        else:
            # Save the block and send approve to everyone.
            self.block_candidate = message_in.block

            logging.info("N{} received a commit from N{} and saved it.".format(self.id, message_in.node_id))

            await self.send_approve_once()

            return True

    async def receive_approve(self, message_in: Message) -> bool:
        """Receive an approve message."""
        # Save new block as a candidate if we do not already have it.
        # This is the case when we got an approve before a commit.
        if not self.block_candidate:
            self.block_candidate = message_in.block
            await self.send_approve_once()

        if message_in.node_id in self.messages_approve:
            # We already have this message, so we disregard it.
            logging.info("N{} received an approve from N{}, but already had it.".format(self.id, message_in.node_id))
            return True

        self.messages_approve[message_in.node_id] = message_in
        logging.info("N{} received an approve from N{} and saved it.".format(self.id, message_in.node_id))

        # Check if we have enough approve votes, we send a status update.
        if len(self.messages_approve) > (len(self.nodes) - 1) / 2.0:
            print("[{}] Enough votes".format(self.id))
            message_out = Message(
                node_id=self.id,
                block=self.block_candidate,
                message_type=Message.TYPE_APPROVE_STATUS_UPDATE,
                # TODO: This should have a separate diff for each node with only messagess that they need to reach approval.
                messages_chain=self.messages_approve,
            )
            await self.broadcast(message_out, exclude_node_ids=[message_in.node_id])

        return True

    async def receive_approve_status_update(self, message_in: Message) -> bool:
        """Receive an approve status update message."""
        # Verify message chain.
        for message_in_chain in message_in.messages_chain:
            if not self.verify_block(message_in.block):
                # Got a message with a wrong block.
                logging.error("N{} received an approve status update from N{} with a wrong block".format(
                    self.id,
                    message_in.node_id,
                ))
                return False
        if len(message_in.messages_chain) <= (len(self.nodes) - 1) / 2.0:
            # This means that there is not enough votes for approval.
            logging.error("N{} received an approve status update from N{} with not enough votes in it".format(self.id,
                                                                                                              message_in.node_id))
            return False

        if self.block_candidate and self.block_candidate != message_in.block:
            # This means that my block is different from the one that is being approved.
            # TODO: I should probably raise a flag or change my mind.
            logging.error(
                "N{} received an approve status update from N{} with a block ({}) that differs from my candidate ({}).".format(
                    self.id,
                    message_in.node_id,
                    message_in.block,
                    self.block_candidate,
                ))
            return False

        ### At this point the blocks match and the messages chan from that node is correct.

        # Update our messages_approve with the new info from the message.
        self.messages_approve.update(message_in.messages_chain)

        # Save the whole approve message chain for that node.
        self.messages_vote[message_in.node_id] = message_in.messages_chain

        # Try forging the candidate.
        await self.try_forging_candidate_block()

        return True

    async def receive_vote_status_update(self, message_in: Message) -> bool:
        """Receive a vote status update message."""
        # If we are getting this, it means that the block is forged by others, but not us.
        # So we update the info and try to forge ourselves.

        # Save new block as a candidate if we do not already have it.
        # This is the case when we got a vote status update before a commit.
        if not self.block_candidate:
            self.block_candidate = message_in.block

        # Update our vote messages chain.
        for node_id_in in message_in.messages_chain.keys():
            if node_id_in in self.messages_vote:
                self.messages_vote[node_id_in].update(message_in.messages_chain[node_id_in])
            else:
                self.messages_vote[node_id_in] = message_in.messages_chain[node_id_in]

            # We update the local messages_approve with messages from all nodes.  
            self.messages_approve.update(message_in.messages_chain[node_id_in])

        # Save the whole approve message chain for that node.
        self.messages_vote[message_in.node_id] = message_in.messages_chain

        # Now with all the new info, let's try to forge the block.
        await self.try_forging_candidate_block()

        return True

    async def receive(self, message_in: Message):
        """Receive a message from another node."""

        # Check if the message has a block.
        if not message_in.block:
            logging.error(
                "N{} received a message from N{} and discarted because there is no block attached.".format(
                    self.id,
                    message_in.node_id,
            ))
            return False

        # First we verify sent block in the message. If it's bad, we disregard the message.
        if not self.verify_block(message_in.block):
            logging.error(
                "N{} received a message from N{} and discarded it because the block is invalid.".format(
                    self.id,
                    message_in.node_id
                )
            )
            return False

        # Check if the block has already been forged.
        if message_in.block in self.chain:
            # TODO: We should probably send the proof message to the requesting node, so it can forge the block as well.
            logging.info(
                "N{} received a message from N{} and discarted it because this block is already forged.".format(
                    self.id,
                    message_in.node_id,
                )
            )
            return False

        # Save new block as a candidate if we do not already have it.
        if not self.block_candidate:
            self.block_candidate = message_in.block

            # Since we just got a new block and verified it to be good, we broadcast an approval for it.
            await self.send_approve_once()

        # COMMIT
        if message_in.message_type == Message.TYPE_COMMIT:
            return await self.receive_commit(message_in)

        # APPROVE
        elif message_in.message_type == Message.TYPE_APPROVE:
            return await self.receive_approve(message_in)

        # APPROVE_STATUS_UPDATE
        elif message_in.message_type == Message.TYPE_APPROVE_STATUS_UPDATE:
            return await self.receive_approve_status_update(message_in)

        # VOTE_STATUS_UPDATE
        elif message_in.message_type == Message.TYPE_VOTE_STATUS_UPDATE:
            return await self.receive_vote_status_update(message_in)

    async def run(self):
        # Main node run method called from main()
        logging.info("Node {} started. Speed={}".format(self.id, self.connection_speed))

        # Try generating the block if we are the appropriate node.
        await self.gen_commit()


async def main():
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)

    # Generate nodes.
    nodes = []
    for i in range(NODE_COUNT):
        Node(
            nodes=nodes,
            id=i,
            x=random.uniform(0, MAX_DISTANCE),
            y=random.uniform(0, MAX_DISTANCE),
            connection_speed=random.uniform(np.nextafter(0, 1), np.nextafter(1, 2)),
            chain=[],
        )
    logging.info("Nodes generated.")

    # Run node that generates the first block.
    await nodes[nodes[0].get_next_block_master_id()].run()

    succeeded = 0
    for i in range(len(nodes)):
        if len(nodes[i].chain):
            succeeded += 1

    logging.info("{} nodes confirmed the block, {} did not.".format(
        succeeded,
        len(nodes) - succeeded,
    ))

    logging.info("THE END")


if __name__ == "__main__":
    asyncio.run(main())