from __future__ import annotations
import time
from abc import abstractmethod
from collections import defaultdict
from typing import List, Union
from block import Block
from candidate import Candidate, CandidateManager
from message import Message
from transport import Transport
from .node_exceptions import NodeExceptions


class NodeBase(NodeExceptions):
    """Base Node class with all variables and a constructor."""

    # ID of the node.
    node_id: int

    # Pointer to the list of all nodes.
    nodes: List[NodeBase]

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
        nodes: List[NodeBase],
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

        # Return the latest of all times.
        return sorted(activity_times, reverse=True)[0]

    @abstractmethod
    def validate_chain(self, chain=None) -> bool:
        pass
