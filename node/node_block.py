from __future__ import annotations
from typing import Union
from colored import fg, bg, attr
from block import Block
from candidate import Candidate
from .node_validator import NodeValidator


class NodeBlock(NodeValidator):
    """Block related Node functionality. Chain, blocks, candidates."""

    def get_last_block(self) -> Block:
        """
        Get the last forged block in self.chain.
        :return: Block instance of the last block.
        :raises: Node.NodeValueError if self.chain is empty.
        """

        if not self.chain:
            raise self.NodeValueError("Chain is empty. There is no last block.")

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
        """
        Get next block master node ID
        :notice: This assumes that nodes never change after initiating the group.
        :return: Node ID for the next block in chain.
        """

        return self.get_next_block_id() % len(self.nodes)

    def set_active_candidate(self, block: Union[Block, None] = None):
        """
        Sets self.active_candidate to the best current candidate or a passed block.
        :param block: Instance of a Block or None
        :raises: NodeValueError if the candidate was not set.
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
        """Returns current node stage in a colored text format."""

        return "N{node_id:02d} B:{chain}{next_candidates}".format(
            node_id=self.node_id,
            chain=self.chain_str(),
            next_candidates=self.candidates_str(self.get_next_block_id()),
        )

    def __repr__(self):
        """Returns current node stage in a blank text format."""

        return "N{} Chain:{} Candidates:{}".format(
            self.node_id,
            self.chain.__repr__(),
            self.candidates.__repr__(),
        )
