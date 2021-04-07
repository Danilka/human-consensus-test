from __future__ import annotations
from typing import Union
from block import Block
from candidate.candidate import Candidate


class CandidateManager(list):

    def check_action(self, action: str) -> bool:
        """
        Check if the action was already taken on any candidate.
        :param action: One of the actions from Candidate.POSSIBLE_ACTIONS
        :return: True if action has already been taken, False if the action has not yet been taken.
        """
        for candidate in self:
            if candidate.check_action(action):
                return True
        return False

    def best_candidate(self) -> Candidate:
        """
        Get the candidate that is furthest in the process.
        :return: Candidate that is further along.
        :raises: ValueError if there are no candidates.
        """
        if not self:
            raise ValueError("No Candidates found.")

        return sorted(self, reverse=True)[0]

    def find(
            self,
            block: Union[Block, False] = False,
            block_node_id: Union[int, False] = False,
    ):
        """
        Find candidate in the list by a passed block or block's node ID.
        :param block: Block to look for. Default is False.
        :param block_node_id: Node ID to look for in the candidate blocks. Default is False.
        :return: Index of the first occurrence of Candidate with this block; None if not found.
        """

        # Search by block.
        if block is not False:
            for i in range(len(self)):
                if self[i].block == block:
                    return i

        # Search by block_node_id.
        if block_node_id is not False:
            for i in range(len(self)):
                if self[i].block.node_id == block_node_id:
                    return i

        return None

    def __str__(self) -> str:
        """Get colored string representing all candidate in this CandidateManager."""

        r = ""
        if not self:
            return r

        for candidate in sorted(self, key=lambda x: x.block.node_id if x.block.node_id is not None else float('inf')):
            r += str(candidate)
        return r

    def __repr__(self) -> str:
        """Get basic text representation of all candidates in this CandidateManager."""

        if not self:
            return ""

        r = "["
        for candidate in self:
            r += candidate.__repr__()

        return r + "]"
