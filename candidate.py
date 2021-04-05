from __future__ import annotations
from collections import Set
from block import Block
from colored import fg, bg, attr


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

    # Candidate block.
    block: Block

    # Incoming approve messages.
    messages_approve: dict

    # Set of node_ids that we received vote status updates from.
    approve_status_updates: Set[int]

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
        self.approve_status_updates = set()
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

    def is_same_kind(self, other: Candidate):
        """
        Check if another Candidate is for the same kind of block.
        :param other: Instance of a Candidate.
        :return: True - it's the same candidate for the same block. False - not.
        """
        return self.block == other.block

    def __eq__(self, other: Candidate) -> bool:
        """Is another object equal to self?"""
        if not isinstance(other, self.__class__):
            return NotImplemented

        return self.block == other.block\
            and self.messages_approve == other.messages_approve\
            and self.messages_vote == other.messages_vote\
            and self.approve_status_updates == other.approve_status_updates\
            and self.vote_status_updates == other.vote_status_updates\
            and self.actions_taken == other.actions_taken\
            and self.forged == other.forged

    def __ne__(self, other: Candidate) -> bool:
        """Is another object not equal to self?"""
        return not self.__eq__(other)

    def __gt__(self, other: Candidate):
        """
        Is self further in approval process than other.
        Note that this does not verify validity of the votes.
        :param other: Instance of a Candidate to compare.
        :return: True - self is greater. False - other is greater or equal.
        :raises: ValueError - if the candidates are for different blocks. NotImplemented - if improper other is passed.
        """
        if not isinstance(other, self.__class__):
            return NotImplemented

        if not self.is_same_kind(other):
            raise ValueError("Cannot compare Candidate with a different kind of block.")

        if self.forged and not other.forged:
            return True
        elif other.forged and not self.forged:
            return False

        if len(self.vote_status_updates) > len(other.vote_status_updates):
            return True
        elif len(self.vote_status_updates) < len(other.vote_status_updates):
            return False

        if len(self.messages_vote) > len(other.messages_vote):
            return True
        elif len(self.messages_vote) < len(other.messages_vote):
            return False

        if len(self.approve_status_updates) > len(other.approve_status_updates):
            return True
        elif len(self.approve_status_updates) < len(other.approve_status_updates):
            return False

        if len(self.messages_approve) > len(other.messages_approve):
            return True
        elif len(self.messages_approve) < len(other.messages_approve):
            return False

        return False    # Candidates are equal

    def __ge__(self, other):
        """
        Is self further or equal in approval process than other.
        Note that this does not verify validity of the votes.
        :param other: Instance of a Candidate to compare.
        :return: True - self is greater or equal. False - other is greater.
        :raises: ValueError - if the candidates are for different blocks. NotImplemented - if improper other is passed.
        """
        return self == other or self > other

    def __lt__(self, other):
        """
        Is self earlier in approval process than other.
        Note that this does not verify validity of the votes.
        :param other: Instance of a Candidate to compare.
        :return: True - other is greater. False - self is greater or equal.
        :raises: ValueError - if the candidates are for different blocks. NotImplemented - if improper other is passed.
        """
        return not self == other and not self > other

    def __le__(self, other):
        """
        Is self earlier or equal in approval process than other.
        Note that this does not verify validity of the votes.
        :param other: Instance of a Candidate to compare.
        :return: True - self is less or equal. False - other is greater.
        :raises: ValueError - if the candidates are for different blocks. NotImplemented - if improper other is passed.
        """
        return self == other or not self > other
        pass

    def __str__(self):
        r = "["

        # Pick background color based on if the block is blank or not.
        r += bg('grey_30') if self.block.node_id is None else bg('green')

        # Block ID
        r += fg('white')
        r += str(self.block.block_id)
        r += attr('reset')

        # Approves
        r += fg('black')
        approves = len(self.messages_approve)
        approve_sus = len(self.approve_status_updates)
        for i in range(max(approves, approve_sus)):
            if i < approve_sus and i < approves:
                r += bg('gold_1')
                r += "|"
            elif i < approves:
                r += bg('gold_1')
                r += " "
            elif i < approve_sus:
                r += "|"
        r += attr('reset')

        # Votes
        r += fg('black')
        votes = len(self.messages_vote)
        vote_sus = len(self.vote_status_updates)
        for i in range(max(votes, vote_sus)):
            r += fg('black')
            if i < vote_sus and i < votes:
                r += bg('deep_sky_blue_3b')
                r += "|"
            elif i < votes:
                r += bg('deep_sky_blue_3b')
                r += " "
            elif i < vote_sus:
                r += "|"
        r += attr('reset')

        r += "]"
        return r

    def __repr__(self):
        return self.__str__()


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

    def find(self, block: Block):
        """
        Find candidate in the list by a passed block.
        :param block: Block to look for.
        :return: Index of the first occurrence of Candidate with this block; None if not found.
        """
        for i in range(len(self)):
            if self[i].block == block:
                return i
        return None

    def __str__(self):
        """Get printable string representing all candidate in this CandidateManager."""
        r = ""
        if not self:
            return r

        for candidate in self:
            r += str(candidate)
        return r
