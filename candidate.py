from collections import Set
from block import Block


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

    def __eq__(self, other) -> bool:
        """Is another object equal to self?"""
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.block == other.block\
            and self.messages_approve == other.messages_approve\
            and self.messages_vote == other.messages_vote\
            and self.vote_status_updates == other.vote_status_updates\
            and self.actions_taken == other.actions_taken\
            and self.forged == other.forged

    def __ne__(self, other) -> bool:
        """Is another object not equal to self?"""
        return not self.__eq__(other)
