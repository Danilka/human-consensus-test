from __future__ import annotations
import logging
import time
from .node_chain_update import NodeChainUpdate


class NodeForge(NodeChainUpdate):
    """Forging related Node functionality."""

    def try_forging_candidate_block(self):
        """
        Attempt to forge a candidate block and send vote status update.
        """

        # Block validation
        if not self.active_candidate:
            logging.info("N{} Unsuccessful forge attempt, there is no candidate block.".format(self.node_id))
            return

        # Check if we have enough vote status updates.
        if not self.enough_vote_status_updates():
            return

        # Just in case validating votes, but this should pass if the vote status update has passed.
        if not self.enough_votes():
            return

        # Log successful forge attempt.
        logging.info("N{} B{} is forged.".format(self.node_id, self.active_candidate.block.block_id))

        # Forge the block and add it to the chain.
        self.chain.append(self.active_candidate.block)
        self.active_candidate.forged = True
        self.active_candidate = None

        # Reset timer since the last forged block.
        self.time_forged = time.time()
