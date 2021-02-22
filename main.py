from __future__ import annotations
from typing import List
import logging
from node import Node
from transport import Transport


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
LOST_MESSAGES_PERCENTAGE = 0.0

# Distance between nodes gets multiplied by this factor and converted to seconds.
DELAY_MULTIPLIER = 0.000001

# How much time should pass before a blank block would be voted for.
BLANK_BLOCK_TIMEOUT = 2.0    # In seconds.


def main():
    # Setup logging.
    logging.basicConfig(format='%(levelname)s: %(message)s', level=LOGGING_LEVEL)

    # Start a transport.
    transport = Transport(
        nodes_count=NODE_COUNT,
        max_distance=MAX_DISTANCE,
        lost_messages_percentage=LOST_MESSAGES_PERCENTAGE,
        delay_multiplier=DELAY_MULTIPLIER,
    )

    # Generate nodes.
    nodes: List[Node] = []
    for i in range(NODE_COUNT):
        Node(
            nodes=nodes,
            node_id=i,
            transport=transport,
            chain=[],
            keep_excessive_messages=KEEP_EXCESSIVE_MESSAGES,
            blank_block_timeout=BLANK_BLOCK_TIMEOUT,
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
