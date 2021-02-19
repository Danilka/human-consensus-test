from __future__ import annotations
import copy
import logging
import math
import random
import time
from typing import List, Union, Tuple
import numpy as np
from message import Message


class Transport:

    # Multiplies all time delays by this value.
    delay_multiplier: float

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

    def __init__(
            self,
            nodes_count: int,
            max_distance: float,
            lost_messages_percentage: float,
            delay_multiplier: float,
    ):
        self.delay_multiplier = delay_multiplier
        self.pool = []
        self.nodes_map = []
        for i in range(nodes_count):
            self.nodes_map.append(
                {
                    "x": random.uniform(0, max_distance),
                    "y": random.uniform(0, max_distance),
                    "drop_rate": lost_messages_percentage,
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
        return distance * avg_connection_speed * self.delay_multiplier

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
