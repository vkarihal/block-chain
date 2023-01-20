from __future__ import annotations

from numpy.random import exponential

import logging

AVERAGE_DELAY = 4
ORDERED_DELIVERY = False
IMPLICIT_ECHOING = True


# base class for protocol messages
class Message:
    def __init__(self):
        pass

    def __str__(self):
        return str(type(self).__name__)


# base class for protocol node
class Node:
    def __init__(self, simulator: Simulator, ID: int):
        self.simulator = simulator
        self.ID = ID
        if IMPLICIT_ECHOING:
            self.messages_seen = set()

    def __str__(self):
        return f"<{self.ID}>"

    def send(self, recipient: Node, message: Message):
        self.simulator.communicate(recipient, message, self, self)

    def broadcast(self, message: Message):
        for node in self.simulator.nodes:
            if node != self:
                self.send(node, message)

    def timeout(self, delay: float, message: Message):
        arrival_time = self.simulator.time + delay
        event = Event(arrival_time, self, message, self, self)
        self.simulator.queue.append(event)
        # logging.debug(f"[{self.simulator.time:.2f}] {self} [{arrival_time:.2f}]: {message}")

    # to be overridden by protocol
    def receive(self, message: Message, author: Node):
        pass


# container for an event
class Event:
    def __init__(self, time: float, recipient: Node, message: Message, sender: Node, author: Node):
        self.time = time
        self.recipient = recipient
        self.message = message
        self.sender = sender
        self.author = author  # original sender (for implicit echoing)


class Simulator:
    def __init__(self):
        self.time = 0
        self.queue = []
        self.nodes = []

    def communicate(self, recipient: Node, message: Message, sender: Node, author: Node):
        # do not call directly from outside this module
        if IMPLICIT_ECHOING:
            sender.messages_seen.add(message)
        arrival_time = self.time + exponential(AVERAGE_DELAY)
        if ORDERED_DELIVERY:
            for event in self.queue:
                # message must arrive no earlier than any other message between these nodes
                if event.recipient == recipient and event.sender == sender and event.time > arrival_time:
                    arrival_time = event.time
        event = Event(arrival_time, recipient, message, sender, author)
        self.queue.append(event)
        if author != sender:
            logging.debug(f"[{self.time:.2f}] {author} -- {sender} --> {recipient} [{arrival_time:.2f}]: {message}")
        else:
            logging.debug(f"[{self.time:.2f}] {sender} --> {recipient} [{arrival_time:.2f}]: {message}")

    def run(self, time_limit: float = 3000):  # time_limit = 6000
        while self.queue:
            # find earliest event to simulate next
            next_event = self.queue[0]
            for event in self.queue:
                if event.time < next_event.time:
                    next_event = event
            self.queue.remove(next_event)
            recipient = next_event.recipient
            message = next_event.message
            sender = next_event.sender
            author = next_event.author
            # terminate if simulation time is up
            # logging.debug(f"event_time : {next_event.time} and time_limit : {time_limit}")
            # print('event_time :', next_event.time, 'and time_limit :', time_limit)
            if next_event.time >= time_limit:
                return
            # advance simulation time to next event
            self.time = next_event.time
            # implicit echoing
            if IMPLICIT_ECHOING and sender != recipient:  # does not apply to timeouts
                if message not in recipient.messages_seen:
                    # unseen message from another node
                    recipient.messages_seen.add(message)
                    for node in self.nodes:
                        if node != recipient and node != sender:
                            # echo from recipient with original author
                            self.communicate(node, message, recipient, author)
                '''            
                else:
                    continue  # skip event handling
                '''
            # let node handle event
            recipient.receive(message, author)
