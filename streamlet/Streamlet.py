from __future__ import annotations

import logging

formatter = "%(message)s"
logging.basicConfig(encoding='utf-8', level=logging.INFO, format=formatter)

from Simulator import *

EPOCH_LENGTH = 1
TOTAL_NUMBER_OF_NODES = 5

class Block:
    def __init__(self, parent: Block, epoch: int):
        self.parent = parent
        self.epoch = epoch
        if parent == None:  # genesis
            self.length = 0
        else:
            self.length = parent.length + 1

    def __str__(self):
        return f"Block({self.epoch})"


GENESIS = Block(None, -1)


class Proposal(Message):
    def __init__(self, block: Block):
        super().__init__()
        self.block = block


class Vote(Message):
    def __init__(self, block: Block):
        super().__init__()
        self.block = block


class NextEpoch(Message):
    def __init__(self):
        super().__init__()


class StreamletNode(Node):
    def __init__(self, simulator: Simulator, ID: int):
        super().__init__(simulator, ID)
        self.notarized = set([GENESIS])
        self.votes = {}
        self.epoch = 0
        self.timeout(EPOCH_LENGTH, NextEpoch())

    def is_leader(self, epoch: int):
        # check if the node is the leader of an epoch
        rand_hash = hash(str(epoch))
        return self.ID == rand_hash % len(self.simulator.nodes)

    def longest_notarized(self) -> Block:
        # return the last block of a longest notarized chain
        longest = GENESIS
        for block in self.notarized:
            if block.length > longest.length: # not one amongst the longest, just picks the first longest block
                longest = block
        return longest

    def count_vote(self, block, sender):
        if block not in self.votes:
            self.votes[block] = set()
        self.votes[block].add(sender)
        if len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes) and block not in self.notarized:
            self.notarized.add(block)
            logging.info(f"{self} notarized {block}")

    def receive(self, message: Message, sender: Node):
        message_type = type(message)
        if message_type == NextEpoch:
            self.epoch += 1
            self.timeout(EPOCH_LENGTH, NextEpoch())
            # if proposer
            if self.is_leader(self.epoch):
                # extend a longest notarized chain
                block = Block(self.longest_notarized(), self.epoch)
                message = Proposal(block)
                self.broadcast(message)
                logging.info(
                    f"Epoch {self.epoch}: {self} is leader and broadcasts block proposal (length = {block.length})")
        elif message_type == Proposal:
            proposed_block = message.block
            # last block of some longest notarized chain
            longest_block = self.longest_notarized()
            if proposed_block.epoch == self.epoch and sender.is_leader(
                    self.epoch) and proposed_block.length == longest_block.length + 1 and proposed_block.parent in self.notarized:
                message = Vote(proposed_block)
                self.broadcast(message)
                self.count_vote(proposed_block, self)
                logging.info(f"{self} votes on {proposed_block}")
            else:
                logging.info(f"{self} rejects {proposed_block}")
        elif message_type == Vote:
            logging.info(f"{self} received Vote({message.block}) from {sender}")
            self.count_vote(message.block, sender)


if __name__ == "__main__":
    simulator = Simulator()
    for i in range(TOTAL_NUMBER_OF_NODES):
        simulator.nodes.append(StreamletNode(simulator, i))
    simulator.run()
