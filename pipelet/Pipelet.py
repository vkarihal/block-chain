from __future__ import annotations

import logging

formatter = "%(message)s"
logging.basicConfig(encoding='utf-8', level=logging.INFO, format=formatter)

from Simulator import *

DELTA = 1
SECOND = 6 * DELTA
MINUTE = 30 * DELTA
# LEADER_ID = -1
# keeps track of sequence number in a epoch
seq = -1
# keeps track of current epoch's leader
EPOCH_LEADER = {}
EPOCH_TIMER = 0
TOTAL_NUMBER_OF_NODES = 5


# NODE_CURRENT_EPOCH = {}


class Block:
    def __init__(self, parent: Block, epoch: int, seq: int):
        self.parent = parent
        self.epoch = epoch
        self.seq = seq

        if parent is None:  # genesis
            self.length = 0
        else:
            self.length = parent.length + 1
            self.hash = hash(parent)

    def __str__(self):
        return f"Block({self.epoch})"


GENESIS = Block(None, -1, -1)


class Proposal(Message):
    def __init__(self, block: Block, vote_count: int):
        super().__init__()
        self.block = block


class InitiateLeader(Message):
    def __init__(self):
        super().__init__()


class Vote(Message):
    def __init__(self, block: Block):
        super().__init__()
        self.block = block


class NextEpoch(Message):
    def __init__(self):
        super().__init__()


class Clock(Message):
    def __init__(self, epoch: int):
        super().__init__()
        self.epoch = epoch


class Sync(Message):
    def __init__(self, set_of_blocks: set):
        super().__init__()
        self.chains = set()
        for block in set_of_blocks:
            self.chains.add(block)


class PipeletNode(Node):
    def __init__(self, simulator: Simulator, ID: int):
        super().__init__(simulator, ID)
        self.notarized = set([GENESIS])
        self.votes = {}
        self.epoch = 0
        self.longest_chain_set = set([GENESIS])
        self.sync_block_set = set([GENESIS])
        # test = set()
        self.clock_message_count = {self.epoch + 1: 1}
        self.clock_message_set = {self.epoch + 1: set()}
        # to initiate the protocol Clock Message for next epoch added to event queue
        self.timeout(MINUTE, Clock(self.epoch + 1))

    def update_clock_message_count(self, epoch: int):
        if len(self.clock_message_set[epoch]) >= 2 / 3 * len(self.simulator.nodes):
            self.clock_message_count[epoch] = 1

    def is_leader(self, epoch: int):
        # check if the node is the leader of an epoch

        # rand_hash = hash(str(epoch))
        # return self.ID == rand_hash % len(self.simulator.nodes)
        return self.ID == len(self.simulator.nodes) % epoch

    def longest_notarized(self) -> Block:
        # return the last block of a longest notarized chain
        longest = GENESIS
        for block in self.notarized:
            if block.length > longest.length:
                longest = block
        return longest

    def update_longest_set(self, longest_length: int):
        longest_blocks = set()
        for block in self.notarized:
            if block.length == longest_length:
                longest_blocks.add(block)
        for block in longest_blocks:
            if block in self.longest_chain_set:
                continue
            else:
                self.longest_chain_set.add(block)
                temp_block = block.parent
                while temp_block not in GENESIS:
                    if temp_block in self.longest_chain_set:
                        break
                    else:
                        self.longest_chain_set.add(temp_block)
                        temp_block = temp_block.parent

    def count_vote(self, block, sender):
        if block not in self.votes:
            self.votes[block] = set()
        self.votes[block].add(sender)
        if len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes) and block not in self.notarized:
            self.notarized.add(block)
            logging.info(f"{self} notarized {block}, self-epoch : {self.epoch}, block-epoch: {block.epoch}")
            # current_epoch = list(EPOCH_LEADER)[-1]
            if self.epoch == block.epoch:  # current_epoch:
                self.reset_next_epoch()
                if EPOCH_LEADER[self.epoch].ID == self.ID and EPOCH_TIMER == 1:
                    self.timeout(0, InitiateLeader())

    def reset_next_epoch(self):
        for event in self.simulator.queue:
            if (self.ID == event.recipient.ID == event.sender.ID == event.author.ID) and type(
                    event.message) == NextEpoch:
                self.simulator.queue.remove(event)
                self.timeout(MINUTE, NextEpoch())

    def receive(self, message: Message, sender: Node):
        global EPOCH_LEADER
        global seq
        global EPOCH_TIMER
        message_type = type(message)
        if message_type == NextEpoch:
            self.broadcast(Clock(self.epoch + 1))
            self.reset_next_epoch()
        elif message_type == Sync:
            previous_longest_notarized_block = self.longest_notarized()
            self.notarized.union(message.chains)
            new_longest_notarized_block = self.longest_notarized()
            if new_longest_notarized_block.length > previous_longest_notarized_block.length:
                self.reset_next_epoch()
        elif message_type == Clock:
            longest_notarized_block_length = self.longest_notarized().length
            self.update_longest_set(longest_notarized_block_length)
            # broadcast sync blocks
            if len((self.longest_chain_set.difference(self.sync_block_set))) > 0:
                self.broadcast(Sync(self.longest_chain_set.difference(self.sync_block_set)))
                self.sync_block_set = self.longest_chain_set.union(self.sync_block_set)
            if message.epoch > self.epoch:
                self.clock_message_count[message.epoch] = 0
                # add clock
                if message.epoch not in self.clock_message_set:
                    self.clock_message_set[message.epoch] = set()
                self.clock_message_set[message.epoch].add((sender.ID, self.ID, message))
                self.update_clock_message_count(message.epoch)
                if self.clock_message_count[message.epoch] == 1:
                    # increment epoch and reset NextEpoch()
                    self.epoch = message.epoch
                    self.timeout(MINUTE, NextEpoch())
                    self.broadcast(message)
                    '''  
                    if self.ID != sender.ID and message not in self.messages_seen:
                        self.broadcast(message)
                    '''
                    if self.is_leader(self.epoch):
                        seq = 1
                        EPOCH_TIMER = 0
                        EPOCH_LEADER[self.epoch] = self
                        self.timeout(SECOND, InitiateLeader())
        elif message_type == InitiateLeader and self.ID == EPOCH_LEADER[self.epoch].ID:
            if EPOCH_TIMER == 0:
                EPOCH_TIMER = 1
            block = Block(self.longest_notarized(), self.epoch, seq)
            seq += 1
            vote_count_for_parent_of_proposed_block = 0
            if block.parent is None:
                vote_count_for_parent_of_proposed_block = TOTAL_NUMBER_OF_NODES
            else:
                vote_count_for_parent_of_proposed_block = len(self.votes[block.parent])
            message = Proposal(block, vote_count_for_parent_of_proposed_block)
            self.broadcast(message)
            message_vote = Vote(block)
            self.send(self, message_vote)
            # self.broadcast(message_vote)
            logging.info(
                f"Epoch {self.epoch}: {self} is leader and broadcasts block proposal (length = {block.length})")
        elif message_type == Proposal:
            proposed_block = message.block
            # last block of the longest notarized chain
            # previous_longest_block = self.longest_notarized()
            # check logic again- written in half sleep
            if message.vote_count >= 2 / 3 * len(self.simulator.nodes):
                previous_longest_block = self.longest_notarized()
                self.notarized.add(proposed_block)
                new_longest_block = self.longest_notarized()
                if new_longest_block.length > previous_longest_block.length:
                    self.reset_next_epoch()
                    condition_1 = ((proposed_block.epoch == self.epoch) and sender.is_leader(
                        self.epoch) and proposed_block not in self.votes)
                    condition_2 = (
                            (
                                    proposed_block.length == previous_longest_block.length + 1) and proposed_block.parent in self.notarized)
                    if condition_1 and condition_2:
                        message = Vote(proposed_block)
                        # self.broadcast(message)
                        self.send(EPOCH_LEADER[self.epoch], message)
                        # i do not think we need to count proposer votes for proposer block notarization in here, they will be done later automatically when Vote event happens
                        # self.count_vote(proposed_block, self)
                        logging.info(f"{self} votes on {proposed_block}")
                    else:
                        logging.info(f"{self} rejects {proposed_block}")
        elif message_type == Vote:
            logging.info(f"{self} received Vote({message.block}) from {sender}")
            self.count_vote(message.block, sender)


if __name__ == "__main__":
    simulator = Simulator()
    for i in range(TOTAL_NUMBER_OF_NODES):
        simulator.nodes.append(PipeletNode(simulator, i))
    simulator.run()
