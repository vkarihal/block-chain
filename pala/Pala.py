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
PROPOSED_BLOCK_RECEIVED = set()
temp_delete = -10
un_notarized_block = {}


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
        return f"Block({self.epoch}, {self.seq})"


GENESIS = Block(None, -1, -1)


class Proposal(Message):
    def __init__(self, block: Block):
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


class PalaNode(Node):
    def __init__(self, simulator: Simulator, ID: int):
        super().__init__(simulator, ID)
        self.notarized = set([GENESIS])
        self.votes = {}
        self.epoch = 0
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
        global temp_delete
        rand_hash = hash(str(epoch))
        temp_delete = rand_hash % len(self.simulator.nodes)
        return self.ID == rand_hash % len(self.simulator.nodes)

    def freshest_notarized(self) -> Block:
        # return the last block of a freshest notarized chain
        freshest = GENESIS
        for block in self.notarized:
            if block.epoch > freshest.epoch:
                freshest = block
            elif block.epoch == freshest.epoch:
                if block.seq > freshest.seq:
                    freshest = block
        return freshest

    def add_votes(self, block, sender):
        if block not in self.votes:
            self.votes[block] = set()
        self.votes[block].add(sender)

    def remove_blocks(self, block: Block):
        global un_notarized_block
        if self in un_notarized_block:
            if block in un_notarized_block[self]:
                un_notarized_block[self].remove(block)
                '''
                for temp_block in un_notarized_block[self]:
                    if len(self.votes[temp_block]) >= 2 / 3 * len(self.simulator.nodes):
                        self.notarized.add(temp_block)
                        un_notarized_block[self].remove(temp_block)
                    else:
                        break
                '''

    def finalize(self):
        i = 1

    def count_vote(self, block: Block):
        global un_notarized_block
        '''
        if block not in self.votes:
            self.votes[block] = set()
        self.votes[block].add(sender)
        '''
        condition_1 = len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes)
        condition_2 = block not in self.notarized
        # at times, it can happen that a node receives the votes required for notarization
        # but has not yet received the block
        # so only notarize when it has received the block
        condition_3 = (self, block) in PROPOSED_BLOCK_RECEIVED
        condition_4 = block.parent not in self.notarized
        if condition_1 and condition_2 and condition_3:
            self.notarized.add(block)
            self.remove_blocks(block)
            logging.info(f"{self} notarized {block}, self-epoch : {self.epoch}, block-epoch: {block.epoch}")
            # current_epoch = list(EPOCH_LEADER)[-1]
            if self.epoch == block.epoch:
                for event in self.simulator.queue:
                    # reset Nextepoch() upon block notarization
                    if (self.ID == event.recipient.ID == event.sender.ID == event.author.ID) and type(
                            event.message) == NextEpoch:
                        # at max only one nextepoch event exist for a epoch, so remove old one and add new one
                        self.simulator.queue.remove(event)
                        self.timeout(MINUTE, NextEpoch())
                        logging.info(f"reset NextEpoch")
                        break
                # if you have multiple nextepoch for the same epoch
                # self.timeout(MINUTE, NextEpoch())
                # logging.info(f"reset NextEpoch")
                if EPOCH_LEADER[self.epoch] == self.ID and EPOCH_TIMER == 1:
                    self.timeout(0, InitiateLeader())
            if condition_4:
                # when parent block is un notarized
                # a un-notarized block should not get added to finalized chain
                if self not in un_notarized_block:
                    un_notarized_block[self] = [block.parent]
                    # un_notarized_block[self].append(block)
                else:
                    if block.parent not in un_notarized_block[self]:
                        un_notarized_block[self].append(block.parent)
                    # if block not in un_notarized_block[self]:
                    # un_notarized_block.append(block)

    def clock_logic(self, message: Clock, sender: Node):
        global EPOCH_LEADER
        global seq
        global EPOCH_TIMER
        if message.epoch > self.epoch:
            if self.epoch > 0:
                self.clock_message_count[message.epoch] = 0
            # add clock
            if message.epoch not in self.clock_message_set:
                self.clock_message_set[message.epoch] = set()
            self.clock_message_set[message.epoch].add((sender.ID, self.ID))
            self.update_clock_message_count(message.epoch)
            if self.clock_message_count[message.epoch] == 1:
                # increment epoch and reset NextEpoch+
                self.epoch = message.epoch
                self.timeout(MINUTE, NextEpoch())
                logging.info(f"{self} moves to {self.epoch}")
                if self.is_leader(self.epoch):
                    seq = 1
                    EPOCH_TIMER = 0
                    EPOCH_LEADER[self.epoch] = self.ID
                    self.timeout(SECOND, InitiateLeader())

    def receive(self, message: Message, sender: Node):
        global EPOCH_LEADER
        global seq
        global EPOCH_TIMER
        global PROPOSED_BLOCK_RECEIVED
        message_type = type(message)
        if message_type == NextEpoch:
            # self.me
            clock_message = Clock(self.epoch + 1)
            self.broadcast(clock_message)
            # sending clock message to self
            self.clock_logic(clock_message, self)
            logging.info(f"{self} broadcast clock for epoch {self.epoch + 1}")
        elif message_type == Clock:
            self.clock_logic(message, sender)
        elif message_type == InitiateLeader and self.ID == EPOCH_LEADER[self.epoch]:
            block = Block(self.freshest_notarized(), self.epoch, seq)
            seq += 1
            EPOCH_TIMER = 1
            message = Proposal(block)
            PROPOSED_BLOCK_RECEIVED.add((self, block))
            self.broadcast(message)
            message_vote = Vote(block)
            # new - self vote
            self.add_votes(message_vote.block, self)
            self.count_vote(message_vote.block)
            self.broadcast(message_vote)
            logging.info(
                f"Epoch {self.epoch}: {self} is leader and broadcasts block proposal (length = {block.length})")
        elif message_type == Proposal:
            proposed_block = message.block
            # last block of the freshest notarized chain
            freshest_block = self.freshest_notarized()
            PROPOSED_BLOCK_RECEIVED.add((self, proposed_block))
            logging.info(f"{self} received proposed block {proposed_block}")
            # at times, a node receives the votes required for notarization before the proposed block
            if proposed_block in self.votes:
                self.count_vote(proposed_block)
            condition_1 = proposed_block.epoch == self.epoch
            condition_2 = sender.is_leader(self.epoch)
            proposed_block_voters = set()
            for node in self.simulator.nodes:
                if proposed_block in node.votes:
                    proposed_block_voters.union(node.votes[proposed_block])
            condition_3 = self not in proposed_block_voters
            condition_4 = proposed_block.length == freshest_block.length + 1
            condition_5 = proposed_block.parent in self.notarized
            condition_6 = True
            if self in un_notarized_block:
                condition_6 = 0 == len(un_notarized_block[self])
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6:
                message = Vote(proposed_block)
                # new - self vote
                self.add_votes(message.block, self)
                self.count_vote(message.block)
                self.broadcast(message)
                # i do not think we need to count proposer votes for proposer block notarization in here,
                # they will be done later automatically when Vote event happens
                # self.count_vote(proposed_block)
                logging.info(f"{self} votes on {proposed_block}")
            else:
                logging.info(f"{self} rejects {proposed_block}")
        elif message_type == Vote:
            logging.info(f"{self} received Vote({message.block}) from {sender}")
            self.add_votes(message.block, sender)
            self.count_vote(message.block)


if __name__ == "__main__":
    simulator = Simulator()
    for i in range(TOTAL_NUMBER_OF_NODES):
        simulator.nodes.append(PalaNode(simulator, i))
    simulator.run()
