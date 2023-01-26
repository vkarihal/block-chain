from __future__ import annotations
from datetime import datetime

import logging

formatter = "%(message)s"
logging.basicConfig(encoding='utf-8', level=logging.INFO, format=formatter)

from Simulator import *

DELTA = 1
SECOND = 6 * DELTA
MINUTE = 30 * DELTA
TOTAL_NUMBER_OF_NODES = 5
OFFLINE_NODES = 0


class Block:
    def __init__(self, parent: Block, epoch: int, seq: int):
        self.parent = parent
        if parent is not None:
            self.parent.child = self
        self.epoch = epoch
        self.seq = seq

        if parent is None:  # genesis
            self.length = 0
        else:
            self.length = parent.length + 1
            # self.hash = hash(parent)

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
    def __init__(self, simulator: Simulator, ID: int, active: bool):
        super().__init__(simulator, ID)
        self.unique_message_received_count_for_finalization = 0
        self.all_messages_received_count_for_finalization = 0
        self.unique_message_received_set = set()
        self.all_messages_received_list = []
        self.messages_send = set()
        self.all_messages_send = []

        self.seq = -1
        self.EPOCH_TIMER = -1
        self.last_un_notarized_block_length = 0
        self.notarized = set([GENESIS])
        self.finalized = set()
        self.votes = {}
        self.hasVoted = set()
        self.epoch = 0
        self.PROPOSED_BLOCK_RECEIVED = set()
        self.un_notarized_list = []
        self.clock_message_count = {self.epoch + 1: 1}
        self.clock_message_set = {self.epoch + 1: set()}
        # to initiate the protocol Clock Message for next epoch added to event queue
        if active:
            self.timeout(MINUTE, Clock(self.epoch + 1))

    def update_clock_message_count(self, epoch: int):
        if len(self.clock_message_set[epoch]) >= 2 / 3 * len(self.simulator.nodes):
            self.clock_message_count[epoch] = 1

    def is_leader(self, epoch: int):
        return self.ID == epoch % len(self.simulator.nodes)

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
        if block in self.un_notarized_list:
            self.un_notarized_list.remove(block)

    def finalize_logic(self, notarized_block: Block):
        if notarized_block.length >= 3:
            blk_2 = notarized_block.parent
            blk_1 = blk_2.parent
            if blk_2 in self.notarized and blk_1 in self.notarized:
                blk_set_left = self.notarized.difference(set([notarized_block]))
                blk_set_left = blk_set_left.difference(self.finalized)
                if len(blk_set_left) > 0:
                    self.finalized = self.finalized.union(blk_set_left)
                    self.unique_message_received_count_for_finalization = len(self.unique_message_received_set)
                    self.all_messages_received_count_for_finalization = len(self.all_messages_received_list)

    def finalize(self, notarized_block: Block):
        if len(self.un_notarized_list) == 0:
            self.finalize_logic(notarized_block)
        else:
            tmp_block = notarized_block
            if tmp_block.length < self.un_notarized_list[0].length:
                while tmp_block.length < self.un_notarized_list[0].length:
                    tmp_block = tmp_block.child
                if notarized_block.length < self.un_notarized_list[0].length:
                    tmp_block = tmp_block.parent
                self.finalize_logic(tmp_block)

    def count_vote(self, block: Block):
        condition_1 = len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes)
        condition_2 = block not in self.notarized
        # at times, it can happen that a node receives the votes required for notarization
        # but has not yet received the block
        # so only notarize when it has received the block
        condition_3 = block in self.PROPOSED_BLOCK_RECEIVED
        if condition_1 and condition_2 and condition_3:
            self.notarized.add(block)
            self.remove_blocks(block)
            self.finalize(block)
            # logging.info(f"{self} notarized {block}, self-epoch : {self.epoch}, block-epoch: {block.epoch}")
            if self.epoch == block.epoch:
                for event in self.simulator.queue:
                    # reset Nextepoch() upon block notarization
                    if (self.ID == event.recipient.ID == event.sender.ID == event.author.ID) and type(
                            event.message) == NextEpoch:
                        # at max only one nextepoch event exist for a epoch, so remove old one and add new one
                        self.simulator.queue.remove(event)
                        self.timeout(MINUTE, NextEpoch())
                        # logging.info(f"reset NextEpoch")
                        break
                if self.is_leader(self.epoch) and self.EPOCH_TIMER == 1:
                    self.timeout(0, InitiateLeader())
            if block.parent not in self.notarized:
                # when parent block is un notarized
                # a un-notarized block should not get added to finalized chain
                tmp_list = []
                blk = block.parent
                while blk.length > self.last_un_notarized_block_length:
                    if blk not in self.notarized:
                        tmp_list.insert(0, blk)
                    blk = blk.parent
                self.un_notarized_list = self.un_notarized_list + tmp_list
                if block.parent.length > self.last_un_notarized_block_length:
                    self.last_un_notarized_block_length = block.parent.length

    def clock_logic(self, message: Clock, sender: Node):
        if message.epoch > self.epoch:
            if self.epoch > 0:
                self.clock_message_count[message.epoch] = 0
            # add clock
            if message.epoch not in self.clock_message_set:
                self.clock_message_set[message.epoch] = set()
            self.clock_message_set[message.epoch].add((sender.ID, self.ID))
            # logging.info(f"{self} in epoch {self.epoch} rx. clock from {sender} for {message.epoch}")
            self.update_clock_message_count(message.epoch)
            if self.clock_message_count[message.epoch] == 1:
                # increment epoch and reset NextEpoch+
                self.epoch = message.epoch
                self.timeout(MINUTE, NextEpoch())
                # logging.info(f"{self} moves to {self.epoch}")
                if self.is_leader(self.epoch):
                    self.seq = 1
                    self.EPOCH_TIMER = 0
                    self.timeout(SECOND, InitiateLeader())

    def receive(self, message: Message, sender: Node):
        self.unique_message_received_set.add(message)
        self.all_messages_received_list.append(message)
        self.messages_send.add(message)
        sender.all_messages_send.append(message)
        message_type = type(message)
        if message_type == NextEpoch:
            # self.me
            clock_message = Clock(self.epoch + 1)
            # sending clock message to self
            self.clock_logic(clock_message, self)
            self.broadcast(clock_message)
            # logging.info(f"{self} broadcast clock for epoch {self.epoch + 1}")
        elif message_type == Clock:
            self.clock_logic(message, sender)
        elif message_type == InitiateLeader and self.is_leader(self.epoch):
            if self.EPOCH_TIMER == 0:
                self.EPOCH_TIMER = 1
            block = Block(self.freshest_notarized(), self.epoch, self.seq)
            self.seq += 1
            message = Proposal(block)
            self.PROPOSED_BLOCK_RECEIVED.add(block)
            self.broadcast(message)
            message_vote = Vote(block)
            # new - self vote
            self.add_votes(message_vote.block, self)
            self.count_vote(message_vote.block)
            self.broadcast(message_vote)
            '''
            logging.info(
             f"Epoch {self.epoch}: {self} is leader and broadcasts block {block} proposal (length = {block.length})")
             '''
        elif message_type == Proposal:
            proposed_block = message.block
            # last block of the freshest notarized chain
            freshest_block = self.freshest_notarized()
            self.PROPOSED_BLOCK_RECEIVED.add(proposed_block)
            # logging.info(f"{self} received proposed block {proposed_block}")
            # at times, a node receives the votes required for notarization before the proposed block
            if proposed_block in self.votes:
                self.count_vote(proposed_block)
            condition_1 = proposed_block.epoch == self.epoch
            condition_2 = sender.is_leader(self.epoch)
            condition_3 = proposed_block not in self.hasVoted
            condition_4 = proposed_block.length == freshest_block.length + 1
            condition_5 = proposed_block.parent in self.notarized
            condition_6 = 0 == len(self.un_notarized_list)
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6:
                message = Vote(proposed_block)
                # new - self vote
                self.add_votes(message.block, self)
                self.count_vote(message.block)
                self.broadcast(message)
                self.hasVoted.add(proposed_block)
                # logging.info(f"{self} votes on {proposed_block}")
            else:
                pass
                # logging.info(f"{self} rejects {proposed_block}")
        elif message_type == Vote:
            # logging.info(f"{self} received Vote({message.block}) from {sender}")
            self.add_votes(message.block, sender)
            self.count_vote(message.block)


if __name__ == "__main__":
    now1 = datetime.now()
    ct1 = now1.strftime("%H:%M:%S")
    logging.info(f"start_time: {ct1}")

    simulator = Simulator()
    '''
    for i in range(TOTAL_NUMBER_OF_NODES):
        simulator.nodes.append(PalaNode(simulator, i))
    '''

    for i in range(TOTAL_NUMBER_OF_NODES):
        if i < TOTAL_NUMBER_OF_NODES - OFFLINE_NODES:
            simulator.nodes.append(PalaNode(simulator, i, True))
        else:
            simulator.nodes.append(PalaNode(simulator, i, False))
            simulator.offline_nodes.add(simulator.nodes[i])
            # simulator.queue.remove()

    if len(simulator.offline_nodes) >= (2 / 3) * TOTAL_NUMBER_OF_NODES:
        logging.info(f"exit: number of offline nodes greater then two-thirds total nodes")
        exit()
    simulator.run()
    for i in range(TOTAL_NUMBER_OF_NODES):
        node = simulator.nodes[i]
        logging.info(f" node {i}, num of blocks finalized:  {len(simulator.nodes[i].finalized)}, "
                     f"num of blocks notarized:  {len(simulator.nodes[i].notarized)}, "
                     f"unique message count: {node.unique_message_received_count_for_finalization}, "
                     f"total message count: {node.all_messages_received_count_for_finalization},"
                     f" total message send: {len(node.all_messages_send)}")

    now2 = datetime.now()
    ct2 = now2.strftime("%H:%M:%S")
    logging.info(f"end_time: {ct2}")
