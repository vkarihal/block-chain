from __future__ import annotations
from datetime import datetime
import logging

formatter = "%(message)s"
logging.basicConfig(encoding='utf-8', level=logging.INFO, format=formatter)

from Simulator import *

EPOCH_LENGTH = 1
TOTAL_NUMBER_OF_NODES = 20


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

        self.unique_message_received_count_for_finalization = 0
        self.all_messages_received_count_for_finalization = 0
        self.unique_message_received_set = set()
        self.all_messages_received_list = []

        self.last_un_notarized_block_length = 0
        self.finalized = set()
        self.hasVoted = set()
        self.PROPOSED_BLOCK_RECEIVED = set()
        self.un_notarized_list = []

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
            if block.length > longest.length:  # not one amongst the longest, just picks the first longest block
                longest = block
        return longest

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
        condition_3 = block in self.PROPOSED_BLOCK_RECEIVED
        if condition_1 and condition_2 and condition_3:
            self.notarized.add(block)
            self.remove_blocks(block)
            self.finalize(block)
            # logging.info(f"{self} notarized {block}, self-epoch : {self.epoch}, block-epoch: {block.epoch}")

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

    '''
    def count_vote(self, block, sender):
        if block not in self.votes:
            self.votes[block] = set()
        self.votes[block].add(sender)
        if len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes) and block not in self.notarized:
            self.notarized.add(block)
            # logging.info(f"{self} notarized {block}")
    '''

    def receive(self, message: Message, sender: Node):
        self.unique_message_received_set.add(message)
        self.all_messages_received_list.append(message)
        message_type = type(message)
        if message_type == NextEpoch:
            self.epoch += 1
            self.timeout(EPOCH_LENGTH, NextEpoch())
            # if proposer
            if self.is_leader(self.epoch):
                # extend a longest notarized chain
                block = Block(self.longest_notarized(), self.epoch)
                message = Proposal(block)
                self.PROPOSED_BLOCK_RECEIVED.add(block)
                self.add_votes(block, self)
                self.count_vote(block)
                self.broadcast(message)
                '''
                logging.info(
                    f"Epoch {self.epoch}: {self} is leader and broadcasts block proposal (length = {block.length})")
                '''
        elif message_type == Proposal:
            proposed_block = message.block
            # last block of some longest notarized chain
            longest_block = self.longest_notarized()
            self.PROPOSED_BLOCK_RECEIVED.add(proposed_block)
            condition_1 = proposed_block.epoch == self.epoch
            condition_2 = sender.is_leader(self.epoch)
            condition_3 = proposed_block not in self.hasVoted
            condition_4 = proposed_block.length == longest_block.length + 1
            condition_5 = proposed_block.parent in self.notarized
            condition_6 = 0 == len(self.un_notarized_list)
            '''
            if proposed_block.epoch == self.epoch and sender.is_leader(
                    self.epoch) and proposed_block.length == longest_block.length + 1 and proposed_block.parent in \
                    self.notarized:
            '''
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6:
                message = Vote(proposed_block)
                self.add_votes(proposed_block, self)
                self.count_vote(proposed_block)
                self.broadcast(message)
                self.hasVoted.add(proposed_block)
                # logging.info(f"{self} votes on {proposed_block}")
            else:
                oo = 1
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
    for i in range(TOTAL_NUMBER_OF_NODES):
        simulator.nodes.append(StreamletNode(simulator, i))
    simulator.run()

    for i in range(TOTAL_NUMBER_OF_NODES):
        node = simulator.nodes[i]
        # logging.info(f"node {i}, num of block notarized: {len(simulator.nodes[i].notarized)}")

        logging.info(f" node {i}, num of blocks finalized:  {len(simulator.nodes[i].finalized)}, "
                     f"num of blocks notarized:  {len(simulator.nodes[i].notarized)}, "
                     f"unique message count: {node.unique_message_received_count_for_finalization}, "
                     f"total message count: {node.all_messages_received_count_for_finalization}")

    now2 = datetime.now()
    ct2 = now2.strftime("%H:%M:%S")
    logging.info(f"end_time: {ct2}")
