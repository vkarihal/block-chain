from __future__ import annotations

import argparse
from datetime import datetime
import logging

formatter = "%(message)s"
logging.basicConfig(encoding='utf-8', level=logging.INFO, format=formatter)

from Simulator import *

EPOCH_LENGTH = 1
TOTAL_NUMBER_OF_NODES = 3
OFFLINE_NODES = 0


class Block:
    def __init__(self, parent: Block, epoch: int):
        self.parent = parent
        self.epoch = epoch
        if parent is None:  # genesis
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
    def __init__(self, simulator: Simulator, ID: int, active: bool):
        super().__init__(simulator, ID)

        self.unique_message_received_count_for_finalization = 0
        self.all_messages_received_count_for_finalization = 0
        self.unique_message_received_set = set()
        self.all_messages_received_list = []
        self.messages_send =[]
        self.all_messages_send = []

        self.last_un_notarized_block_length = 0
        self.finalized = set()
        self.hasVoted = set()
        self.PROPOSED_BLOCK_RECEIVED = set()
        self.un_notarized_list = []

        self.notarized = set([GENESIS])
        self.votes = {}
        self.epoch = 0
        # self.timeout(EPOCH_LENGTH, NextEpoch())
        if active:
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
            # logging.info(f"{self} moves to {self.epoch}")
            # if proposer
            if self.is_leader(self.epoch):
                # extend a longest notarized chain
                block = Block(self.longest_notarized(), self.epoch)
                message = Proposal(block)
                self.PROPOSED_BLOCK_RECEIVED.add(block)
                self.add_votes(block, self)
                self.count_vote(block)
                self.broadcast(message)
                message_vote = Vote(block)
                self.broadcast(message_vote)
                '''
                logging.info(
                    f"Epoch {self.epoch}: {self} is leader and broadcasts block proposal {block} at (length = {block.length})")
                '''
        elif message_type == Proposal:
            proposed_block = message.block
            # last block of some longest notarized chain
            longest_block = self.longest_notarized()
            self.PROPOSED_BLOCK_RECEIVED.add(proposed_block)
            # logging.info(f"{self} receives on {proposed_block}")
            if proposed_block in self.votes:
                self.count_vote(proposed_block)
            condition_1 = proposed_block.epoch == self.epoch
            condition_2 = sender.is_leader(self.epoch)
            condition_3 = proposed_block.epoch not in self.hasVoted
            condition_4 = proposed_block.length == longest_block.length + 1
            condition_5 = proposed_block.parent in self.notarized
            condition_6 = 0 == len(self.un_notarized_list)
            if condition_1 and condition_2 and condition_3 and condition_4 and condition_5 and condition_6:
                message = Vote(proposed_block)
                # logging.info(f"{self} votes on {proposed_block}")
                self.add_votes(proposed_block, self)
                self.count_vote(proposed_block)
                self.broadcast(message)
                self.hasVoted.add(proposed_block.epoch)
            else:
                '''
                #debug purpose
                oo = set()
                if condition_1 is False:
                    oo.add("proposed block epoch does not matches node's epoch, ")
                if condition_2 is False:
                    oo.add("sender is not the leader of the epoch, ")
                if condition_3 is False:
                    str_id = "node" + str(self.ID) + "has already voted for the epoch, "
                    oo.add(str_id)
                if condition_4 is False:
                    oo.add("proposed block length is not 1 greater than longest notarized block length, ")
                if condition_5 is False:
                    oo.add("proposed block parent not in notarized set, ")
                if condition_6 is False:
                    oo.add("there exist a un notarized block, ")
                logging.info(f"{self} rejects {proposed_block} for reason: {oo}")
                '''
                pass
        elif message_type == Vote:
            # logging.info(f"{self} received Vote({message.block}) from {sender}")
            self.add_votes(message.block, sender)
            self.count_vote(message.block)


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, help="Total number of nodes", default=3)  # 5
    parser.add_argument("--timer", type=int, help="Maximum number of iterations", default=5)  # 5000
    parser.add_argument("--offline_nodes", type=int, help="No of offline nodes", default=0)
    parser.add_argument("--delay", type=int, help="average_delay", default=0.1)
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    '''
    x = get_args()
    print(x.nodes, x.timer)
    '''

    now1 = datetime.now()
    ct1 = now1.strftime("%H:%M:%S")
    logging.info(f"start_time: {ct1}")

    simulator = Simulator()
    for i in range(TOTAL_NUMBER_OF_NODES):
        if i < TOTAL_NUMBER_OF_NODES - OFFLINE_NODES:
            simulator.nodes.append(StreamletNode(simulator, i, True))
        else:
            simulator.nodes.append(StreamletNode(simulator, i, False))
            simulator.offline_nodes.add(simulator.nodes[i])
            # simulator.queue.remove()

    if len(simulator.offline_nodes) >= (2 / 3) * TOTAL_NUMBER_OF_NODES:
        logging.info(f"exit: number of offline nodes greater then two-thirds total nodes")
        exit()

    simulator.run()

    sum_message = 0
    common_finalized_blocks = simulator.nodes[0].finalized
    for i in range(TOTAL_NUMBER_OF_NODES):
        node = simulator.nodes[i]
        sum_message = sum_message + len(node.all_messages_send)
        common_finalized_blocks = common_finalized_blocks.intersection(node.finalized)
        logging.info(f" node {i}, num of blocks finalized:  {len(node.finalized)}, "
                     f"num of blocks notarized:  {len(node.notarized)}, "
                     f"unique message rx. count: {node.unique_message_received_count_for_finalization}, "
                     f"total message rx. count: {node.all_messages_received_count_for_finalization},"
                     f" total message send: {len(node.all_messages_send)}, "
                     f"==> total send message count from queue: {len(node.messages_send)} ")

    logging.info(f"For N = {TOTAL_NUMBER_OF_NODES} ,total messages send: {sum_message}, "
                 f"finalized blocks: {len(common_finalized_blocks)} ")

    # f"total message send: {node.all_messages_send}",
    # f"unique message send: {node.messages_send}")

    '''
    for mess in simulator.nodes[4].all_messages_send:
        logging.info(f"node 5 message: {mess}")
        pass
    '''

    now2 = datetime.now()
    ct2 = now2.strftime("%H:%M:%S")
    logging.info(f"end_time: {ct2}")
