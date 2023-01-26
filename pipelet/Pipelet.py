from __future__ import annotations
from datetime import datetime
import logging

formatter = "%(message)s"
logging.basicConfig(encoding='utf-8', level=logging.INFO, format=formatter)

from Simulator import *

DELTA = 1
SECOND = 6 * DELTA
MINUTE = 30 * DELTA
EPOCH_LEADER = {}
# EPOCH_TIMER = 0
TOTAL_NUMBER_OF_NODES = 12
OFFLINE_NODES = 0

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
    def __init__(self, block: Block, vote_count: set):
        super().__init__()
        self.block = block
        self.vote_count = vote_count.union()


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
    def __init__(self, set_of_blocks: set, vote_dict: dict):
        super().__init__()
        # self.vote_for_longest_block = vote_for_longest_block.union()
        self.chains = set()
        self.vote_dict = {}
        for block in set_of_blocks:
            self.chains.add(block)
        for key in vote_dict:
            self.vote_dict[key] = vote_dict[key].union()


class PipeletNode(Node):
    def __init__(self, simulator: Simulator, ID: int, active: bool):
        super().__init__(simulator, ID)
        self.all_messages_received_count_for_finalization = 0
        self.all_messages_received_set = set()
        self.unique_message_received_set = set()
        self.all_messages_received_list = []
        self.messages_send = set()
        self.all_messages_send = []

        self.seq = -1
        self.EPOCH_TIMER = -1
        self.notarized = set([GENESIS])
        self.finalized = set()
        self.votes = {}
        self.hasVoted = set()
        self.epoch = 0
        self.longest_chain_set = set([GENESIS])
        self.sync_block_set = set([GENESIS])
        self.clock_message_count = {self.epoch + 1: 1}
        self.clock_message_set = {self.epoch + 1: set()}
        # to initiate the protocol Clock Message for next epoch added to event queue
        if active:
            self.timeout(MINUTE, Clock(self.epoch + 1))

    def initialize_vote_set(self):
        self.votes[GENESIS] = set()
        for node_item in self.simulator.nodes:
            self.votes[GENESIS].add(node_item)

    def update_clock_message_count(self, epoch: int):
        if len(self.clock_message_set[epoch]) >= 2 / 3 * len(self.simulator.nodes):
            self.clock_message_count[epoch] = 1

    def is_leader(self, epoch: int):
        return self.ID == epoch % len(self.simulator.nodes)

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
                while temp_block is not GENESIS:
                    if temp_block in self.longest_chain_set:
                        break
                    else:
                        self.longest_chain_set.add(temp_block)
                        temp_block = temp_block.parent

    def add_votes(self, block, sender):
        if block not in self.votes:
            self.votes[block] = set()
        self.votes[block].add(sender)

    def reset_next_epoch(self):
        for event in self.simulator.queue:
            if (self.ID == event.recipient.ID == event.sender.ID == event.author.ID) and type(
                    event.message) == NextEpoch:
                self.simulator.queue.remove(event)
                self.timeout(MINUTE, NextEpoch())
                # logging.info(f"func- reset NextEpoch for node {self.ID}")
                break

    def finalize(self, notarized_block: Block):
        if notarized_block.length >= 3:
            blk_2 = notarized_block.parent
            blk_1 = blk_2.parent
            if blk_2 in self.notarized and blk_1 in self.notarized:
                blk_set_left = self.notarized.difference(set([notarized_block]))
                blk_set_left = blk_set_left.difference(self.finalized)
                if len(blk_set_left) > 0:
                    self.finalized = self.finalized.union(blk_set_left)
                    self.all_messages_received_count_for_finalization = len(self.all_messages_received_set)

    def count_vote(self, block: Block):
        # global un_notarized_block
        condition_1 = len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes)
        condition_2 = block not in self.notarized
        if condition_1 and condition_2:
            self.notarized.add(block)
            self.finalize(block)
            # logging.info(f"{self} notarized {block}, self-epoch : {self.epoch}, block-epoch: {block.epoch}")
            if self.epoch == block.epoch:
                # reset Nextepoch() upon block notarization
                self.reset_next_epoch()
                if self.is_leader(self.epoch) and self.EPOCH_TIMER == 1:
                    self.timeout(0, InitiateLeader())

    def clock_logic(self, message: Clock, sender: Node):
        longest_notarized_block_length = self.longest_notarized().length
        self.update_longest_set(longest_notarized_block_length)
        block_set = self.longest_chain_set.difference(self.sync_block_set)
        temp_dict_for_votes = {}
        for block in block_set:
            temp_dict_for_votes[block] = self.votes[block].union()
        # broadcast sync blocks
        if len(block_set) > 0:
            self.broadcast(Sync(block_set, temp_dict_for_votes))
            self.sync_block_set = self.longest_chain_set.union(self.sync_block_set)
            # logging.info(f"{self} broadcast SYNC message")
        if message.epoch > self.epoch:
            if self.epoch > 0:
                self.clock_message_count[message.epoch] = 0
            # add clock
            if message.epoch not in self.clock_message_set:
                self.clock_message_set[message.epoch] = set()
            self.clock_message_set[message.epoch].add((sender.ID, self.ID))
            self.update_clock_message_count(message.epoch)
            if self.clock_message_count[message.epoch] == 1:
                # increment epoch and reset NextEpoch()
                self.epoch = message.epoch
                if self.epoch == 1:
                    self.timeout(MINUTE, NextEpoch())
                else:
                    self.reset_next_epoch()
                # logging.info(f"{self} moves to {self.epoch}")
                self.broadcast(message)

                if self.is_leader(self.epoch):
                    self.seq = 1
                    self.EPOCH_TIMER = 0
                    EPOCH_LEADER[self.epoch] = self
                    self.timeout(SECOND, InitiateLeader())

    def receive(self, message: Message, sender: Node):
        global EPOCH_LEADER
        # logging.info(f" Event triggered - recipient: Node{self}, sender: Node{sender}, message: {message}")
        self.all_messages_received_set.add(message)
        self.messages_send.add(message)
        sender.all_messages_send.append(message)
        message_type = type(message)
        if message_type == NextEpoch:
            clock_message = Clock(self.epoch + 1)
            self.clock_logic(clock_message, self)
            self.broadcast(clock_message)
            # logging.info(f"{self} broadcast clock for epoch {self.epoch + 1}")
            self.timeout(MINUTE, NextEpoch())
            # logging.info(f"reset NextEpoch for node {self.ID}")
        elif message_type == Sync:
            previous_longest_notarized_block = self.longest_notarized()
            # will go away just for debugging
            debug_diff_blocks = message.chains.difference(self.notarized)
            self.notarized = self.notarized.union(message.chains)
            for key in message.vote_dict:
                if key in self.votes:
                    self.votes[key] = self.votes[key].union(message.vote_dict[key])
                else:
                    self.votes[key] = message.vote_dict[key].union()
            new_longest_notarized_block = self.longest_notarized()
            # debug purpose
            '''
            debug_diff_blocks_list = []
            for debug_item in debug_diff_blocks:
                debug_diff_blocks_list.append(f"{debug_item}")
            
            logging.info(f"{self} in SYNC section: {debug_list}, after SYNC longest blk: {new_longest_notarized_block}"
                         f" at length: {new_longest_notarized_block.length}, before SYNC longest blk: "
                         f"{previous_longest_notarized_block} at length {previous_longest_notarized_block.length}")
            '''
            if new_longest_notarized_block.length > previous_longest_notarized_block.length:
                '''
                # debug purpose
                logging.info(
                    f"{self} SYNC notarized {debug_diff_blocks_list}, self-epoch : {self.epoch} ")
                '''
                for block in debug_diff_blocks:
                    self.finalize(block)
                self.reset_next_epoch()

        elif message_type == Clock:
            self.clock_logic(message, sender)
        elif message_type == InitiateLeader and self.is_leader(self.epoch):
            if self.EPOCH_TIMER == 0:
                self.EPOCH_TIMER = 1
            block = Block(self.longest_notarized(), self.epoch, self.seq)
            self.seq += 1

            vote_count_for_parent_of_proposed_block = set()
            if block.parent is GENESIS:
                for node_item in self.simulator.nodes:
                    vote_count_for_parent_of_proposed_block.add(node_item)
            else:
                vote_count_for_parent_of_proposed_block = self.votes[block.parent].union()

            message = Proposal(block, vote_count_for_parent_of_proposed_block)
            message_vote = Vote(block)
            self.add_votes(message_vote.block, self)
            self.count_vote(message_vote.block)
            self.broadcast(message)
            '''
            logging.info(
                f"Epoch {self.epoch}: {self} is leader and broadcasts block: {block} block proposal (length: "
                f"{block.length})")
            '''
        elif message_type == Proposal:
            proposed_block = message.block
            # logging.info(f"{self} received proposed block {proposed_block}")
            self.votes[proposed_block.parent] = message.vote_count.union()
            # last block of the longest notarized chain
            if len(self.votes[proposed_block.parent]) >= 2 / 3 * len(self.simulator.nodes):
                previous_longest_block = self.longest_notarized()
                '''
                # just for debug, it will go away later
                if proposed_block.parent not in self.notarized:
                    logging.info(
                        f"{self} notarized {proposed_block.parent}, self-epoch : {self.epoch}, block-epoch: "
                        f"{proposed_block.parent.epoch}")
                '''
                self.notarized.add(proposed_block.parent)
                self.finalize(proposed_block.parent)
                current_longest_block = self.longest_notarized()
                genesis_block_test = current_longest_block == previous_longest_block == GENESIS
                if (current_longest_block.length > previous_longest_block.length) or genesis_block_test:
                    self.reset_next_epoch()
                condition_1 = ((proposed_block.epoch == self.epoch) and sender.is_leader(
                    self.epoch) and proposed_block not in self.hasVoted)
                condition_2 = (
                        (
                                proposed_block.length == current_longest_block.length + 1) and proposed_block.parent in
                        self.notarized)
                if condition_1 and condition_2:
                    message = Vote(proposed_block)
                    # self.broadcast(message)
                    self.send(EPOCH_LEADER[self.epoch], message)
                    self.hasVoted.add(proposed_block)
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

    for i in range(TOTAL_NUMBER_OF_NODES):
        if i < TOTAL_NUMBER_OF_NODES - OFFLINE_NODES:
            simulator.nodes.append(PipeletNode(simulator, i, True))
        else:
            simulator.nodes.append(PipeletNode(simulator, i, False))
            simulator.offline_nodes.add(simulator.nodes[i])
    simulator.run()

    if len(simulator.offline_nodes) >= (2 / 3) * TOTAL_NUMBER_OF_NODES:
        logging.info(f"exit: number of offline nodes greater then two-thirds total nodes")
        exit()
    simulator.run()
    for i in range(TOTAL_NUMBER_OF_NODES):
        node = simulator.nodes[i]
        logging.info(f" node {i}, num of blocks finalized:  {len(simulator.nodes[i].finalized)}, "
                     f"num of blocks notarized:  {len(simulator.nodes[i].notarized)}, "
                     f"total message count: {node.all_messages_received_count_for_finalization},"
                     f" total message send: {len(node.all_messages_send)}")

    now2 = datetime.now()
    ct2 = now2.strftime("%H:%M:%S")
    logging.info(f"end_time: {ct2}")