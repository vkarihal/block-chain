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
# seq = -1
# keeps track of current epoch's leader
EPOCH_LEADER = {}
# EPOCH_TIMER = 0
TOTAL_NUMBER_OF_NODES = 5


# PROPOSED_BLOCK_RECEIVED = set()
# un_notarized_block = {}


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
    def __init__(self, simulator: Simulator, ID: int):
        super().__init__(simulator, ID)
        self.seq = -1
        self.EPOCH_TIMER = -1
        self.notarized = set([GENESIS])
        self.votes = {}
        self.hasVoted = set()
        # self.initialize_vote_set()
        self.epoch = 0
        self.longest_chain_set = set([GENESIS])
        self.sync_block_set = set([GENESIS])
        # test = set()
        self.clock_message_count = {self.epoch + 1: 1}
        self.clock_message_set = {self.epoch + 1: set()}
        # to initiate the protocol Clock Message for next epoch added to event queue
        self.timeout(MINUTE, Clock(self.epoch + 1))

    def initialize_vote_set(self):
        self.votes[GENESIS] = set()
        for node in self.simulator.nodes:
            self.votes[GENESIS].add(node)

    def update_clock_message_count(self, epoch: int):
        if len(self.clock_message_set[epoch]) >= 2 / 3 * len(self.simulator.nodes):
            self.clock_message_count[epoch] = 1

    def is_leader(self, epoch: int):
        # check if the node is the leader of an epoch

        # rand_hash = hash(str(epoch))
        # return self.ID == rand_hash % len(self.simulator.nodes)
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
                logging.info(f"func- reset NextEpoch for node {self.ID}")
                break

    def count_vote(self, block: Block):
        # global un_notarized_block
        condition_1 = len(self.votes[block]) >= 2 / 3 * len(self.simulator.nodes)
        condition_2 = block not in self.notarized
        # at times, it can happen that a node receives the votes required for notarization
        # but has not yet received the block
        # so only notarize when it has received the block
        # condition_3 = (self, block) in PROPOSED_BLOCK_RECEIVED
        # future work - i think condition_4 will go away for pipelet - but it does not create any harm right now
        condition_4 = block.parent not in self.notarized
        if condition_1 and condition_2:
            self.notarized.add(block)
            # this should go away for pipelet
            # self.remove_blocks(block)
            logging.info(f"{self} notarized {block}, self-epoch : {self.epoch}, block-epoch: {block.epoch}")
            if self.epoch == block.epoch:
                # reset Nextepoch() upon block notarization
                self.reset_next_epoch()
                '''
                for event in self.simulator.queue:
                    # reset Nextepoch() upon block notarization
                    self.reset_next_epoch()
                '''
                # if you have multiple nextepoch for the same epoch
                # self.timeout(MINUTE, NextEpoch())
                # logging.info(f"reset NextEpoch")
                # if EPOCH_LEADER[self.epoch].ID == self.ID and EPOCH_TIMER == 1:
                if self.is_leader(self.epoch) and self.EPOCH_TIMER == 1:
                    self.timeout(0, InitiateLeader())
            '''
            # future work - i think this entire condition_4 will go away for Pipelet. right now causes no harm
            if condition_4:
                # when parent block is un notarized
                # a un-notarized block should not get added to finalized chain
                if self not in un_notarized_block:
                    un_notarized_block[self] = [block.parent]
                else:
                    if block.parent not in un_notarized_block[self]:
                        un_notarized_block[self].append(block.parent)
            '''

    '''
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
    '''

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
            logging.info(f"{self} broadcast SYNC message")
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
                # self.timeout(MINUTE, NextEpoch())
                # self.reset_next_epoch()
                logging.info(f"{self} moves to {self.epoch}")
                self.broadcast(message)
                '''  
                if self.ID != sender.ID and message not in self.messages_seen:
                    self.broadcast(message)
                '''
                if self.is_leader(self.epoch):
                    self.seq = 1
                    self.EPOCH_TIMER = 0
                    EPOCH_LEADER[self.epoch] = self
                    self.timeout(SECOND, InitiateLeader())

    def receive(self, message: Message, sender: Node):
        global EPOCH_LEADER
        # global seq
        # global EPOCH_TIMER
        # logging.info(f" Event triggered - recipient: Node{self}, sender: Node{sender}, message: {message}")
        message_type = type(message)
        if message_type == NextEpoch:
            clock_message = Clock(self.epoch + 1)
            self.clock_logic(clock_message, self)
            self.broadcast(clock_message)
            logging.info(f"{self} broadcast clock for epoch {self.epoch + 1}")
            self.timeout(MINUTE, NextEpoch())
            logging.info(f"reset NextEpoch for node {self.ID}")
            # self.reset_next_epoch()
        elif message_type == Sync:
            # go away_debug
            '''
            n_p = []
            for d_b in self.notarized:
                n_p.append(f"{d_b}")
            logging.info(f"prev_notarized for {self}: {n_p}")
            '''

            previous_longest_notarized_block = self.longest_notarized()
            # will go away just for debugging
            debug_diff_blocks = message.chains.difference(self.notarized)

            '''
            if len(debug_diff_blocks) > 0:
                kl = {}
                try:
                    er = kl[10]
                except KeyError:
                    kl[1] = 'hi'
            '''

            self.notarized = self.notarized.union(message.chains)
            '''
            # debug purpose
            n_l = []
            for d_n in self.notarized:
                n_l.append(f"{d_n}")
            logging.info(f"new_notarized for {self}: {n_l}")
            '''

            for key in message.vote_dict:
                if key in self.votes:
                    self.votes[key] = self.votes[key].union(message.vote_dict[key])
                else:
                    self.votes[key] = message.vote_dict[key].union()
            new_longest_notarized_block = self.longest_notarized()
            # debug purpose

            debug_diff_blocks_list = []
            for debug_item in debug_diff_blocks:
                debug_diff_blocks_list.append(f"{debug_item}")
            '''
            logging.info(f"{self} in SYNC section: {debug_list}, after SYNC longest blk: {new_longest_notarized_block}"
                         f" at length: {new_longest_notarized_block.length}, before SYNC longest blk: "
                         f"{previous_longest_notarized_block} at length {previous_longest_notarized_block.length}")
            '''
            if new_longest_notarized_block.length > previous_longest_notarized_block.length:
                # debug purpose
                logging.info(
                    f"{self} SYNC notarized {debug_diff_blocks_list}, self-epoch : {self.epoch} ")
                self.reset_next_epoch()
        elif message_type == Clock:
            self.clock_logic(message, sender)
        elif message_type == InitiateLeader and self.is_leader(self.epoch):
            if self.EPOCH_TIMER == 0:
                self.EPOCH_TIMER = 1
            block = Block(self.longest_notarized(), self.epoch, self.seq)
            self.seq += 1
            # vote_count_for_parent_of_proposed_block = len(self.votes[block.parent])

            vote_count_for_parent_of_proposed_block = set()
            if block.parent is GENESIS:
                for node in self.simulator.nodes:
                    vote_count_for_parent_of_proposed_block.add(node)
                # vote_count_for_parent_of_proposed_block = TOTAL_NUMBER_OF_NODES
                '''
                if GENESIS not in self.votes:
                    self.votes[GENESIS] = set()
                for node in self.simulator.nodes:
                    self.votes[GENESIS].add(node)
                '''
            else:
                vote_count_for_parent_of_proposed_block = self.votes[block.parent].union()

            message = Proposal(block, vote_count_for_parent_of_proposed_block)
            message_vote = Vote(block)
            self.add_votes(message_vote.block, self)
            self.count_vote(message_vote.block)
            self.broadcast(message)

            # self.send(self, message_vote)
            # self.broadcast(message_vote)
            logging.info(
                f"Epoch {self.epoch}: {self} is leader and broadcasts block: {block} block proposal (length: "
                f"{block.length})")
            '''
            try:
                if self.ID == EPOCH_LEADER[self.epoch].ID:
                    if EPOCH_TIMER == 0:
                        EPOCH_TIMER = 1
                    block = Block(self.longest_notarized(), self.epoch, seq)
                    seq += 1
                    # vote_count_for_parent_of_proposed_block = len(self.votes[block.parent])

                    vote_count_for_parent_of_proposed_block = set()
                    if block.parent is GENESIS:
                        for node in self.simulator.nodes:
                            vote_count_for_parent_of_proposed_block.add(node)
                    else:
                        vote_count_for_parent_of_proposed_block = self.votes[block.parent].union()

                    message = Proposal(block, vote_count_for_parent_of_proposed_block)
                    self.broadcast(message)
                    message_vote = Vote(block)
                    self.send(self, message_vote)
                    # self.broadcast(message_vote)
                    logging.info(
                        f"Epoch {self.epoch}: {self} is leader and broadcasts block: {block} block proposal (length: "
                        f"{block.length})")
            except KeyError:
                # at times a leader of previous epoch gets a block notarized. hence it will lead previous epoch leader
                # to propose when it has already moved to a later epoch which has a different leader but the leader of
                # the later epoch is yet to enter the epoch, hence it raises key error
                logging.info(f" Key error caught: - recipient: {self}, sender: {sender}, message: {message}, key "
                             f"missing: {self.epoch}")
            '''

        elif message_type == Proposal:
            proposed_block = message.block
            logging.info(f"{self} received proposed block {proposed_block}")
            self.votes[proposed_block.parent] = message.vote_count.union()
            # last block of the longest notarized chain
            # previous_longest_block = self.longest_notarized()
            # check logic again- written in half sleep
            if len(self.votes[proposed_block.parent]) >= 2 / 3 * len(self.simulator.nodes):
                previous_longest_block = self.longest_notarized()
                # just for debug, it will go away later
                if proposed_block.parent not in self.notarized:
                    logging.info(
                        f"{self} notarized {proposed_block.parent}, self-epoch : {self.epoch}, block-epoch: "
                        f"{proposed_block.parent.epoch}")
                self.notarized.add(proposed_block.parent)
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
                    # i do not think we need to count proposer votes for proposer block notarization in here, they will
                    # be done later automatically when Vote event happens self.count_vote(proposed_block, self)
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
        simulator.nodes.append(PipeletNode(simulator, i))
    simulator.run()
