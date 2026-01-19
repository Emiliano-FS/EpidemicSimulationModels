from hashlib import sha256
import random
import json
import time


class Transaction:
    def __init__(self, author):
        self.trans_id = "T-" + str(random.randint(11111111, 99999999))
        self.author = author
        self.timestamp = time.time()
        self.content = (
            "Transaccion de "
            + str(self.author)
            + " con numero "
            + self.trans_id
            + " con tiempo "
            + str(self.timestamp)
        )

    def __str__(self):
        return f"Transaction(trans_id={self.trans_id}, author={self.author}, timestamp={self.timestamp} , content={self.content})"

    def to_dict(self):
        return {
            "trans_id": self.trans_id,
            "author": self.author,
            "timestamp": self.timestamp,
            "content": self.content,
        }

    @classmethod
    def from_dict(cls, data):
        tx = cls(data["author"])
        tx.trans_id = data["trans_id"]
        tx.timestamp = data["timestamp"]
        tx.content = data["content"]
        return tx


def compute_merkle_root(transactions):
    def _hash(data):
        return sha256(data.encode("utf-8")).hexdigest()

    if not transactions:
        return _hash("")

    layer = [
        _hash(json.dumps(tx.to_dict(), sort_keys=True)) for tx in transactions
    ]
    while len(layer) > 1:
        if len(layer) % 2 == 1:  # duplicate last if odd
            layer.append(layer[-1])
        layer = [_hash(layer[i] + layer[i + 1]) for i in range(0, len(layer), 2)]
    return layer[0]


class Block:
    def __init__(self, index, transactions, timestamp, previous_hash, nonce=0):
        self.index = index
        self.transactions = transactions
        self.timestamp = timestamp
        self.previous_hash = previous_hash
        self.nonce = nonce
        self.merkle_root = compute_merkle_root(transactions)

    def to_dict(self):
        return {
            "index": self.index,
            "transactions": [tx.__dict__ for tx in self.transactions],
            "timestamp": self.timestamp,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "merkle_root": self.merkle_root,
            "hash": getattr(self, "hash", None),
        }

    @classmethod
    def from_dict(cls, data):
        block = cls(
            index=data["index"],
            transactions=[Transaction.from_dict(tx) for tx in data["transactions"]],
            timestamp=data["timestamp"],
            previous_hash=data["previous_hash"],
            nonce=data["nonce"],
        )
        block.merkle_root = data.get("merkle_root")
        block.hash = data.get("hash")
        return block

    def compute_hash(self):
        block_content = {
            "index": self.index,
            "timestamp": self.timestamp,
            "previous_hash": self.previous_hash,
            "nonce": self.nonce,
            "merkle_root": self.merkle_root,
        }
        block_string = json.dumps(block_content, sort_keys=True)
        return sha256(block_string.encode()).hexdigest()

    def __str__(self):
        return f"BLOCK (ID = {self.index}, TRX={str(len(self.transactions))}, timestamp={self.timestamp} , PREVIHASH={self.previous_hash}, MERKLE={self.merkle_root[:8]}...)"

    def __repr__(self):
        return self.__str__()


class Blockchain:
    difficulty = 1

    def __init__(self):
        self.unconfirmed_transactions = []
        self.blocks = {}          # hash -> Block
        self.children = {}        # hash -> list of child hashes
        self.heights = {}         # hash -> height
        self.head = None          # hash of best chain tip
        self.chain = []
        self.orphans = []

    def create_genesis_block(self):
        genesis_block = Block(0, [], 0, "0")
        genesis_block.hash = genesis_block.compute_hash()
        self.chain.append(genesis_block)

    @property
    def last_block(self):
        return self.chain[-1]

    def block_validity(self, block):
        if not Blockchain.is_valid_proof(block, block.hash):
            return False
    
        if block.index == 0:
            return True
    
        if block.previous_hash not in self.blocks:
            return False
    
        parent = self.blocks[block.previous_hash]
        return block.index == parent.index + 1

    def add_block(self, block):
        self.chain.append(block)
        return True

    @staticmethod
    def proof_of_work(block):
        block.nonce = 0
        computed_hash = block.compute_hash()
        while not computed_hash.startswith("0" * Blockchain.difficulty):
            block.nonce += 1
            computed_hash = block.compute_hash()
        return computed_hash

    def add_new_transaction(self, transaction):
        self.unconfirmed_transactions.append(transaction)

    @classmethod
    def is_valid_proof(cls, block, block_hash):
        return block_hash.startswith("0" * Blockchain.difficulty) and block_hash == block.compute_hash()

    @classmethod
    def check_chain_validity(cls, chain):
        result = True
        previous_hash = "0"

        for block in chain:
            reconstructed = Block(
                index=block.index,
                transactions=block.transactions,
                timestamp=block.timestamp,
                previous_hash=block.previous_hash,
                nonce=block.nonce,
            )
            reconstructed.merkle_root = block.merkle_root
            computed_hash = reconstructed.compute_hash()

            if computed_hash != block.hash or previous_hash != block.previous_hash:
                result = False
                break

            previous_hash = block.hash

        return result

    def mine(self):
        if not self.unconfirmed_transactions:
            return False

        last_block = self.last_block

        new_block = Block(
            index=last_block.index + 1,
            transactions=self.unconfirmed_transactions[:100],
            timestamp=time.time(),
            previous_hash=last_block.hash,
        )

        proof = self.proof_of_work(new_block)

        if Blockchain.is_valid_proof(new_block, proof):
            new_block.hash = proof
            self.add_block(new_block)
            self.unconfirmed_transactions = self.unconfirmed_transactions[100:]
            return True

        return False

    def consensus(self, block):
        # Reject invalid blocks
        if not self.block_validity(block):
            return False

        # Already known
        if block.hash in self.blocks:
            return False

        # Store block
        self.blocks[block.hash] = block
        self.children.setdefault(block.previous_hash, []).append(block.hash)

        # Genesis
        if block.index == 0:
            self.heights[block.hash] = 0
            self.head = block.hash
            self.chain = [block]
            return True

        # Orphan
        if block.previous_hash not in self.blocks:
            self.orphans.append(block)
            return False

        # Normal block
        self.heights[block.hash] = self.heights[block.previous_hash] + 1

        # Update best chain
        if self.head is None or self.heights[block.hash] > self.heights[self.head]:
            self.head = block.hash
            self._rebuild_chain()

        return True

    def _rebuild_chain(self):
        chain = []
        current = self.head

        while current:
            block = self.blocks[current]
            chain.append(block)
            current = block.previous_hash

        self.chain = list(reversed(chain))


    def remove_confirmed_transactions(self, block):
        confirmed_ids = {tx.tx_id for tx in block.transactions}
        self.current_transactions = [
            tx for tx in self.current_transactions
            if tx.tx_id not in confirmed_ids
        ]
