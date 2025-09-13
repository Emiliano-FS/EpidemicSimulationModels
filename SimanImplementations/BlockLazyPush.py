from Blockchain import * 
from simian import Simian
import random, math, argparse

parser = argparse.ArgumentParser(
    description='Simple Gossip Protocol Simulation.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int, 
                    help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float, 
                    help='simulation end time')
parser.add_argument('--seedR', type=int, metavar='SEED', default=10, 
                    help='seed for random')
#parser.add_argument("-d", "--distance", type=float, metavar='DISTANCE', default=100.0, 
#                    help="maximum distance to be neighbors")
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1, 
                    help="min delay of mailboxes")
parser.add_argument("-r", "--maxrounds", type=int, default=math.inf, 
                    help="maximum number of times a message is retransmitted by nodes")
parser.add_argument("-f", "--fanout", type=int, default=5, 
                    help="number of nodes that are selected as gossip targets")
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0, 
                    help="use mpi")
parser.add_argument("--activeChurn", type=int, metavar='CHURN', default=0,
                    help="activates the network churn-> default 0")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0,
                    help="node fail rate [0.0 ... 1.0]")

args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True


nodes = args.total_nodes
lookahead = args.lookahead
maxrounds = args.maxrounds
fanout = args.fanout
random.seed(args.seedR)
churn = args.activeChurn
failRate = args.failRate

downNodes = []
upNodes = []

name = "LazyPushSimulation/"+"LazyPush"+ str(args.total_nodes)+'-Seed'+str(args.seedR)+'-LOOKAHEAD'+str(args.lookahead)+'-CHURN'+str(args.activeChurn)+'-FAILRATE'+str(args.failRate)
simName, startTime, endTime, minDelay, useMPI, mpiLib = name, 0, args.endtime, 0.00001, uMPI, "/usr/lib/x86_64-linux-gnu/libmpich.so"
simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)


class msg2:
    def __init__(self,type,m,mID,hops,sender):
        self.type = type
        self.payload = m
        self.ID = mID
        self.hops = hops
        self.sender = int(sender)

class msgReport:
    def __init__(self, msgs, degree, miner, length, trx):
        self.msgsFromNode = msgs
        self.degree = degree
        self.isMiner = miner
        self.lengthChainFromNode = length
        self.trxFromNode = trx

class ChurnManager(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ChurnManager, self).__init__(baseInfo)
        self.reqService(300, "do_churn_cycle", "none")

    def do_churn_cycle(self, *args):
        churn_size = int(nodes * failRate)
        # Churn out
        if len(upNodes) > churn_size:
            to_churn = random.sample(upNodes, churn_size)
            for node_id in to_churn:
                delay = random.expovariate(1/20)
                self.reqService(delay, "force_churn_out", "", "Node", node_id)

class ReportNode(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super().__init__(baseInfo)
        # Per-message metrics
        self.reliability = {}      # number of nodes that received each message
        self.latency = {}          # max hops per message
        self.redundancy = {}       # [gossip, inv, requests] per message

        # Node-level aggregates
        self.degree_sum = 0
        self.totalMiners = 0
        self.totalTransactions = 0
        self.longestChain = 0
        self.chainLengths = 0
        self.maxDegree = 0
        self.minDegree = float('inf')
        self.shortestPathSum = 0
        self.servedNodes = 0

        # Schedule the final report
        self.reqService(endTime, "PrintSystemReport", "none")

    def SystemReport(self, *args):
        report = args[0]
        msgs = report.msgsFromNode
        trx = report.trxFromNode
        chain = report.lengthChainFromNode
        isMiner = report.isMiner
        degree = report.degree

        self.servedNodes += 1

        # Update degree stats
        self.degree_sum += degree
        self.maxDegree = max(self.maxDegree, degree)
        self.minDegree = min(self.minDegree, degree)

        # Process each message
        for m in msgs:
            msg_id = m['id']
            hops = m['hopsDone']
            self.shortestPathSum += hops

            # Latency: max hops for this message
            if msg_id not in self.latency or hops > self.latency[msg_id]:
                self.latency[msg_id] = hops

            # Reliability: count of nodes that received this message
            self.reliability[msg_id] = self.reliability.get(msg_id, 0) + 1

            # Redundancy sums
            if msg_id not in self.redundancy:
                self.redundancy[msg_id] = [0, 0, 0]
            self.redundancy[msg_id][0] += m['gossip']
            self.redundancy[msg_id][1] += m['inv']
            self.redundancy[msg_id][2] += m['requests']

        # Aggregate transactions
        self.totalTransactions += trx

        # Track longest chain
        if chain > self.longestChain:
            self.longestChain = chain
        self.chainLengths += chain

        # Count miners
        if isMiner:
            self.totalMiners += 1

    def PrintSystemReport(self, *args):
        node_count = nodes * (1 - failRate)
        avg_degree = round(self.degree_sum / node_count, 2) if node_count > 0 else 0

        total_msgs = len(self.reliability)
        if total_msgs == 0:
            self.out.write("No messages received.\n")
            return

        # Initialize aggregates
        avRel = avNodes = avLat = avRmr = avGossip = avInv = avReq = 0

        # Per-message report
        for msg_id in sorted(self.reliability.keys()):
            received_count = self.reliability[msg_id]
            reliability_pct = round(received_count / node_count * 100, 3)
            latency = self.latency[msg_id]
            rmr = round((self.redundancy[msg_id][0] / (received_count - 1)) - 1, 3) if received_count > 1 else 0

            # Sum for averages
            avRel += reliability_pct
            avNodes += received_count
            avLat += latency
            avRmr += rmr
            avGossip += self.redundancy[msg_id][0]
            avInv += self.redundancy[msg_id][1]
            avReq += self.redundancy[msg_id][2]

            self.out.write(f"{msg_id} -- Reliability:{reliability_pct}%  Nodes:{received_count}  "
                           f"Latency:{latency}  RMR:{rmr}  Gossip:{self.redundancy[msg_id][0]}  "
                           f"Inv:{self.redundancy[msg_id][1]}  Requests:{self.redundancy[msg_id][2]}\n")

        # Compute averages
        avRel /= total_msgs
        avNodes /= total_msgs
        avLat /= total_msgs
        avRmr /= total_msgs
        avGossip /= total_msgs
        avInv /= total_msgs
        avReq /= total_msgs
        avg_shortest_path = self.shortestPathSum / (total_msgs * node_count)
        avg_chain_len_pct = (self.chainLengths / node_count) / self.longestChain * 100 if self.longestChain else 0

        # Print summary
        self.out.write(f"\nNumber of Miners: {self.totalMiners}  Total Transactions: {self.totalTransactions}  "
                       f"Longest Chain: {self.longestChain}  Avg Chain Length: {avg_chain_len_pct:.2f}%\n")
        self.out.write(f"AVERAGE -- Reliability:{avRel:.3f}%  Nodes:{avNodes:.1f}  Latency:{avLat:.1f}  "
                       f"RMR:{avRmr:.3f}  Gossip:{avGossip:.1f}  Inv:{avInv:.1f}  Requests:{avReq:.1f}\n")
        self.out.write(f"Degree Avg:{avg_degree}  Min:{self.minDegree}  Max:{self.maxDegree}  "
                       f"Shortest Path Avg:{avg_shortest_path:.2f}\n")

total_seeders = [1,2,3]

class Seeder(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Seeder, self).__init__(baseInfo)

        self.sid = args[0]  # seeder ID

        # --- biased region for this seeder ---
        region_size = nodes // len(total_seeders)
        region_start = self.sid * region_size
        region_end = (self.sid + 1) * region_size
        bias_pool = list(range(region_start, region_end))

        # --- sample nodes ---
        # half from its own region, half from the full network
        bias_sample = random.sample(
            bias_pool, int(0.15 * nodes)
        )
        global_sample = random.sample(
            range(nodes), int(0.15 * nodes)
        )

        # final known_nodes = ~30% of the network
        self.known_nodes = list(set(bias_sample + global_sample))

    def PeerRequest(self, *args):
        requester_id = args[0]
        candidates = [p for p in self.known_nodes if p != requester_id]
        sample_size = min(30, len(candidates))
        peers = random.sample(candidates, sample_size)
        self.reqService(lookahead, "ReceivePeers", peers, "Node", requester_id)

class Node(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Node, self).__init__(baseInfo)
        self.node_idx = args[0]
        self.total_nodes = args[1]
        self.miner = False
        self.trxMade = 0
        self.active = True
        self.blockchain = Blockchain()
        self.blockchain.create_genesis_block()
        self.mining = False

        self.known_peers = []
        self.outpeers = []
        self.receivedMsgs = {}
        self.report = {}

        JOIN_WINDOW = (0, 200)
        self.reqService(random.uniform(*JOIN_WINDOW), "Bootstrap", "none")
        self.reqService(endTime - 1, "TriggerSystemReport", "none")

    def Bootstrap(self, *args):
        #print(f"Node {self.node_idx} is bootstrapping. and knows {len(self.known_peers)} peers.")
        if len(self.known_peers) > 8:
            self.UpdateActivePeers()
            self.reqService(80, "RefreshPeers", "none")
        else:
            self.reqService(lookahead, "PeerRequest", self.node_idx, "Seeder", random.choice(total_seeders))
            self.reqService(80, "RefreshPeers", "none")

    def ReceivePeers(self, *args):
        peer_list = args[0]
        for p in peer_list:
            if p not in self.known_peers and p != self.node_idx:
                self.known_peers.append(p)
        self.UpdateActivePeers()

    def UpdateActivePeers(self):
        if len(self.outpeers) < 8 and len(self.known_peers) > 0:
            additional_peers = random.sample(self.known_peers, min(8 - len(self.outpeers), len(self.known_peers)))
            self.outpeers.extend(additional_peers)
            self.known_peers = [x for x in self.known_peers if x not in self.outpeers]
            for peer in additional_peers:
                handshakePeers = random.sample(self.known_peers, min(15, len(self.known_peers))) if self.known_peers else []
                self.reqService(lookahead, "HandShake", (self.node_idx, handshakePeers), "Node", peer)

    def HandShake(self, *args):
        payload = args[0]
        peer_id = payload[0]
        receivedPeers = payload[1]
        for p in receivedPeers:
            if p not in self.known_peers and p != self.node_idx:
                self.known_peers.append(p)
        self.known_peers.append(peer_id)
        handshakePeers = random.sample(self.known_peers, min(15, len(self.known_peers))) if self.known_peers else []
        self.reqService(lookahead, "HandShakeResponse", handshakePeers, "Node", peer_id)

    def HandShakeResponse(self, *args):
        handshakePeers = args[0]
        for p in handshakePeers:
            if p not in self.known_peers and p != self.node_idx:
                self.known_peers.append(p)
        #print(f"Node {self.node_idx} now knows {len(self.known_peers)} peers after handshake.")

    def RefreshPeers(self, *args):
        if not self.active:
            return
        if len(self.outpeers) < 8:
            self.UpdateActivePeers()
        else:
            proactiveNode = self.known_peers[-1] 
            peerToReplace = random.choice(self.outpeers)
            self.known_peers.remove(proactiveNode)
            self.outpeers.remove(peerToReplace)
            self.outpeers.append(proactiveNode)
            handshakePeers = random.sample(self.known_peers, min(15, len(self.known_peers))) if self.known_peers else []
            self.reqService(lookahead, "HandShake", (self.node_idx, handshakePeers), "Node", proactiveNode)
        self.reqService(80, "RefreshPeers", "none")


    def Receive(self, *args):
        if not self.active:
            return
        msg = args[0]

        if msg.ID not in self.report.keys():
            self.report[msg.ID] = [1,0,0]
        else:
            self.report[msg.ID][0] += 1
    
        if msg.ID not in self.receivedMsgs.keys():
            self.receivedMsgs[msg.ID] = msg

            if msg.type  == "BLOCK":
                block = Block.from_dict(msg.payload)
                accepted = self.blockchain.consensus(block)
                if accepted:
                    self.blockchain.remove_confirmed_transactions(block)
                    if self.mining:
                        self.mining = False

                self.SendInv(msg.ID)

            elif msg.type  == "TRX":
                if self.miner:
                    self.blockchain.add_new_transaction(msg.payload)
                    if self.miner and not self.mining and len(self.blockchain.unconfirmed_transactions) >= 100:
                        self.mining = True
                        avg_mining_time = 30
                        delay = random.expovariate(1/avg_mining_time)
                        self.reqService(delay, "mine_block", "none")

                self.SendInv(msg.ID)


    def mine_block(self, *args):
        if self.active and self.mining:
            self.blockchain.mine()
            new_block = self.blockchain.last_block
            block_id = "B-" +str(random.randint(11111111,99999999))
            block_msg = msg2("BLOCK", new_block.to_dict(), block_id, 0, self.node_idx)
            self.receivedMsgs[block_msg.ID] = block_msg
            self.report[block_msg.ID] = [1, 0, 0]
            self.SendInv(block_msg.ID)
            self.mining = False


    def SendInv(self, msg_id):
        for peer in self.outpeers:
            self.reqService(lookahead, "ReceiveInv", (msg_id, self.node_idx), "Node", peer)

    def ReceiveInv(self, *args):
        if not self.active:
            return
        msg_id, sender_id = args[0]

        if msg_id not in self.report.keys():
            self.report[msg_id] = [0,1,0]
        else:
            self.report[msg_id][1] += 1

        if msg_id not in self.receivedMsgs:
            self.reqService(lookahead, "RequestMessage", (msg_id, self.node_idx), "Node", sender_id)

    def RequestMessage(self, *args):
        msg_id, requester_id = args[0]

        if msg_id not in self.report.keys():
            self.report[msg_id] = [0,0,1]
        else:
            self.report[msg_id][2] += 1

        if msg_id in self.receivedMsgs:
            msg = self.receivedMsgs[msg_id]
            self.reqService(lookahead, "Receive", msg2(msg.type , msg.payload , msg.ID, msg.hops + 1 , msg.sender), "Node", requester_id)
    

    def TriggerSystemReport(self,*args):
        if self.active:
            messageMetrics = []
            degree = len(self.outpeers)
            for m in self.receivedMsgs.keys():
                messageMetrics.append( {
                                        'id': m,
                                        'hopsDone': self.receivedMsgs[m].hops,
                                        'gossip': self.report[m][0],
                                        'inv': self.report[m][1],
                                        'requests': self.report[m][2]
                                        })

            msgToSend = msgReport(messageMetrics, degree, self.miner, len(self.blockchain.chain), self.trxMade)
            self.reqService(lookahead, "SystemReport", msgToSend , "ReportNode", 0)

    def BecomeMiner(self, *args):
        self.miner = True  

    def force_churn_out(self, *args):
        if self.active:
            self.active = False
            self.peers = []
            self.blockchain.chain = []
            self.blockchain.forks = {}
            self.blockchain.orphans = []
            self.receivedMsgs = {}
            self.report = {}

            if self.node_idx in upNodes:
                upNodes.remove(self.node_idx)
            if self.node_idx not in downNodes:
                downNodes.append(self.node_idx)
        print(f"Node {self.node_idx} has gone down.")

    def create_transaction(self,*args):
        n = random.choice(upNodes)
        avg_transactionT = 1.4
        delay = random.expovariate(1/avg_transactionT)
        if self.active and not self.miner:
            transaction = Transaction(self.node_idx)
            tx_msg = msg2("TRX", transaction, transaction.trans_id, 0, self.node_idx)
            self.reqService(lookahead, "Receive", tx_msg, "Node", self.node_idx)
            self.trxMade += 1
            self.reqService(delay , "create_transaction", "" , "Node", n)
        else:
            self.reqService(delay , "create_transaction", "" , "Node", n)


SEEDER_COUNT = 3
total_seeders = list(range(SEEDER_COUNT))

for s in total_seeders:
    simianEngine.addEntity("Seeder", Seeder, s, s)

for i in range(0, nodes):
    simianEngine.addEntity("Node", Node, i, i, nodes)

simianEngine.addEntity("ReportNode", ReportNode, 0, 0)

if churn:
    simianEngine.addEntity("ChurnManager", ChurnManager, 0, 0)

for i in range(0, nodes):
    upNodes.append(i)

for i in range(0, math.ceil((0.01 * nodes))):
    n = random.choice(upNodes)
    simianEngine.schedService(lookahead, "BecomeMiner", "", "Node", n)
    upNodes.remove(n)

n = random.choice(upNodes)

simianEngine.schedService(250 + lookahead , "create_transaction","" , "Node", n)


simianEngine.run()
simianEngine.exit()