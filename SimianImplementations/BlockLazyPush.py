import report
from Blockchain import * 
from simian import Simian
import random, math, argparse
from collections import defaultdict

parser = argparse.ArgumentParser(
    description='Simple Gossip Protocol Simulation.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int, 
                    help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float, 
                    help='simulation end time')
parser.add_argument('--seedR', type=int, metavar='SEED', default=10, 
                    help='seed for random')
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1, 
                    help="min delay of mailboxes")
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
random.seed(args.seedR)
churn = args.activeChurn
failRate = args.failRate

seederTimer = 1.5

downNodes = []
upNodes = []
minerNodes = [] 

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
    def __init__(self, msgs, diss_degree, miner, length, trx,
                 node_id, neighbors, membership_msgs=None):
        self.msgsFromNode = msgs
        self.disseminationDegree = diss_degree
        self.isMiner = miner
        self.lengthChainFromNode = length
        self.trxFromNode = trx
        self.node_id = node_id
        self.neighbors = neighbors 
        self.membership_msgs = membership_msgs if membership_msgs else {}

class ChurnManager(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ChurnManager, self).__init__(baseInfo)
        self.reqService(300, "do_churn_cycle", "none")

    def do_churn_cycle(self, *args):
        churn_size = int(nodes * failRate)
        if len(upNodes) > churn_size:
            to_churn = random.sample(upNodes, churn_size)
            for node_id in to_churn:
                delay = random.expovariate(1/20)
                self.reqService(delay, "force_churn_out", "", "Node", node_id)

class ReportNode(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super().__init__(baseInfo)
        self.reliability = {}
        self.latency = {}
        self.redundancy = {}

        self.degree_sum = 0
        self.totalMiners = 0
        self.totalTransactions = 0
        self.longestChain = 0
        self.chainLengths = 0
        self.maxDegree = 0
        self.minDegree = float('inf')
        self.shortestPathSum = 0
        self.servedNodes = 0
        self.graph = {}

        # Membership message counters
        self.membership_msgs_sum = defaultdict(int)

        self.reqService(endTime, "PrintSystemReport", "none")

    def SystemReport(self, *args):
        report = args[0]
        msgs = report.msgsFromNode
        trx = report.trxFromNode
        chain = report.lengthChainFromNode
        isMiner = report.isMiner
        degree = report.disseminationDegree
        u = report.node_id
        Nu = set(report.neighbors)
        
        self.graph.setdefault(u, set()).update(Nu)
        
        for v in Nu:
            self.graph.setdefault(v, set()).add(u)

        self.servedNodes += 1

        self.degree_sum += degree
        self.maxDegree = max(self.maxDegree, degree)
        self.minDegree = min(self.minDegree, degree)

        # Aggregate membership messages
        if report.membership_msgs:
            for k, v in report.membership_msgs.items():
                self.membership_msgs_sum[k] += v

        for m in msgs:
            msg_id = m['id']
            hops = m['hopsDone']
            self.shortestPathSum += hops

            if msg_id not in self.latency or hops > self.latency[msg_id]:
                self.latency[msg_id] = hops

            self.reliability[msg_id] = self.reliability.get(msg_id, 0) + 1

            if msg_id not in self.redundancy:
                self.redundancy[msg_id] = [0, 0, 0]
            self.redundancy[msg_id][0] += m['gossip']
            self.redundancy[msg_id][1] += m['inv']
            self.redundancy[msg_id][2] += m['requests']

        self.totalTransactions += trx

        if chain > self.longestChain:
            self.longestChain = chain
        self.chainLengths += chain

        if isMiner:
            self.totalMiners += 1

    def compute_clustering(self):
         total = 0.0
         count = 0

         for u, Nu in self.graph.items():
             k = len(Nu)
             if k < 2:
                 continue

             links = 0
             for v in Nu:
                 if v not in self.graph:
                     continue
                 links += len(Nu & self.graph[v])

             links /= 2

             Ci = links / (k * (k - 1) / 2)
             total += Ci
             count += 1

         return total / count if count > 0 else 0


    def PrintSystemReport(self, *args):
        node_count = nodes * (1 - failRate)
        avg_degree = round(self.degree_sum / node_count, 2) if node_count > 0 else 0

        total_msgs = len(self.reliability)
        if total_msgs == 0:
            self.out.write("No messages received.\n")
            return

        avRel = avNodes = avLat = avRmr = avGossip = avInv = avReq = 0

        clustering = self.compute_clustering()

        for msg_id in sorted(self.reliability.keys()):
            received_count = self.reliability[msg_id]
            reliability_pct = round(received_count / node_count * 100, 3)
            latency = self.latency[msg_id]
            rmr = round((self.redundancy[msg_id][0] / (received_count - 1)) - 1, 3) if received_count > 1 else 0

            avRel += reliability_pct
            avNodes += received_count
            avLat += latency
            avRmr += rmr
            avGossip += self.redundancy[msg_id][0]
            avInv += self.redundancy[msg_id][1]
            avReq += self.redundancy[msg_id][2]

            self.out.write(f"{msg_id} -- Reliability:{reliability_pct}%  Nodes:{received_count}  "
                           f"Latency:{latency}  RMR:{rmr}  PayloadSent:{self.redundancy[msg_id][0]}  "
                           f"INVSent:{self.redundancy[msg_id][1]}  GETDATASent:{self.redundancy[msg_id][2]}\n")

        avRel /= total_msgs
        avNodes /= total_msgs
        avLat /= total_msgs
        avRmr /= total_msgs
        avGossip /= total_msgs
        avInv /= total_msgs
        avReq /= total_msgs
        avg_shortest_path = self.shortestPathSum / (total_msgs * node_count)

        self.out.write(f"\nNumber of Miners: {self.totalMiners}  Total Transactions: {self.totalTransactions}\n")
        self.out.write(f"AVERAGE -- Reliability:{avRel:.3f}%  Nodes:{avNodes:.1f}  Latency:{avLat:.1f}  "
                       f"RMR:{avRmr:.3f}  PayloadSent:{avGossip:.1f}  INVSent:{avInv:.1f}  GETDATASent:{avReq:.1f}\n")
        self.out.write(f"Degree Avg:{avg_degree}  Min:{self.minDegree}  Max:{self.maxDegree}  "
                      f"Shortest Path Avg:{avg_shortest_path:.2f} Clustering Coefficient  {clustering:.4f}\n")

        # Membership message summary
        self.out.write("\n--- Membership control messages ---\n")
        total_membership = sum(self.membership_msgs_sum.values())
        if total_membership == 0:
            self.out.write("No membership messages recorded.\n")
        else:
            for msg_type in sorted(self.membership_msgs_sum.keys()):
                count = self.membership_msgs_sum[msg_type]
                pct = count / total_membership * 100
                self.out.write(f"{msg_type:>25}: {count:>10}  ({pct:>6.2f}%)\n")
            self.out.write(f"{'TOTAL':>25}: {total_membership:>10}\n")

        
class Seeder(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Seeder, self).__init__(baseInfo)

        self.sid = args[0]

        region_size = nodes // SEEDER_COUNT
        region_start = self.sid * region_size
        region_end = (self.sid + 1) * region_size
        bias_pool = list(range(region_start, region_end))

        known_size = max(100, int(0.05 * nodes))
        bias_count = int(0.6 * known_size)
        global_count = known_size - bias_count
        bias_sample = random.sample(bias_pool, min(bias_count, len(bias_pool)))
        global_sample = random.sample(range(nodes), min(global_count, nodes))
        self.known_nodes = list(set(bias_sample + global_sample))

    def PeerRequest(self, *args):
        requester_id = args[0]
        candidates = [p for p in self.known_nodes if p != requester_id]
        sample_size = min(200, len(candidates))
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
        self.mining_parent = None
        self.mining = False


        #Seeder/Peer Management
        self.inbound = []
        self.outbound = []
        self.MAX_OUT = 8
        self.MAX_IN = 117
        self.addr_new = []
        self.addr_tried = []
        self.MAX_NEW = 1000
        self.MAX_TRIED = 256
        self.nodeTimer = set()
        self.pingTimer = {}  
        self.ping_nonce = 0

        self.receivedMsgs = {}
        self.report = {}

        # Membership message counters
        self.membership_msgs = defaultdict(int)

        JOIN_WINDOW = (0, 200)
        self.reqService(random.uniform(*JOIN_WINDOW), "Bootstrap", "none")
        self.reqService(endTime - 1, "TriggerSystemReport", "none")
           
    def next_nonce(self):
        self.ping_nonce += 1
        return self.ping_nonce

    def start_mining(self):
        if not (self.miner and self.active):
            return

        if self.mining:
            return

        self.mining = True
        self.mining_parent = self.blockchain.head
        
        total_miners = math.ceil(0.01 * self.total_nodes)

        NETWORK_BLOCK_TIME = 10

        avg_mining_time = NETWORK_BLOCK_TIME * total_miners

        delay = random.expovariate(1 / avg_mining_time)

        self.reqService(delay, "mine_block", "none")



#--------------------------------------- PEER SELECTION ---------------------------------------------------

    def Bootstrap(self, *args):

        self.membership_msgs['PeerRequest'] += 1
        self.reqService(lookahead, "PeerRequest", self.node_idx, "Seeder", random.choice(total_seeders))
        self.reqService(120, "FeelerMechanism", "none")
        self.reqService(50, "Ping", "none")

    def ReceivePeers(self, *args):
        peer_list = args[0]
        for p in peer_list:
            if p not in self.addr_new and p not in self.addr_tried and p != self.node_idx:
                self.addr_new.append(p)
                if len(self.addr_new) > self.MAX_NEW:
                    self.addr_new.pop()

        self.OutboundConnect()

    def OutboundConnect(self):

        while len(self.outbound) < self.MAX_OUT:
            peer = None

            if not self.addr_new and not self.addr_tried:
                break

            if self.addr_tried and (not self.addr_new or random.random() < 0.5):
                peer = random.choice(list(self.addr_tried))
                self.addr_tried.remove(peer) 
            else:
                if not self.addr_new:
                    break
                peer = random.choice(list(self.addr_new))
                self.addr_new.remove(peer)

            self.outbound.append(peer)
            
            if len(self.addr_tried) > self.MAX_TRIED:
                self.addr_tried.pop(0)

            triedPeers = random.sample(list(self.addr_tried),min(10, len(self.addr_tried)))
            newPeers = random.sample(list(self.addr_new),min(5, len(self.addr_new)))
            handshakePeers = triedPeers + newPeers
            self.nodeTimer.add(peer)
            self.reqService(seederTimer, "NodeTimer", peer)
            self.membership_msgs['HandShake'] += 1
            self.reqService(lookahead,"HandShake",(self.node_idx, handshakePeers),"Node",peer)


    def HandShake(self, *args):

        sender, receivedPeers = args[0]

        if sender in self.outbound or sender in self.inbound:
            self.membership_msgs['HandShakeResponse'] += 1
            self.reqService(lookahead, "HandShakeResponse", (self.node_idx, "Duplicate"), "Node", sender)
            return

        if len(self.inbound) >= self.MAX_IN:
            return

        self.inbound.append(sender)
        for p in receivedPeers:
            if p == self.node_idx:
                continue
            if p not in self.addr_tried and p not in self.addr_new: 
                self.addr_new.append(p)
                if len(self.addr_new) > self.MAX_NEW:
                    self.addr_new.pop()

        triedPeers = random.sample(list(self.addr_tried),min(10, len(self.addr_tried)))
        newPeers = random.sample(list(self.addr_new),min(5, len(self.addr_new)))
        responsePeers = triedPeers + newPeers

        self.membership_msgs['HandShakeResponse'] += 1
        self.reqService(lookahead,"HandShakeResponse", (self.node_idx, responsePeers),"Node",sender)


    def HandShakeResponse(self, *args):
        sender, receivedPeers = args[0]

        if receivedPeers == "Duplicate":
            self.nodeTimer.discard(sender)
            if sender in self.outbound:
                self.outbound.remove(sender)
            if sender not in self.addr_tried and sender not in self.addr_new:
                self.addr_tried.append(sender)
            self.OutboundConnect()
            return

       
        if sender not in self.outbound:
            return

        self.nodeTimer.discard(sender)
        self.addr_tried.append(sender)
        for p in receivedPeers:
            if p == self.node_idx:
                continue
            if p not in self.addr_tried and p not in self.addr_new:
                self.addr_new.append(p)
                if len(self.addr_new) > self.MAX_NEW:
                    self.addr_new.pop()


    def FeelerMechanism(self, *args):
        if not self.active:
            return

        self.reqService(120, "FeelerMechanism", "none")

        if not self.addr_new:
            return

        peer = random.choice(list(self.addr_new))

        self.nodeTimer.add(peer)
        self.reqService(seederTimer, "NodeTimer", peer)
        self.membership_msgs['FeelerHandshake'] += 1
        self.reqService(lookahead,"FeelerHandshake",self.node_idx,"Node",peer)

        
    def FeelerHandshake(self, *args):
        sender = args[0]
        if sender in self.outbound or sender in self.inbound:
            self.membership_msgs['FeelerHandshakeResponse'] += 1
            self.reqService(lookahead,"FeelerHandshakeResponse",  (-1 * self.node_idx) ,"Node",sender)
            return
        self.membership_msgs['FeelerHandshakeResponse'] += 1
        self.reqService(lookahead,"FeelerHandshakeResponse",self.node_idx,"Node",sender)


    def FeelerHandshakeResponse(self, *args):
        sender = args[0]
        
        if sender < 0:

            self.nodeTimer.remove((-1 * sender))
            return

        self.nodeTimer.discard(sender)

        if sender in self.addr_new:
            self.addr_new.remove(sender)
        if sender not in self.addr_tried:
            self.addr_tried.append(sender)

    def Ping(self, *args):
        if not self.active:
            return

        self.reqService(50, "Ping", "none")

        for peer in self.outbound + self.inbound:
            nonce = self.next_nonce()
            self.pingTimer[peer] = nonce
            self.reqService(seederTimer, "PingTimer", (peer, nonce))
            self.membership_msgs['PingMsg'] += 1
            self.reqService(lookahead, "PingMsg", (self.node_idx, nonce), "Node", peer)


    def PingMsg(self, *args):
        sender, nonce = args[0]
        self.membership_msgs['PongMsg'] += 1
        self.reqService(lookahead, "PongMsg", (self.node_idx, nonce), "Node", sender)

    
    def PongMsg(self, *args):
        sender, nonce = args[0]
        if self.pingTimer.get(sender) == nonce:
            del self.pingTimer[sender]


    def PingTimer(self, *args):
        peer, nonce = args[0]
        if self.pingTimer.get(peer) != nonce:
            return  
    
        del self.pingTimer[peer]
    
        if peer in self.outbound:
            self.NodeFailure(peer) 
        elif peer in self.inbound:
            self.inbound.remove(peer)

    def NodeTimer(self, *args):
        peer = args[0]
        if peer not in self.nodeTimer:
            return 

        self.nodeTimer.remove(peer)

        if peer not in self.outbound and peer not in self.inbound:
            if peer not in self.addr_new and peer not in self.addr_tried:
                self.addr_new.append(peer)
            self.OutboundConnect()

    def NodeFailure(self, node_id):
        if node_id in self.inbound:
            self.inbound.remove(node_id)

        if node_id in self.outbound:
            self.outbound.remove(node_id)

        if node_id in self.addr_tried:
            self.addr_tried.remove(node_id)
        
        if node_id in self.addr_new:
            self.addr_new.remove(node_id)

        self.OutboundConnect()

 #--------------------------------------- GOSSIP ---------------------------------------------------

    def Receive(self, *args):
        if not self.active:
            return
        msg = args[0]

        if msg.ID not in self.receivedMsgs.keys():
            self.receivedMsgs[msg.ID] = msg
            if msg.ID not in self.report.keys():
                self.report[msg.ID] = [0,0,0]

            if msg.type == "BLOCK":
                block = Block.from_dict(msg.payload)
                accepted = self.blockchain.consensus(block)
                if accepted and block.hash == self.blockchain.head:
                    self.blockchain.remove_confirmed_transactions(block)
                    self.mining = False  
                self.SendInv(msg.ID, msg.sender)

            elif msg.type == "TRX":
                if self.miner:
                    self.blockchain.add_new_transaction(msg.payload)
                self.SendInv(msg.ID, msg.sender)


    def mine_block(self, *args):
        if not self.mining:
            self.start_mining()
            return

        if self.blockchain.head != self.mining_parent:
            self.mining = False
            self.start_mining()
            return

        self.blockchain.mine()
        new_block = self.blockchain.last_block
        self.blockchain.remove_confirmed_transactions(new_block)

        
        block_id = "B-" + new_block.hash[:8]
        block_msg = msg2("BLOCK", new_block.to_dict(), block_id, 0, self.node_idx)
        self.receivedMsgs[block_msg.ID] = block_msg
        self.SendInv(block_msg.ID)

        self.mining = False
        self.start_mining()

    def SendInv(self, msg_id, exclude = None):
        for peer in self.outbound:
            if peer != exclude:
                if msg_id not in self.report.keys():
                    self.report[msg_id] = [0,0,0]
                self.report[msg_id][1] += 1
                self.reqService(lookahead, "ReceiveInv", (msg_id, self.node_idx), "Node", peer)

        for peer in self.inbound:
            if peer != exclude:
                if msg_id not in self.report.keys():
                    self.report[msg_id] = [0,0,0]
                self.report[msg_id][1] += 1
                self.reqService(lookahead, "ReceiveInv", (msg_id, self.node_idx), "Node", peer)

    def ReceiveInv(self, *args):
        if not self.active:
            return
        msg_id, sender_id = args[0]

        if msg_id not in self.receivedMsgs:
            if msg_id not in self.report.keys():
                self.report[msg_id] = [0,0,0]
            self.report[msg_id][2] += 1
            self.reqService(lookahead, "RequestMessage", (msg_id, self.node_idx), "Node", sender_id)

    def RequestMessage(self, *args):
        msg_id, requester_id = args[0]

        if msg_id in self.receivedMsgs:
            if msg_id not in self.report.keys():
                self.report[msg_id] = [0,0,0]
            self.report[msg_id][0] += 1

            msg = self.receivedMsgs[msg_id]
            self.reqService(lookahead, "Receive", msg2(msg.type , msg.payload , msg.ID, msg.hops + 1 , self.node_idx), "Node", requester_id)
    

    def TriggerSystemReport(self,*args):
        if self.active:
            messageMetrics = []

            for m in self.receivedMsgs.keys():
                messageMetrics.append( {
                                        'id': m,
                                        'hopsDone': self.receivedMsgs[m].hops,
                                        'gossip': self.report[m][0],
                                        'inv': self.report[m][1],
                                        'requests': self.report[m][2]
                                        })
                
            neighbors = list(set(self.outbound + self.inbound))
            diss_degree = len(neighbors)

            msgToSend = msgReport(
                messageMetrics,
                diss_degree,
                self.miner,
                len(self.blockchain.chain),
                self.trxMade,
                self.node_idx,
                neighbors,
                dict(self.membership_msgs)
            )

            self.reqService(lookahead, "SystemReport", msgToSend , "ReportNode", 0)

    def BecomeMiner(self, *args):
        self.miner = True
        if self.node_idx not in minerNodes:
            minerNodes.append(self.node_idx) 
        self.start_mining()
  

    def force_churn_out(self, *args):
        if self.active:
            self.active = False
            self.peers = []
            self.blockchain.chain = []
            self.blockchain.orphans = []
            self.receivedMsgs = {}
            self.report = {}

            if self.node_idx in upNodes:
                upNodes.remove(self.node_idx)
            if self.node_idx not in downNodes:
                downNodes.append(self.node_idx)
        #print(f"Node {self.node_idx} has gone down.")

    def create_transaction(self,*args):
        avg_transactionT = 5.0
        delay = random.expovariate(1/avg_transactionT)

        if self.active and not self.miner:
            transaction = Transaction(self.node_idx)
            tx_msg = msg2("TRX", transaction, transaction.trans_id, 0, self.node_idx)
            self.reqService(lookahead, "Receive", tx_msg, "Node", self.node_idx)
            self.trxMade += 1

        non_miner_nodes = [n for n in upNodes if n not in minerNodes]
        if non_miner_nodes:
            n = random.choice(non_miner_nodes)
            self.reqService(delay, "create_transaction", "", "Node", n)


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
   
    simianEngine.schedService(250 + lookahead, "BecomeMiner", "", "Node", n)
    upNodes.remove(n)

n = random.choice(upNodes)

simianEngine.schedService(250 + lookahead , "create_transaction","" , "Node", n)


simianEngine.run()
simianEngine.exit()
