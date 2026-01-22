import report
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

seederTimer = 1.5

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
    def __init__(self, msgs, diss_degree, miner, length, trx,
                 node_id, neighbors):
        self.msgsFromNode = msgs
        self.disseminationDegree = diss_degree
        self.isMiner = miner
        self.lengthChainFromNode = length
        self.trxFromNode = trx
        self.node_id = node_id
        self.neighbors = neighbors 

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
        self.graph = {}  # node_id -> set(active neighbors)

        # Schedule the final report
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

             links /= 2  # undirected double count

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

        # Initialize aggregates
        avRel = avNodes = avLat = avRmr = avGossip = avInv = avReq = 0

        clustering = self.compute_clustering()

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

        # Print summary
        self.out.write(f"\nNumber of Miners: {self.totalMiners}  Total Transactions: {self.totalTransactions}\n")
        self.out.write(f"AVERAGE -- Reliability:{avRel:.3f}%  Nodes:{avNodes:.1f}  Latency:{avLat:.1f}  "
                       f"RMR:{avRmr:.3f}  Gossip:{avGossip:.1f}  Inv:{avInv:.1f}  Requests:{avReq:.1f}\n")
        self.out.write(f"Degree Avg:{avg_degree}  Min:{self.minDegree}  Max:{self.maxDegree}  "
                      f"Shortest Path Avg:{avg_shortest_path:.2f} Clustering Coefficient  {clustering:.4f}\n")
        
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
        self.mining_parent = None
        self.mining = False


        #Seeder/Peer Management
        self.inbound = []           # inbound connections 117
        self.outbound = []          # outbound connections (max 8)
        self.MAX_OUT = 8
        self.MAX_IN = 117
        self.addr_new = []    # learned but untried
        self.addr_tried = []   # successfully connected
        self.MAX_NEW = 1000
        self.MAX_TRIED = 256
        self.nodeTimer = set()   # peers with outstanding connection attempts
        self.pingTimer = {}  
        self.ping_nonce = 0

        self.receivedMsgs = {}
        self.report = {}

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
            return  # already mining

        self.mining = True
        self.mining_parent = self.blockchain.head
        avg_mining_time = 30
        delay = random.expovariate(1 / avg_mining_time)
        self.reqService(delay, "mine_block", "none")



#--------------------------------------- PEER SELECTION ---------------------------------------------------

    def Bootstrap(self, *args):

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

        i = 0
        while len(self.outbound) < self.MAX_OUT:
            peer = None

            if not self.addr_new:
                break


            if self.addr_tried:
                i += 1
                p_tried = math.sqrt(1.0 * (10 - i)) / (i + math.sqrt(1.0 * (10 - i)))
                if random.random() < p_tried:
                    peer = random.choice(list(self.addr_tried))
                else:
                    peer = random.choice(list(self.addr_new))
                    self.addr_new.remove(peer)
            else:
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
            self.reqService(lookahead,"HandShake",(self.node_idx, handshakePeers),"Node",peer)


    def HandShake(self, *args):

        sender, receivedPeers = args[0]

        # Reject duplicate connection
        if sender in self.outbound or sender in self.inbound:
            self.reqService(lookahead, "HandShakeResponse", (self.node_idx, "Duplicate"), "Node", sender)
            return

        if len(self.inbound) >= self.MAX_IN:
            return

        self.inbound.append(sender)
        for p in receivedPeers:
            if p == self.node_idx:
                continue
            if p not in self.addr_tried:
                self.addr_new.append(p)
                if len(self.addr_new) > self.MAX_NEW:
                    self.addr_new.pop()

        # Send response 
        triedPeers = random.sample(list(self.addr_tried),min(10, len(self.addr_tried)))
        newPeers = random.sample(list(self.addr_new),min(5, len(self.addr_new)))
        responsePeers = triedPeers + newPeers

        #print(f"Node {self.node_idx} sending this respinse peers {responsePeers} to node {sender}")
        self.reqService(lookahead,"HandShakeResponse", (self.node_idx, responsePeers),"Node",sender)


    def HandShakeResponse(self, *args):
        sender, receivedPeers = args[0]

        if receivedPeers == "Duplicate":
            self.nodeTimer.discard(sender)
            self.outbound.remove(sender)
            self.OutboundConnect()
            return

        self.nodeTimer.discard(sender)
        self.addr_tried.append(sender)
        for p in receivedPeers:
            if p == self.node_idx:
                continue
            if p not in self.addr_tried:
                self.addr_new.append(p)
                if len(self.addr_new) > self.MAX_NEW:
                    self.addr_new.pop()


    def FeelerMechanism(self, *args):
        if not self.active:
            return
        
        if not self.addr_new:
            return

        peer = random.choice(list(self.addr_new))

        # Temporary connection
        self.nodeTimer.add(peer)
        self.reqService(seederTimer, "NodeTimer", peer)
        self.reqService(lookahead,"FeelerHandshake",self.node_idx,"Node",peer)

        
    def FeelerHandshake(self, *args):
        sender = args[0]
        if sender in self.outbound or sender in self.inbound:
            self.reqService(lookahead,"FeelerHandshakeResponse",  (-1 * self.node_idx) ,"Node",sender)
            return
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
        for peer in self.outbound + self.inbound:
            nonce = self.next_nonce()
            self.pingTimer[peer] = nonce
            self.reqService(seederTimer, "PingTimer", (peer, nonce))
            self.reqService(lookahead, "PingMsg", (self.node_idx, nonce), "Node", peer)


    def PingMsg(self, *args):
        sender, nonce = args[0]
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

        # Only fail if still not connected
        if peer not in self.outbound and peer not in self.inbound:
            self.NodeFailure(peer)

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

        if msg.ID not in self.report.keys():
            self.report[msg.ID] = [1,0,0]
        else:
            self.report[msg.ID][0] += 1
    
        if msg.ID not in self.receivedMsgs.keys():
            self.receivedMsgs[msg.ID] = msg

            if msg.type  == "BLOCK":
                block = Block.from_dict(msg.payload)
                accepted = self.blockchain.consensus(block)
                if accepted and block.hash == self.blockchain.head:
                    self.blockchain.remove_confirmed_transactions(block)
                    if accepted and block.previous_hash == self.blockchain.last_block.hash:
                        self.mining = False

                self.SendInv(msg.ID)

            elif msg.type == "TRX":
                if self.miner:
                    self.blockchain.add_new_transaction(msg.payload)
                self.SendInv(msg.ID)


    def mine_block(self, *args):
        if not self.mining:
            return

        # Chain advanced → abort
        if self.blockchain.head != self.mining_parent:
            self.mining = False
            self.start_mining()
            return

        # No transactions → wait
        if not self.blockchain.unconfirmed_transactions:
            self.mining = False
            self.start_mining()
            return

        # Success
        self.blockchain.mine()
        new_block = self.blockchain.last_block

        new_block = self.blockchain.last_block
        block_id = "B-" + str(random.randint(11111111,99999999))

        block_msg = msg2("BLOCK", new_block.to_dict(), block_id, 0, self.node_idx)
        self.receivedMsgs[block_msg.ID] = block_msg
        self.report[block_msg.ID] = [1, 0, 0]
        self.SendInv(block_msg.ID)

        self.mining = False
        self.start_mining()



    def SendInv(self, msg_id):
        for peer in self.outbound:
            self.reqService(lookahead, "ReceiveInv", (msg_id, self.node_idx), "Node", peer)

        for peer in self.inbound:
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
                neighbors
            )

            #print("\n=== BLOCK DAG PER NODE ===")
            #print(f"\nNode {self.node_idx}")
            #self.blockchain.print_block_dag()

            self.reqService(lookahead, "SystemReport", msgToSend , "ReportNode", 0)

    def BecomeMiner(self, *args):
        self.miner = True
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