import report
from Blockchain import * 
from simian import Simian
import random, math, argparse, hashlib
from collections import defaultdict

parser = argparse.ArgumentParser(
    description='The PHOLD model.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int,
                    help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float,
                    help='simulation end time')
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1,
                    help="min delay of mailboxes -> default 0.1")
parser.add_argument("--seedR", type=int, metavar='SEED', default=10,
                    help="seed for random number generation -> default 10")
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0,
                    help="use mpi -> 0-false  1-true")
parser.add_argument("--updateViews", type=float, metavar='TIME', default=5,
                    help="update passive Views trigger time -> default 5")
parser.add_argument("--activeChurn", type=int, metavar='CHURN', default=0,
                    help="activates the network churn-> default 0")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0,
                    help="node fail rate [0.0 ... 1.0]")

parser.add_argument("--c", type=int, metavar='VIEWSIZE', default=3,
                    help="c value -> view Size = log n + c -> default 3")
parser.add_argument("--a", type=float, metavar='ALFA', default=0.45,
                    help="alpha value [0.0 ... 1.0]")
parser.add_argument("--b", type=float, metavar='BETA', default=0.45,
                    help="beta value [0.0 ... 1.0]")
args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True

## PLUMTREE variables
nodes = args.total_nodes
lookahead = args.lookahead
failRate = args.failRate
random.seed(args.seedR)
churn = args.activeChurn
triggerSysReportTime = args.endtime - 1
timeout1 = args.lookahead
timeout2 = args.lookahead / 2
delayLazy = 1

TriggerBrahmsTime = args.updateViews
TriggerBrahmsTime2 = args.updateViews
churnEndtime = 50
stabilizationTime = 250

TriggerBrahmsTimePost = args.updateViews * 5

l1 = math.ceil(args.c * math.pow(args.total_nodes, 1.0/3.0))
l2 = math.ceil(args.c * math.pow(args.total_nodes, 1.0/3.0))
a = args.a
b = args.b
y = 1 - a - b

downNodes = []
upNodes = []

name = "PlumTreeBrahms/PlumTree + Brahms" + str(args.total_nodes)+'-Seed'+str(args.seedR)+'-Update'+str(args.updateViews)+'-View'+str(args.c)+'-LOOKAHEAD'+str(args.lookahead)+'-CHURN'+str(args.activeChurn)+'-FAILRATE'+str(args.failRate)

simName, startTime, endTime, minDelay, useMPI, mpiLib = name, 0, args.endtime, 0.00001, uMPI, "/usr/lib/x86_64-linux-gnu/libmpich.so"
simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)


class msgGossip:
    def __init__(self,type,ptype,m,mID,hops,sender):
        self.type = type
        self.payloadType = ptype       
        self.payload = m
        self.ID = mID
        self.hops = hops
        self.sender = int(sender)

    def toString(self):
        return '%s-%s-%d-%d-%d'%(self.type,self.payload,self.ID,self.hops,self.sender)

class msgReport:
    def __init__(self, msgs, diss_degree, miner, length, trx,
                 node_id, neighbors, membership_degree, membership_msgs=None):
        self.msgsFromNode = msgs
        self.disseminationDegree = diss_degree
        self.isMiner = miner
        self.lengthChainFromNode = length
        self.trxFromNode = trx
        self.node_id = node_id
        self.neighbors = neighbors 
        self.membership_degree = membership_degree 
        self.membership_msgs = membership_msgs if membership_msgs else {}

class msgBrahms:
    def __init__(self,type,view,sender):
        self.type = type
        self.view = eval(view)
        self.sender = sender

class msgOverlaySnapshot:
    def __init__(self, node_id, t, eager, lazy, v, membership_view):
        self.node_id = node_id
        self.t = t
        self.eager = eager
        self.lazy = lazy
        self.v = v
        self.membership_view = membership_view 

class Sampler:
    def __init__(self):
        self.h = random.random() 
        self.q = None
        self.hq = None

    @staticmethod
    def stable_hash(seed, elem):
        return int(hashlib.sha256(f"{seed}-{elem}".encode()).hexdigest(), 16)
    
    def next(self, elem):
        helem = self.stable_hash(self.h, elem)
        if self.q is None or helem < self.hq:
            self.q = elem
            self.hq = helem

    def sample(self):
        return self.q

    def invalidate(self):
        self.__init__()

    def toString(self):
        return '%s-' %(self.q)
    
class ChurnManager(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ChurnManager, self).__init__(baseInfo)
        if churn == 1:
            self.reqService(stabilizationTime + 50, "do_churn_cycle", "none")

    def do_churn_cycle(self, *args):
        churn_size = int(nodes * failRate)
        if len(upNodes) > churn_size and churn_size > 0:
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
        self.membership_degree_sum = 0  

        self.snapshots = {}

        self.membership_msgs_sum = defaultdict(int)

        self.reqService(endTime, "PrintSystemReport", "none")

    def OverlaySnapshot(self, *args):
        snap = args[0]
        t = snap.t
        if t not in self.snapshots:
            self.snapshots[t] = {'eager': [], 'lazy': [], 'v': []}
        self.snapshots[t]['eager'].append(snap.eager)
        self.snapshots[t]['lazy'].append(snap.lazy)
        self.snapshots[t]['v'].append(snap.v)

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

        self.membership_degree_sum += report.membership_degree

        self.servedNodes += 1

        self.degree_sum += degree
        self.maxDegree = max(self.maxDegree, degree)
        self.minDegree = min(self.minDegree, degree)

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
                self.redundancy[msg_id] = [0, 0, 0, 0]
            self.redundancy[msg_id][0] += m['gossip']
            self.redundancy[msg_id][1] += m['ihave']
            self.redundancy[msg_id][2] += m['graft']
            self.redundancy[msg_id][3] += m['prune']

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
            Nu_list = list(Nu)
            for i in range(len(Nu_list)):
                for j in range(i+1, len(Nu_list)):
                    v, w = Nu_list[i], Nu_list[j]
                    if (v in self.graph and w in self.graph[v]) or \
                       (w in self.graph and v in self.graph[w]):
                        links += 1

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

        avRel = avNodes = avLat = avRmr = avGossip = avInv = avReq = avPru = 0

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
            avPru += self.redundancy[msg_id][3]
            
            self.out.write(f"{msg_id} -- Reliability:{reliability_pct}%  Nodes:{received_count}  "
                           f"Latency:{latency}  RMR:{rmr}  GossipSent:{self.redundancy[msg_id][0]}  "
                           f"IHAVESent:{self.redundancy[msg_id][1]}  GRAFTSent:{self.redundancy[msg_id][2]} PRUNESent:{self.redundancy[msg_id][3]}\n")

        avRel /= total_msgs
        avNodes /= total_msgs
        avLat /= total_msgs
        avRmr /= total_msgs
        avGossip /= total_msgs
        avInv /= total_msgs
        avReq /= total_msgs
        avPru /= total_msgs
        avg_shortest_path = self.shortestPathSum / (total_msgs * node_count)

        self.out.write(f"\nNumber of Miners: {self.totalMiners}  Total Transactions: {self.totalTransactions}\n")
        self.out.write(f"AVERAGE -- Reliability:{avRel:.3f}%  Nodes:{avNodes:.1f}  Latency:{avLat:.1f}  "
                       f"RMR:{avRmr:.3f}  GossipSent:{avGossip:.1f}  IHAVESent:{avInv:.1f}  GRAFTSent:{avReq:.1f} PRUNESent:{avPru:.1f}\n")
        avg_membership_degree = round(self.membership_degree_sum / node_count, 2) if node_count > 0 else 0
        self.out.write(f"PlumTree Degree Avg:{avg_degree}  Min:{self.minDegree}  Max:{self.maxDegree}  "
                       f"Shortest Path Avg:{avg_shortest_path:.2f} Clustering Coefficient  {clustering:.4f}\n")
        self.out.write(f"Membership Degree Avg:{avg_membership_degree}\n")

        # Degree distribution histogram
        self.out.write("\n--- Degree distribution (membership, directed) ---\n")
        degree_dist = {}
        for u, Nu in self.graph.items():
            d = len(Nu)
            degree_dist[d] = degree_dist.get(d, 0) + 1

        total_nodes_graph = len(self.graph)
        degrees = [len(Nu) for Nu in self.graph.values()]
        mean_d  = sum(degrees) / len(degrees) if degrees else 0
        var_d   = sum((d - mean_d)**2 for d in degrees) / len(degrees) if degrees else 0
        std_d   = var_d ** 0.5

        self.out.write(f"{'Grado':>8}  {'Nodos':>8}  {'% red':>10}\n")
        for d in sorted(degree_dist.keys()):
            pct = degree_dist[d] / total_nodes_graph * 100
            bar = '#' * int(pct / 2)
            self.out.write(f"{d:>8}  {degree_dist[d]:>8}  {pct:>9.2f}%  {bar}\n")
        self.out.write(f"Media: {mean_d:.2f}  Varianza: {var_d:.2f}  Desv. Est.: {std_d:.2f}\n")

        # Overlay convergence snapshots
        if self.snapshots:
            self.out.write("\n--- Overlay convergence snapshots ---\n")
            self.out.write(f"{'t':>6}  {'nodes':>6}  {'eager_avg':>10}  {'eager_std':>10}  "
                           f"{'lazy_avg':>9}  {'lazy_std':>9}  {'v_avg':>7}  {'v_std':>7}  "
                           f"{'lazy=0 (%)':>10}  {'tree_formed (%)':>15}\n")
            for t in sorted(self.snapshots.keys()):
                s = self.snapshots[t]
                n_snap = len(s['eager'])
                e_avg = sum(s['eager']) / n_snap
                e_std = (sum((x - e_avg)**2 for x in s['eager']) / n_snap) ** 0.5
                l_avg = sum(s['lazy'])  / n_snap
                l_std = (sum((x - l_avg)**2  for x in s['lazy'])  / n_snap) ** 0.5
                v_avg = sum(s['v'])     / n_snap
                v_std = (sum((x - v_avg)**2  for x in s['v'])     / n_snap) ** 0.5
                lazy_zero_pct = sum(1 for x in s['lazy'] if x == 0) / n_snap * 100
                tree_formed_pct = sum(1 for e, l in zip(s['eager'], s['lazy']) if e > 0 and l > 0) / n_snap * 100

                self.out.write(f"{t:>6}  {n_snap:>6}  {e_avg:>10.2f}  {e_std:>10.2f}  "
                               f"{l_avg:>9.2f}  {l_std:>9.2f}  {v_avg:>7.2f}  {v_std:>7.2f}  "
                               f"{lazy_zero_pct:>10.1f}  {tree_formed_pct:>15.1f}\n")
            self.out.write("tree_formed: % of nodes with eager>0 AND lazy>0 at snapshot time\n")
            self.out.write("lazy=0: % of nodes with no lazy peers (pure eager flood)\n")

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


class Node(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Node, self).__init__(baseInfo)
        self.total_nodes = int(args[1])
        self.node_idx = int(args[0])
        self.miner = False
        self.trxMade = 0

        self.active = True
        self.blockchain = Blockchain()
        self.blockchain.create_genesis_block()
        self.mining_parent = None               
        self.mining = False

        #plumTree variables
        self.eagerPushPeers = set()
        self.lazyPushPeers = set()
        self.lazyQueues = []
        self.missing = []
        self.receivedMsgs = {}
        self.timers = []

        self.report = {}
        self.timersAck = {}

        #brahms variables 
        self.S = []               
        self.V = set()
        self.Vpush = []
        self.Vpull = []

        self.samplerPingTimer = {}
        self.sampler_nonce = 0

        self.membership_msgs = defaultdict(int)

        contactNode = self.node_idx - 1
        contactNode2 = self.node_idx - 2
        contactNode3 = self.node_idx + 1
        contactNode4 = self.node_idx + 2
        if contactNode2 < 0:
            contactNode2 += nodes
        if contactNode4 >= nodes:
            contactNode4 -= nodes 
        if contactNode < 0:
            contactNode += nodes
        if contactNode3 >= nodes:
            contactNode3 -= nodes

        self.BrahmsInit([contactNode,contactNode2])
        self.reqService(5, "TriggerBrahmsSend", "none")
        self.reqService(stabilizationTime, "TriggerSamplerProbe", "none")

        for snap_t in [50, 100, 150, 200, 250, 300]:
            if snap_t < endTime:
                self.reqService(snap_t, "SendOverlaySnapshot", snap_t)

        self.reqService(triggerSysReportTime, "TriggerSystemReport", "none")

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


#--------------------------------------- GOSSIP ---------------------------------------------------

    def PlumTreeGossip(self, *args):
        msg = args[0]
        if self.active==True:
            if msg.type =='PRUNE':
                if msg.sender in self.timersAck.keys():
                    val = False
                    for pair in self.timersAck[msg.sender]:
                        if val == False and pair[0] == msg.ID and pair[1] == msg.hops:
                             self.timersAck[msg.sender].remove(pair)
                             val = True
                  
                if msg.sender in self.eagerPushPeers:
                    self.eagerPushPeers.discard(msg.sender)
                if msg.sender in self.V:
                    self.lazyPushPeers.add(msg.sender)

            elif msg.type =='IHAVE':
                msgToSend = msgGossip('ACK','','',msg.ID,msg.hops,self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                if msg.ID not in self.receivedMsgs.keys():
                    self.missing.append((msg.ID,msg.sender,msg.hops))
                    if msg.ID not in self.timers:
                        self.timers.append(msg.ID)
                        self.reqService(timeout1, "Timer", msg.ID)

            elif msg.type =='GRAFT':
                if msg.sender in self.V:
                    self.eagerPushPeers.add(msg.sender)
                self.lazyPushPeers.discard(msg.sender)
                if msg.ID in self.receivedMsgs.keys():
            
                    if msg.ID not in self.report.keys():
                        self.report[msg.ID] = [0,0,0,0]
                    self.report[msg.ID][0] += 1

                    msgToSend = msgGossip('GOSSIP',self.receivedMsgs[msg.ID].payloadType,self.receivedMsgs[msg.ID].payload,msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='GOSSIP':
                if msg.ID not in self.receivedMsgs.keys():
                    msgToSend = msgGossip('ACK','','',msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                    self.receivedMsgs[msg.ID] = msg

                    if msg.ID not in self.report.keys():
                        self.report[msg.ID] = [0,0,0,0]
                    
                    if msg.ID in self.timers:
                        self.timers.remove(msg.ID)

                    if msg.payloadType  == "BLOCK":
                        block = Block.from_dict(msg.payload)
                        accepted = self.blockchain.consensus(block)
                        if accepted and block.hash == self.blockchain.head:
                            self.blockchain.remove_confirmed_transactions(block)
                            self.mining = False

                        self.LazyPush(msg)
                        self.EagerPush(msg)            

                    elif msg.payloadType == "TRX":
                        if self.miner:
                            self.blockchain.add_new_transaction(msg.payload)
                
                        self.LazyPush(msg)
                        self.EagerPush(msg)

                    if msg.sender in self.V:
                        self.eagerPushPeers.add(msg.sender)
                    self.lazyPushPeers.discard(msg.sender)

                else:
                    self.eagerPushPeers.discard(msg.sender)
                    if msg.sender in self.V:
                        self.lazyPushPeers.add(msg.sender)

                    if msg.ID not in self.report.keys():
                        self.report[msg.ID] = [0,0,0,0]
                    else:
                        self.report[msg.ID][3] += 1

                    msgToSend = msgGossip('PRUNE','','',msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='BROADCAST':
                mID = msg.ID
                msg.type = 'GOSSIP'
                self.receivedMsgs[mID] = msg
                self.report[msg.ID] = [0,0,0,0]
                self.EagerPush(msg)
                self.LazyPush(msg)

            elif msg.type =='ACK':
                if msg.sender in self.timersAck.keys():
                    val = False
                    for pair in self.timersAck[msg.sender]:
                        if val == False and pair[0] == msg.ID and pair[1] == msg.hops:
                             self.timersAck[msg.sender].remove(pair)
                             val = True


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
        block_msg = msgGossip('GOSSIP',"BLOCK", new_block.to_dict(), block_id, 0, self.node_idx)        
        self.receivedMsgs[block_msg.ID] = block_msg
        self.report[block_msg.ID] = [0, 0, 0, 0]

        self.EagerPush(block_msg)
        self.LazyPush(block_msg)

        self.mining = False
        self.start_mining()

    def EagerPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('GOSSIP',msg.payloadType,msg.payload,msg.ID,msg.hops + 1,self.node_idx)
        for n in self.eagerPushPeers:
            if n != sender:
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,0,0,0]
                self.report[msg.ID][0] += 1

                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", n)

    def LazyPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('IHAVE','',msg.payload,msg.ID,msg.hops + 1,self.node_idx)
        for n in self.lazyPushPeers:
            if n != sender:
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,0,0,0]
                self.report[msg.ID][1] += 1

                self.reqService(lookahead * delayLazy, "PlumTreeGossip", msgToSend, "Node", n)

    def NeighborUP(self, node):
        self.eagerPushPeers.add(node)

    def NeighborDown(self, node):
        self.eagerPushPeers.discard(node)
        self.lazyPushPeers.discard(node)

        for pair in self.missing:
            if pair[1] == node:
                self.missing.remove(pair)

    def Timer(self,*args):
        mID = args[0]
        if mID in self.timers:
            m = (mID,0,0)
            val = True
            for p in self.missing:
                if p[0] == mID and val:
                    m = p
                    val = False
                    self.missing.remove(p)

            if val == False:
                if m[1] in self.V:
                    self.eagerPushPeers.add(m[1])
                    self.lazyPushPeers.discard(m[1])

                if mID not in self.report.keys():
                    self.report[mID] = [0,0,0,0]
                self.report[mID][2] += 1

                msgToSend = msgGossip('GRAFT','','',mID,m[2],self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", m[1])

                self.reqService(timeout2, "Timer", mID)

    def TimerAcks(self,*args):
        pair = args[0]
        dest = pair[0]
        mID = pair[1]
        round = pair[2]
        if dest in self.timersAck.keys():
            val = False
            for pair in self.timersAck[dest]:
                if val == False and pair[0] == mID and pair[1] == round:
                     self.timersAck[dest].remove(pair)
                     val = True

            if val == True:
                self.NodeFailure(dest)
                self.timersAck[dest] = []
                self.NeighborDown(dest)

#--------------------------------------- PEER SELECTION ---------------------------------------------------

    def BrahmsInit(self, v0):
        self.V = v0
        self.eagerPushPeers = set()
        self.lazyPushPeers = set(v0)

        for i in range(l2):
            s = Sampler()
            self.S.append(s)

        self.updateSample(v0)


    def updateSample(self,V):
        for id in V:
            for i in range(l2):
                self.S[i].next(id)

    def rand(self,V,n):
        if n > len(V):
            n = len(V)
        return random.sample(V,n)

    def Brahms(self, *args):
        msg = args[0]
        if self.active:
            if msg.type == 'PUSH':
                if msg.sender not in self.Vpush:
                    self.Vpush.append(msg.sender)
            elif msg.type == 'PULL':
                msgToSend = msgBrahms('PULL_REPLY',str(self.V),self.node_idx)
                self.membership_msgs['PULL_REPLY'] += 1
                self.reqService(lookahead, "Brahms", msgToSend, "Node", msg.sender)
            elif msg.type == 'PULL_REPLY':
                if len(self.Vpull) < math.floor(b * l1):
                    v2 = list(msg.view)
                    if self.node_idx in v2:
                        v2.remove(self.node_idx)
                    self.Vpull = self.Vpull + v2

    def NodeFailure(self,node):
        self.V.discard(node)



#--------------------------------------- TRIGGERS ---------------------------------------------------

    def TriggerBrahmsSend(self, *args):
        if self.active==True:
            self.Vpull = list( dict.fromkeys(self.Vpull) )

            push_flooded = len(self.Vpush) > math.ceil(a * l1)

            if not push_flooded and (len(self.Vpush) != 0 or len(self.Vpull) != 0):
                sample = [s.sample() for s in self.S if s.sample() is not None]

                new_V = list(dict.fromkeys(
                    self.rand(self.Vpush, math.ceil(a*l1)) +
                    self.rand(self.Vpull, math.floor(b*l1)) +
                    self.rand(sample, math.ceil(y*l1))
                ))

                if self.node_idx in new_V:
                    new_V.remove(self.node_idx)

                old_V_set = set(self.V)
                new_V_set = set(new_V)

                for n in old_V_set - new_V_set:
                    self.eagerPushPeers.discard(n)
                    self.lazyPushPeers.discard(n)

                for n in new_V_set - old_V_set:
                    if n not in self.eagerPushPeers:
                        self.lazyPushPeers.add(n)

                self.V = set(new_V)

                self.updateSample(list( dict.fromkeys(self.Vpush + self.Vpull) ))

            if push_flooded:
                self.updateSample(list(dict.fromkeys(self.Vpush + self.Vpull)))

            self.Vpush = []
            self.Vpull = []

            if len(self.V) > 0:
                V_list = list(self.V)
                for n in random.choices(V_list, k=math.floor(a * l1)):
                    msgToSend = msgBrahms('PUSH',"[]",self.node_idx)
                    self.membership_msgs['PUSH'] += 1
                    self.reqService(lookahead, "Brahms", msgToSend, "Node", n)

                for n in random.choices(V_list, k=math.floor(b * l1)):
                    msgToSend = msgBrahms('PULL',"[]",self.node_idx)
                    self.membership_msgs['PULL'] += 1
                    self.reqService(lookahead, "Brahms", msgToSend, "Node", n)

            if self.engine.now < churnEndtime:
                self.reqService(TriggerBrahmsTime, "TriggerBrahmsSend", "none")
            elif self.engine.now < stabilizationTime:
                self.reqService(TriggerBrahmsTime2, "TriggerBrahmsSend", "none")
            else:
                self.reqService(TriggerBrahmsTimePost, "TriggerBrahmsSend", "none")
                
    def next_sampler_nonce(self):
        self.sampler_nonce += 1
        return self.sampler_nonce

    def TriggerSamplerProbe(self, *args):
        if not self.active:
            return
        for i, s in enumerate(self.S):
            if s.sample() is not None:
                nonce = self.next_sampler_nonce()
                self.samplerPingTimer[i] = nonce
                self.membership_msgs['SamplerPing'] += 1
                self.reqService(lookahead, "SamplerPingMsg",
                               (self.node_idx, i, nonce), "Node", s.sample())
                self.reqService(1.5, "SamplerPingTimer", (i, nonce))
        self.reqService(stabilizationTime / 10, "TriggerSamplerProbe", "none")

    def SamplerPingMsg(self, *args):
        sender, sampler_idx, nonce = args[0]
        if self.active:
            self.membership_msgs['SamplerPong'] += 1
            self.reqService(lookahead, "SamplerPongMsg",
                           (self.node_idx, sampler_idx, nonce), "Node", sender)

    def SamplerPongMsg(self, *args):
        sender, sampler_idx, nonce = args[0]
        if self.samplerPingTimer.get(sampler_idx) == nonce:
            del self.samplerPingTimer[sampler_idx]

    def SamplerPingTimer(self, *args):
        sampler_idx, nonce = args[0]
        if self.samplerPingTimer.get(sampler_idx) != nonce:
            return 
        del self.samplerPingTimer[sampler_idx]
        self.S[sampler_idx].invalidate()

    def SendOverlaySnapshot(self, *args):
        snap_t = args[0]
        if self.active:
            snap = msgOverlaySnapshot(
                self.node_idx,
                snap_t,
                len(self.eagerPushPeers),
                len(self.lazyPushPeers),
                len(self.V),
                list(self.V)  
            )
            self.reqService(lookahead, "OverlaySnapshot", snap, "ReportNode", 0)

    def TriggerSystemReport(self,*args):
        if self.active:
            messageMetrics = []
           
            for m in self.receivedMsgs.keys():
                messageMetrics.append( {
                                        'id': m,
                                        'hopsDone': self.receivedMsgs[m].hops,
                                        'gossip': self.report[m][0],
                                        'ihave': self.report[m][1],
                                        'graft': self.report[m][2],
                                        'prune': self.report[m][3]
                                        })

            diss_degree = len(self.eagerPushPeers) + len(self.lazyPushPeers)

            msgToSend = msgReport(
                messageMetrics,
                diss_degree,
                self.miner,
                len(self.blockchain.chain),
                self.trxMade,
                self.node_idx,
                list(self.V),
                len(self.V),
                dict(self.membership_msgs)
            )

            self.reqService(lookahead, "SystemReport", msgToSend , "ReportNode", 0)


    def nodeFail(self, *args):
        if self.active:
            self.active = False
        else:
            self.active = True

    def printViews(self, *args):
        res = ''
        sampleStr = ''
        for i in range(len(self.S)):
            sampleStr += self.S[i].toString() + '-'
        if self.active:
            self.out.write("%d:Peers %s %s msg %s\n"%(self.node_idx,str(self.V),str(sampleStr),res))
    
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

    def create_transaction(self,*args):
        n = random.choice(upNodes)
        avg_transactionT = 5.0
        delay = random.expovariate(1/avg_transactionT)
        if self.active and not self.miner:
            transaction = Transaction(self.node_idx)
            tx_msg = msgGossip('BROADCAST',"TRX", transaction, transaction.trans_id, 0, self.node_idx)
            self.reqService(lookahead, "PlumTreeGossip", tx_msg, "Node", self.node_idx)
            self.trxMade += 1
            self.reqService(delay , "create_transaction", "" , "Node", n)
        else:
            self.reqService(delay , "create_transaction", "" , "Node", n)


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
