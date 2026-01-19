from Blockchain import *
from simian import Simian
import random, math, argparse

parser = argparse.ArgumentParser(
    description='PlumTree + DIMPLE Protocol Simulation.',
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
parser.add_argument("--activeChurn", type=int, metavar='CHURN', default=0,
                    help="activates the network churn-> default 0")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0,
                    help="node fail rate [0.0 ... 1.0]")

###### DIMPLE
parser.add_argument("--shuffleTime", type=float, metavar='TIME', default=25,
                    help="Time to trigger the shuffle -> default 25")

args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True

## PLUMTREE variables
nodes = args.total_nodes
lookahead = args.lookahead
random.seed(args.seedR)
churn = args.activeChurn
triggerSysReportTime = args.endtime - 1
timeout1 = args.lookahead
timeout2 = args.lookahead / 2
threshold = 3

timerPTacks = 2.5
failRate = args.failRate
delayLazy = 1

# DIMPLE variables
shuffleTime = args.shuffleTime
shuffleSize = math.floor(math.log(nodes,2))
maxPartialView = 2 * math.floor(math.log(nodes,2))
dimpleTimer = 1.5

downNodes = []
upNodes = []

name = "PlumTreeDIMPLE/PlumTree + DIMPLE" + str(args.total_nodes)+'-Seed'+str(args.seedR)+'-ShuffleTime'+str(args.shuffleTime)+'-LOOKAHEAD'+str(args.lookahead)+'-CHURN'+str(args.activeChurn)

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

class msgDimple:
    def __init__(self,type,payload,sender):
        self.type = type
        self.payload = payload
        self.sender = sender

    def toString(self):
        return '%s-%d-%d-%d'%(self.type,self.msgs)
    
class partialViewEntry:
    def __init__(self,node_idx,age,visited):
        self.node_idx = node_idx
        self.age = age
        self.visited = visited

    def __str__(self):
        return f"partialViewEntry(node_idx={self.node_idx}, age={self.age}, visited={self.visited})"

    # Optional: for better printing in lists or debugging
    def __repr__(self):
        return self.__str__()
    
class msgReport:
    def __init__(self, msgs, diss_degree, miner, length, trx,
                 node_id, neighbors):
        self.msgsFromNode = msgs
        self.disseminationDegree = diss_degree
        self.isMiner = miner
        self.lengthChainFromNode = length
        self.trxFromNode = trx
        self.node_id = node_id
        self.neighbors = neighbors  # activeView snapshot

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

        # ensure node exists
        self.graph.setdefault(u, set()).update(Nu)

        # enforce undirected edges
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
            self.redundancy[msg_id][1] += m['ihave']
            self.redundancy[msg_id][2] += m['graft']

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
                           f"IHAVE:{self.redundancy[msg_id][1]}  GRAFT:{self.redundancy[msg_id][2]}\n")

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
                       f"RMR:{avRmr:.3f}  Gossip:{avGossip:.1f}  IHAVE:{avInv:.1f}  GRAFT:{avReq:.1f}\n")
        self.out.write(f"Degree Avg:{avg_degree}  Min:{self.minDegree}  Max:{self.maxDegree}  "
                       f"Shortest Path Avg:{avg_shortest_path:.2f} Clustering Coefficient  {clustering:.4f}\n")



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
        self.mining = False

        #plumTree variables
        self.eagerPushPeers = []
        self.lazyPushPeers = []
        self.lazyQueues = []
        self.missing = []
        self.receivedMsgs = {}
        self.timers = []
        self.timersAck = {}

        #Report Variables
        self.report = {}


        #DIMPLE Variables
        self.partial_view = []
        self.timerDimple = []
        delay = 10 / self.total_nodes

        JOIN_WINDOW = (0, 200)
        delay2 = random.uniform(*JOIN_WINDOW)

        if self.node_idx == 0:  # Seed nodes
            self.partial_view.append(partialViewEntry(self.node_idx, 0, [self.node_idx]))
            self.reqService(shuffleTime, "DimpleShuffle", "none")

        else:
            contactNode = 0 
            msg = msgDimple('JOIN',[],self.node_idx)
            self.reqService( delay2, "Dimple", msg, "Node", contactNode)  
        self.reqService(endTime - 1, "TriggerSystemReport", "none")
                                  

   #--------------------------------------- GOSSIP ---------------------------------------------------

    def PlumTreeGossip(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.sender)))
        if self.active==True:
            if msg.type =='PRUNE':
                if msg.sender in self.timersAck.keys():
                    val = False
                    for pair in self.timersAck[msg.sender]:
                        if val == False and pair[0] == msg.ID and pair[1] == msg.hops:
                             self.timersAck[msg.sender].remove(pair)
                             val = True

                if msg.sender in self.eagerPushPeers:
                    self.eagerPushPeers.remove(msg.sender)
                if msg.sender not in self.lazyPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                    self.lazyPushPeers.append(msg.sender)

            elif msg.type =='IHAVE':
                msgToSend = msgGossip('ACK','','' ,msg.ID,msg.hops,self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,1,0]
                else:
                    self.report[msg.ID][1] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    self.missing.append((msg.ID,msg.sender,msg.hops))
                    # setup timer
                    if msg.ID not in self.timers:
                        self.timers.append(msg.ID)
                        self.reqService(timeout1, "Timer", msg.ID)

            elif msg.type =='GRAFT':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,0,1]
                else:
                    self.report[msg.ID][2] += 1

                if msg.sender not in self.eagerPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                    self.eagerPushPeers.append(msg.sender)
                if msg.sender in self.lazyPushPeers:
                    self.lazyPushPeers.remove(msg.sender)
                if msg.ID in self.receivedMsgs.keys():
                    msgToSend = msgGossip('GOSSIP', self.receivedMsgs[msg.ID].payloadType ,self.receivedMsgs[msg.ID].payload,msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='GOSSIP':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [1,0,0]
                else:
                    self.report[msg.ID][0] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    msgToSend = msgGossip('ACK','','',msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                    self.receivedMsgs[msg.ID] = msg

                    if msg.ID in self.timers:
                        self.timers.remove(msg.ID)

                    if msg.payloadType  == "BLOCK":
                        block = Block.from_dict(msg.payload)
                        accepted = self.blockchain.consensus(block)
                        if accepted and block.hash == self.blockchain.head:
                            self.blockchain.remove_confirmed_transactions(block)
                            if accepted and block.previous_hash == self.blockchain.last_block.hash:
                                self.mining = False

                        self.LazyPush(msg)
                        self.EagerPush(msg)            

                    elif msg.payloadType  == "TRX":
                        if self.miner:
                            self.blockchain.add_new_transaction(msg.payload)
                            if self.miner and not self.mining and len(self.blockchain.unconfirmed_transactions) >= 100:
                                self.mining = True
                                avg_mining_time = 30
                                delay = random.expovariate(1/avg_mining_time)
                                self.reqService(delay, "mine_block", "none")

                        self.LazyPush(msg)
                        self.EagerPush(msg)

                    if msg.sender not in self.eagerPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                        self.eagerPushPeers.append(msg.sender)
                    if msg.sender in self.lazyPushPeers:
                        self.lazyPushPeers.remove(msg.sender)
                else:
                    if msg.sender in self.eagerPushPeers:
                        self.eagerPushPeers.remove(msg.sender)
                    if msg.sender not in self.lazyPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                        self.lazyPushPeers.append(msg.sender)

                    msgToSend = msgGossip('PRUNE','','',msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='BROADCAST':
                mID = msg.ID
                msg.type = 'GOSSIP'
                self.EagerPush(msg)
                self.LazyPush(msg)
                self.receivedMsgs[mID] = msg
                self.report[msg.ID] = [0,0,0]

            elif msg.type =='ACK':
                #remover lista de timers
                if msg.sender in self.timersAck.keys():
                    val = False
                    for pair in self.timersAck[msg.sender]:
                        if val == False and pair[0] == msg.ID and pair[1] == msg.hops:
                             self.timersAck[msg.sender].remove(pair)
                             val = True


    def mine_block(self, *args):
        if not self.active or not self.mining:
            return
        self.blockchain.mine()
        new_block = self.blockchain.last_block
        block_id = "B-" +str(random.randint(11111111,99999999))
        block_msg = msgGossip('GOSSIP',"BLOCK", new_block.to_dict(), block_id, 0, self.node_idx)        
        self.receivedMsgs[block_msg.ID] = block_msg
        self.report[block_msg.ID] = [1, 0, 0]
        self.LazyPush(block_msg)
        self.EagerPush(block_msg)
        self.mining = False

    def EagerPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('GOSSIP',msg.payloadType ,msg.payload,msg.ID,msg.hops + 1,self.node_idx)        
        for n in self.eagerPushPeers:
            if n != sender:
                #create timer to receive ack
                if n not in self.timersAck.keys():
                    self.timersAck[n] = []
                self.timersAck[n].append((msg.ID, msg.hops + 1))
                self.reqService(lookahead * timerPTacks, "TimerAcks", (n, msg.ID, msg.hops + 1))
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", n)

    def LazyPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('IHAVE','',msg.payload,msg.ID,msg.hops + 1,self.node_idx)
        for n in self.lazyPushPeers:
            if n != sender:
                #create timer to receive ack
                if n not in self.timersAck.keys():
                    self.timersAck[n] = []
                self.timersAck[n].append((msg.ID, msg.hops + 1))
                self.reqService(lookahead * delayLazy * timerPTacks, "TimerAcks", (n, msg.ID, msg.hops + 1))
                self.reqService(lookahead * delayLazy, "PlumTreeGossip", msgToSend, "Node", n)

    def Optimization(self, mID, round, sender):
        val = True
        for pair in self.missing:
            if val and pair[0] == mID:
                if pair[2] < round and round - pair[2] >= threshold:
                    val = False
                    msgToSend = msgGossip('PRUNE','','',mID,round,self.node_idx)
                    msgToSend = msgGossip('GRAFT','','',mID,round,self.node_idx)

    def NeighborUP(self, node):
        if node not in self.eagerPushPeers:
            self.eagerPushPeers.append(node)

    def NeighborDown(self, node):
        if node in self.eagerPushPeers:
            self.eagerPushPeers.remove(node)
        if node in self.lazyPushPeers:
            self.lazyPushPeers.remove(node)

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
                if m[1] not in self.eagerPushPeers and any(entry.node_idx == m[1] for entry in self.partial_view):
                    self.eagerPushPeers.append(m[1])
                if m[1] in self.lazyPushPeers:
                    self.lazyPushPeers.remove(m[1])

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
                #self.out.write("ack n chegou no node:"+ str(self.node_idx)+"\n")
                self.NodeFailure(dest)
                self.timersAck[dest] = []
                self.NeighborDown(dest)

#--------------------------------------- PEER SELECTION ---------------------------------------------------
    
    def Dimple(self, *args):
        msg = args[0]
        if self.active:
            if msg.type == 'VIEW_EXCHANGE_REQUEST':
                # Q receives P's subset, prepares reply
                own_subset = random.sample(self.partial_view, min(shuffleSize, len(self.partial_view)))
        
                # Q does NOT update its own view!
                reply = msgDimple("VIEW_EXCHANGE_RESPONSE", [own_subset, msg.payload], self.node_idx)
                self.reqService(lookahead, "Dimple", reply, "Node", msg.sender)
                
            elif msg.type == 'VIEW_EXCHANGE_RESPONSE':
                self.timerDimple.remove(msg.sender)
                received_subset, sent_subset = msg.payload
                self.ExchangeProcedure(received_subset, sent_subset)

                # Repeat Reinforcement l times in parallel.
                for i in range(shuffleSize):
                    msg = msgDimple("REINFORCEMENT", None , self.node_idx)
                    self.reqService(lookahead, "Dimple", msg, "Node", self.node_idx)

            elif msg.type == 'REINFORCEMENT':
                node_toreinforce = max(self.partial_view, key=lambda x: x.age)
                self.timerDimple.append(node_toreinforce.node_idx)
                self.reqService(dimpleTimer, "TimerDimple", node_toreinforce.node_idx)
                dimpleMsg = msgDimple('REINFORCEMENT_INITIATE',self.node_idx, self.node_idx)
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", node_toreinforce.node_idx)

            elif msg.type == 'REINFORCEMENT_INITIATE':
                response = self.ReinforcementInitiateProcedure(msg.payload)
                #print(response)
                dimpleMsg = msgDimple('REINFORCEMENT_RESPONSE',[response, msg.payload] , self.node_idx)
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", msg.sender)
                
            elif msg.type == 'REINFORCEMENT_RESPONSE':
                response_entry, _ = msg.payload   # Q sent back an evicted entry or None
                self.timerDimple.remove(msg.sender)
            
                # Use msg.sender (Q's id) as the entry_to_replace_id
                if response_entry is not None:
                    self.ReinforcementResponseProcedure(response_entry, msg.sender)

            elif msg.type == 'JOIN':
                # JOIN Request → Q returns local view built from visited[-2] or fallback
                local_view = []
                for entry in self.partial_view:
                    local_view.append(entry.visited[-1])
           
                # Send join_candidates to P
                response = msgDimple("JOIN_RESPONSE", local_view, self.node_idx)
                self.reqService(lookahead, "Dimple", response, "Node", msg.sender)
                
            elif msg.type == 'JOIN_RESPONSE':
                # Node P gets view, performs reinforcements
                candidates = msg.payload

                for peer_id in candidates:
                    if peer_id != self.node_idx:
                        self.timerDimple.append(peer_id)
                        self.reqService(dimpleTimer, "TimerDimple", peer_id)
                        reinforce_msg = msgDimple("REINFORCEMENT_INITIATE", self.node_idx,self.node_idx)
                        self.reqService(lookahead, "Dimple", reinforce_msg, "Node", peer_id)
                
                for candidate in candidates: 
                    if candidate != self.node_idx and all(entry.node_idx != candidate for entry in self.partial_view):
                        self.partial_view.append(partialViewEntry(candidate, 0, [candidate, self.node_idx]))
                
                self.UpdatePlumTreePeers()
                self.reqService(shuffleTime, "DimpleShuffle", "none") 
                
    
    def DimpleShuffle(self, *args):

        #print(f"Node {self.node_idx} Partial View before shuffle: {[str(entry) for entry in self.partial_view]}")
        # Age all entries
        for entry in self.partial_view:
            entry.age += 1

        # Select the oldest entry as the target Q
        oldest_entry = max(self.partial_view, key=lambda e: e.age)

        # Select l-1 random peers + self
        eligible = [e for e in self.partial_view if e.node_idx != oldest_entry.node_idx]
        subset = random.sample(eligible, min(shuffleSize - 1, len(eligible)))
        subset.append(partialViewEntry(self.node_idx, 0, [self.node_idx]))

        # Set timeout
        self.timerDimple.append(oldest_entry.node_idx)
        self.reqService(dimpleTimer, "TimerDimple", oldest_entry.node_idx)

        # Send VIEW_EXCHANGE_REQUEST to Q
        msg = msgDimple("VIEW_EXCHANGE_REQUEST", subset, self.node_idx)
        self.reqService(lookahead, "Dimple", msg, "Node", oldest_entry.node_idx)
    
        # Reschedule next shuffle
        self.reqService(shuffleTime, "DimpleShuffle", "none")


    def ReinforcementInitiateProcedure(self, received_entry_id):

        existing_ids = {entry.node_idx for entry in self.partial_view}

        #If already present, reset age

        if received_entry_id in existing_ids:
            for entry in self.partial_view:
                if entry.node_idx == received_entry_id:
                    entry.age = 0
                    return None
        
        # If space available, add directly
        if len(self.partial_view) < maxPartialView:
            new_entry = partialViewEntry(received_entry_id, 0, [received_entry_id, self.node_idx])
            self.partial_view.append(new_entry)
            self.UpdatePlumTreePeers()
            return None  

        # Replace a random entry
        to_replace = random.randrange(len(self.partial_view))
        evicted_entry = self.partial_view[to_replace]
        self.partial_view[to_replace] = partialViewEntry(received_entry_id, 0, [received_entry_id, self.node_idx])
        self.UpdatePlumTreePeers()
        return evicted_entry  # Send this back to P
    
    def ReinforcementResponseProcedure(self, response_entry, entry_to_replace_id):

        existing_ids = {entry.node_idx for entry in self.partial_view}

        if response_entry.node_idx in existing_ids:
            for entry in self.partial_view:
                if entry.node_idx == response_entry.node_idx:
                    entry.age = 0
                    return

        # If P receives a valid response, P adds the node information to an empty slot if there is one
        if len(self.partial_view) < maxPartialView:
            self.partial_view.append(response_entry)
            self.UpdatePlumTreePeers()
            return

        # Try to replace the entry for the node we just reinforced (entry_to_replace_id)
        for i, entry in enumerate(self.partial_view):
            if entry.node_idx == entry_to_replace_id:
                self.partial_view[i] = response_entry
                self.UpdatePlumTreePeers()
                return



    def ExchangeProcedure(self, received_subset, sent_subset):
        current_ids = {entry.node_idx for entry in self.partial_view}

        sent_ids = [entry.node_idx for entry in sent_subset]
        sent_slot_indices = [i for i, e in enumerate(self.partial_view) if e.node_idx in sent_ids]

        for received_entry in received_subset:
            if received_entry.node_idx == self.node_idx or received_entry.node_idx in current_ids:
                continue

            new_visited = received_entry.visited.copy()
            if self.node_idx not in new_visited:
                new_visited.append(self.node_idx)

            new_entry = partialViewEntry(received_entry.node_idx, 0, new_visited)

            # First try to put into an empty slot
            if len(self.partial_view) < maxPartialView:
                self.partial_view.append(new_entry)
                current_ids.add(new_entry.node_idx)
                continue

            # Otherwise, replace one of the slots we originally sent to Q (one slot per received entry).
            if sent_slot_indices:
                idx_to_replace = sent_slot_indices.pop(0)

                old_node = self.partial_view[idx_to_replace].node_idx
                current_ids.discard(old_node)

                self.partial_view[idx_to_replace] = new_entry
                current_ids.add(new_entry.node_idx)

                continue

        self.UpdatePlumTreePeers()


    def NodeFailure(self, node_id):
        self.partial_view = [entry for entry in self.partial_view if entry.node_idx != node_id]
        self.NeighborDown(node_id)
        
    def TimerDimple(self, *args):
        dest = args[0]
        if dest in self.timerDimple:
            self.timerDimple.remove(dest)
            self.NodeFailure(dest)

    def UpdatePlumTreePeers(self):
        current_peers = [entry.node_idx for entry in self.partial_view if entry.node_idx != self.node_idx]

        # Reset eager from partial_view
        self.eagerPushPeers = current_peers

        # Retain lazy peers only if they still exist in partial_view
        self.lazyPushPeers = [peer for peer in self.lazyPushPeers if peer in current_peers]

 #--------------------------------------- TRIGGERS ---------------------------------------------------#   
      
    def TriggerSystemReport(self,*args):
        if self.active:
            messageMetrics = []
            for m in self.receivedMsgs.keys():
                messageMetrics.append( {
                                        'id': m,
                                        'hopsDone': self.receivedMsgs[m].hops,
                                        'gossip': self.report[m][0],
                                        'ihave': self.report[m][1],
                                        'graft': self.report[m][2]
                                        })

            diss_degree = len(self.eagerPushPeers) + len(self.lazyPushPeers)

            neighbors = [entry.node_idx for entry in self.partial_view]

            msgToSend = msgReport(
                messageMetrics,
                diss_degree,
                self.miner,
                len(self.blockchain.chain),
                self.trxMade,
                self.node_idx,
                neighbors
            )

            self.reqService(lookahead, "SystemReport", msgToSend , "ReportNode", 0)

    def BecomeMiner(self, *args):
        self.miner = True

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
        avg_transactionT = 1.4
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
    simianEngine.schedService(lookahead, "BecomeMiner", "", "Node", n)
    upNodes.remove(n)

n = random.choice(upNodes)

simianEngine.schedService(250 + lookahead , "create_transaction","" , "Node", n)


simianEngine.run()
simianEngine.exit()