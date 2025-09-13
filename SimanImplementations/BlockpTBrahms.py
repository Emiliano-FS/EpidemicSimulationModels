from Blockchain import * 
from simian import Simian
import random, math, argparse

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
parser.add_argument("--a", type=float, metavar='ALFA', default=0.5,
                    help="alpha value [0.0 ... 1.0]")
parser.add_argument("--b", type=float, metavar='BETA', default=0.5,
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
churnEndtime = 50 #args.endtime
stabilizationTime = 200 #args.endtime

l1 = math.ceil(math.log(args.total_nodes,10)) + args.c #math.ceil(math.pow(nodes,1.0/3.0))
l2 = math.ceil(math.log(args.total_nodes,10)) + args.c #math.ceil(math.pow(nodes,1.0/3.0))
a = args.a
b = args.b
y = 1 - a - b

downNodes = []
upNodes = []

name = "PlumTreeBrahms/PlumTree + Brahms" + str(args.total_nodes)+'-Seed'+str(args.seedR)+'-Update'+str(args.updateViews)+'-View'+str(args.c)+'-LOOKAHEAD'+str(args.lookahead)+'-CHURN'+str(args.activeChurn)

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
    def __init__(self, msgs, degree, miner, length, trx):
        self.msgsFromNode = msgs
        self.degree = degree
        self.isMiner = miner
        self.lengthChainFromNode = length
        self.trxFromNode = trx

class msgBrahms:
    def __init__(self,type,view,sender):
        self.type = type
        self.view = eval(view)
        self.sender = sender

class Sampler:
    def __init__(self):
        self.h = random.randrange(0,1000)
        #random.seed(self.h)
        #self.state = random.getstate()
        self.q = -1

    def next(self,elem):
        if self.q == -1:
            self.q = elem
        else:
            helem = random.randrange(0,nodes)
            hq = random.randrange(0,nodes)
            if helem < hq:
                self.q = elem

    def sample(self):
        return self.q

    def toString(self):
        return str(self.q)
    
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
        avg_chain_len_pct = (self.chainLengths / node_count) / self.longestChain * 100 if self.longestChain else 0

        # Print summary
        self.out.write(f"\nNumber of Miners: {self.totalMiners}  Total Transactions: {self.totalTransactions}  "
                       f"Longest Chain: {self.longestChain}  Avg Chain Length: {avg_chain_len_pct:.2f}%\n")
        self.out.write(f"AVERAGE -- Reliability:{avRel:.3f}%  Nodes:{avNodes:.1f}  Latency:{avLat:.1f}  "
                       f"RMR:{avRmr:.3f}  Gossip:{avGossip:.1f}  IHAVE:{avInv:.1f}  GRAFT:{avReq:.1f}\n")
        self.out.write(f"Degree Avg:{avg_degree}  Min:{self.minDegree}  Max:{self.maxDegree}  "
                       f"Shortest Path Avg:{avg_shortest_path:.2f}\n")


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

        self.report = {}
        self.timersAck = {}

        #brahms variables
        self.S = []
        self.V = []
        self.Vpush = []
        self.Vpull = []

        contactNode = self.node_idx - 1 #random.randrange(nodes)
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

        self.reqService(triggerSysReportTime, "TriggerSystemReport", "none")



#--------------------------------------- GOSSIP ---------------------------------------------------

    def PlumTreeGossip(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.hops)))
        if self.active==True:
            if msg.type =='PRUNE':
                if msg.sender in self.eagerPushPeers:
                    self.eagerPushPeers.remove(msg.sender)
                    if msg.sender not in self.lazyPushPeers:
                        self.lazyPushPeers.append(msg.sender)

            elif msg.type =='IHAVE':
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

                if msg.sender in self.lazyPushPeers:
                    self.lazyPushPeers.remove(msg.sender)
                    if msg.sender not in self.eagerPushPeers:
                        self.eagerPushPeers.append(msg.sender)
                if msg.ID in self.receivedMsgs.keys():
                    msgToSend = msgGossip('GOSSIP',self.receivedMsgs[msg.ID].payloadType,self.receivedMsgs[msg.ID].payload,msg.ID,msg.hops,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='GOSSIP':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [1,0,0]
                else:
                    self.report[msg.ID][0] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    self.receivedMsgs[msg.ID] = msg

                    if msg.ID in self.timers:
                        self.timers.remove(msg.ID)

                    if msg.payloadType  == "BLOCK":
                        block = Block.from_dict(msg.payload)
                        accepted = self.blockchain.consensus(block)
                        if accepted:
                            self.blockchain.remove_confirmed_transactions(block)
                            if self.mining:
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

                    if msg.sender in self.lazyPushPeers:
                        self.lazyPushPeers.remove(msg.sender)

                        if msg.sender not in self.eagerPushPeers:
                            self.eagerPushPeers.append(msg.sender)
                else:
                    if msg.sender in self.eagerPushPeers:
                        self.eagerPushPeers.remove(msg.sender)
                        if msg.sender not in self.lazyPushPeers:
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
        msgToSend = msgGossip('GOSSIP',msg.payloadType,msg.payload,msg.ID,msg.hops + 1,self.node_idx)
        for n in self.eagerPushPeers:
            if n != sender:
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", n)

    def LazyPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('IHAVE','',msg.payload,msg.ID,msg.hops + 1,self.node_idx)
        for n in self.lazyPushPeers:
            if n != sender:
                self.reqService(lookahead * delayLazy, "PlumTreeGossip", msgToSend, "Node", n)

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
                if m[1] in self.V:
                    if m[1] not in self.eagerPushPeers:
                        self.eagerPushPeers.append(m[1])
                    if m[1] in self.lazyPushPeers:
                        self.lazyPushPeers.remove(m[1])

                msgToSend = msgGossip('GRAFT','','',mID,m[2],self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", m[1])

                self.reqService(timeout2, "Timer", mID)

#--------------------------------------- PEER SELECTION ---------------------------------------------------



    def BrahmsInit(self, v0):
        self.V = v0
        self.eagerPushPeers = v0

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
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.sender)))
        if self.active:
            if msg.type == 'PUSH':
                if msg.sender not in self.Vpush:
                    self.Vpush.append(msg.sender)
            elif msg.type == 'PULL':
                msgToSend = msgBrahms('PULL_REPLY',str(self.V),self.node_idx)
                self.reqService(lookahead, "Brahms", msgToSend, "Node", msg.sender)
            elif msg.type == 'PULL_REPLY':
                if len(self.Vpull) == 0:
                    v2 = msg.view
                    if self.node_idx in msg.view:
                        v2.remove(self.node_idx)

                    self.Vpull = self.Vpull + v2

    def NodeFailure(self,node):
        #self.out.write("FAIL DETECTED ---------\n")
        if node in self.V:
            self.V.remove(node)



#--------------------------------------- TRIGGERS ---------------------------------------------------

    def TriggerBrahmsSend(self, *args):
        #self.out.write(str(self.engine.now) + (":%d: %s %s %s\n" % (self.node_idx, str(self.Vpull),str(self.Vpush),str(self.V))))
        if self.active==True:
            #self.Vpull = list( dict.fromkeys(self.Vpull) )
            if len(self.Vpush) <= (math.ceil(a*l1)) and len(self.Vpush) != 0 and len(self.Vpull) != 0:
                #sample = []
                #for s in self.S:
                #    if s.q not in sample:
                #        sample.append(s.q)

                self.V = list( dict.fromkeys( self.rand(self.Vpush,math.ceil(a*l1)) + self.rand(self.Vpull,math.floor(b*l1))  )   ) #+ self.rand(self.S,math.ceil(y*l1))
                #self.out.write(str(self.engine.now) + (":%d: %s %s %s\n" % (self.node_idx, str(self.Vpull),str(self.Vpush),str(self.V))))
                listE = []
                listL = []
                for n in self.V:
                    if n in self.lazyPushPeers:
                        listL.append(n)
                    else:
                        listE.append(n)

                self.eagerPushPeers = listE
                self.lazyPushPeers = listL
            #self.updateSample(list( dict.fromkeys(self.Vpush + self.Vpull) ) )


            self.Vpush = []
            self.Vpull = []

            if len(self.V) > 0:
                for i in range(math.floor(a * l1)):
                    idx = random.randrange(len(self.V))
                    n = self.V[idx]
                    msgToSend = msgBrahms('PUSH',"[]",self.node_idx)
                    self.reqService(lookahead, "Brahms", msgToSend, "Node", n)
                
                for i in range(math.floor(b * l1)):
                    idx = random.randrange(len(self.V))
                    n = self.V[idx]
                    msgToSend = msgBrahms('PULL',"[]",self.node_idx)
                    self.reqService(lookahead, "Brahms", msgToSend, "Node", n)

            if self.engine.now < churnEndtime:
                self.reqService(TriggerBrahmsTime, "TriggerBrahmsSend", "none")
            elif self.engine.now < stabilizationTime:
                self.reqService(TriggerBrahmsTime2, "TriggerBrahmsSend", "none")

    def TriggerSystemReport(self,*args):
        if self.active:
            messageMetrics = []
            degree = len(self.eagerPushPeers) + len(self.lazyPushPeers)
            for m in self.receivedMsgs.keys():
                messageMetrics.append( {
                                        'id': m,
                                        'hopsDone': self.receivedMsgs[m].hops,
                                        'gossip': self.report[m][0],
                                        'ihave': self.report[m][1],
                                        'graft': self.report[m][2]
                                        })

            msgToSend = msgReport(messageMetrics, degree, self.miner, len(self.blockchain.chain), self.trxMade)
            self.reqService(lookahead, "SystemReport", msgToSend , "ReportNode", 0)


    def nodeFail(self, *args):
        #self.out.write("FAIL NODE "+str(self.node_idx)+'\n')
        if self.active:
            self.active = False
        else:
            self.active = True

    def printViews(self, *args):
        res = ''
        # for m in self.receivedMsgs.keys():
        #     res += self.receivedMsgs[m].toString() + " "
        sampleStr = ''
        for i in range(len(self.S)):
            sampleStr += self.S[i].toString() + '-'
        if self.active:
            self.out.write("%d:Peers %s %s msg %s\n"%(self.node_idx,str(self.V),str(sampleStr),res))
    
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

    def create_transaction(self,*args):
        n = random.choice(upNodes)
        avg_transactionT = .7
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