import simian
from simian import Simian
import random, math, argparse, textwrap

parser = argparse.ArgumentParser(
    description='Simple Gossip Protocol Simulation.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int, help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float, help='simulation end time')
parser.add_argument("--msgs", type=int, metavar='NMSGS', default=0, help="number of broadcast messages -> default 0")
parser.add_argument('--seedR', type=int, metavar='SEED', default=10, help='seed for random')
parser.add_argument("-d", "--distance", type=float, metavar='DISTANCE', default=100.0, help="maximum distance to be neighbors")
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1, help="min delay of mailboxes")
parser.add_argument("-r", "--maxrounds", type=int, default=math.inf, help="maximum number of times a message is retransmitted by nodes")
parser.add_argument("-f", "--fanout", type=int, default=5, help="number of nodes that are selected as gossip targets")
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0, help="use mpi")
parser.add_argument("--multipleSender", type=float, metavar='SENDER', default=0, help="multiple Senders for broadcast messages -> default 0-false")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0, help="node fail rate [0.0 ... 1.0]")
args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True
nodes = args.total_nodes
lookahead = args.lookahead
maxrounds = args.maxrounds
fanout = args.fanout
failRate = args.failRate
random.seed(args.seedR)

name = "SimpleGossipTests/"+"SimpleGossip" + str(args.total_nodes)+'-FR'+str(args.failRate)+'-Seed'+str(args.seedR)+'-Sender'+str(args.multipleSender)+'-MGS'+str(args.msgs)+'-LOOKAHEAD'+str(args.lookahead)

simName, startTime, endTime, minDelay = name, 0, args.endtime, 0.1
simianEngine = Simian(simName, startTime, endTime, minDelay, uMPI)

# Init grid
positions = []
# Calculate the grid size
grid_size = math.ceil(math.sqrt(nodes))
spacing = 15

# Place nodes in the grid
for i in range(nodes):
    x = i % grid_size
    y = i // grid_size
    px = 50 + x * spacing + random.uniform(-20, 20)
    py = 50 + y * spacing + random.uniform(-20, 20)
    positions.append((px, py))


class msg2:
    def __init__(self, mID, m, round):
        self.ID = int(mID)
        self.payload = m
        self.round = int(round)

class msgReport:
    def __init__(self, type, msgs, degree):
        self.type = type
        self.msgs = msgs
        self.degree = degree

class ReportNode(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ReportNode, self).__init__(baseInfo)
        self.reliability = {}
        self.latency = {}
        self.redundancy = {}
        self.degree = 0
        self.maxDegree = 0
        self.minDegree = 1000
        self.shortestPath = 0
        self.reqService(endTime, "PrintSystemReport", "none")

    def SystemReport(self, *args):
        msg = args[0]
        if msg.degree > self.maxDegree:
            self.maxDegree = msg.degree
        if msg.degree < self.minDegree:
            self.minDegree = msg.degree
        self.degree += msg.degree

        for m in msg.msgs:
            self.shortestPath += m[1]
            id = m[0]
            if id not in self.latency.keys() or self.latency[id] < m[1]:
                self.latency[id] = m[1]
            if id not in self.reliability.keys():
                self.reliability[id] = 0
            if id not in self.redundancy.keys():
                self.redundancy[id] = 0
            self.reliability[id] += 1
            self.redundancy[id] += m[2]
            

    def PrintSystemReport(self, *args):
        degree = round(self.degree / (nodes * (1 - failRate)), 2)
        avRel = 0
        avNodes = 0
        avLat = 0
        avRmr = 0
        avGossip = 0

        avRel10 = 0
        count = 0

        for id in sorted(self.reliability.keys()):
            r = self.reliability[id]
            reliability = round(r / (nodes * (1 - failRate)) * 100, 3)
            lat = self.latency[id]
            if r <= 1:
                rmr = 0
            else:
                rmr = round((self.redundancy[id] / (r - 1)) - 1, 3)

            avRel += reliability
            avNodes += r
            avLat += lat
            avRmr += rmr
            avGossip += self.redundancy[id]
            

            self.out.write(f"{id}--Reliability: {reliability} %    Nodes: {r}    Latency: {lat}   RMR: {rmr}        Gossip: {self.redundancy[id]}  \n")
            if reliability > avRel10:
                avRel10 = reliability
                count += 1

            if count % 10 == 0:
                id = count / 10
                avRel10 /= 10
                print(f"{id}--Reliability: {avRel10} %\n")
                avRel10 = 0

        msgs = len(self.reliability.keys())
        if msgs > 0:
            avRel /= msgs
            avNodes /= msgs
            avLat /= msgs
            avRmr /= msgs
            avGossip /= msgs
            self.shortestPath /= msgs
            self.shortestPath /= (nodes * (1 - failRate))

            self.out.write(f"AVERAGE--Reliability: {avRel} %    Nodes: {avNodes}    Latency: {avLat}   RMR: {avRmr}        Gossip: {avGossip} \n")
        self.out.write(f"Degree: {degree}  min: {self.minDegree}    max: {self.maxDegree}    shortest path: {self.shortestPath}\n")

class Node(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Node, self).__init__(baseInfo)
        self.node_idx = args[0]
        self.total_nodes = args[1]
        self.peers = []
        self.receivedMsgs = {}
        self.report = {}
        self.active = True
        self.GetPeers()
        self.reqService(endTime - 1, "TriggerSystemReport", "none")

    def Receive(self, *args):
        if not self.active:
            print("Error")
            return
        m = args[0]
        m2 = m.split("-")
        msg = msg2(m2[0], m2[1], m2[2])
        if msg.ID not in self.report.keys():
            self.report[msg.ID] = 1
        else:
            self.report[msg.ID] += 1   

        if msg.ID not in self.receivedMsgs.keys() and msg.round < maxrounds:
            self.receivedMsgs[msg.ID] = msg
            self.Gossip(msg)

    def Gossip(self, msg):
        msg.round += 1
        for peer in self.peers:
            self.reqService(lookahead, "Receive", f'{msg.ID}-{self.receivedMsgs[msg.ID]}-{msg.round}', "Node", peer)
        self.GetPeers()

    def GetPeers(self):
        lsize = int(math.sqrt(args.total_nodes))
        idx = self.node_idx
        xp = idx // lsize
        yp = idx % lsize

        rxmin = xp - 2
        rxmax = xp + 2
        rymin = yp - 2
        rymax = yp + 2

        if rxmin < 0:
            rxmin = 0
        if rxmax > lsize:
            rxmax = lsize
        if rymin < 0:
            rymin = 0
        if rymax > lsize:
            rymax = lsize
        
        
        for x in range(rxmin, rxmax):
            for y in range(rymin, rymax):
                peer = (x * lsize) + y
                v1 = positions[self.node_idx][0] - positions[peer][0]
                v2 = positions[self.node_idx][1] - positions[peer][1]
                dist = math.sqrt(v1 * v1 + v2 * v2)
                if peer != self.node_idx and dist < args.distance:
                    self.peers.append(peer)
        random.shuffle(self.peers)
        
        self.peers = self.peers[0:fanout]
        

    def nodeFail(self, *args):
        if self.active:
            self.active = False
        else:
            self.active = True

    def TriggerSystemReport(self, *args):
        if self.active:
            report = []
            degree = len(self.peers)
            for m in self.receivedMsgs.keys():
                report.append((m, self.receivedMsgs[m].round, self.report[m]))

            msgToSend = msgReport('reply', report, degree)
            self.reqService(lookahead, "SystemReport", msgToSend, "ReportNode", 0)



for i in range(nodes):
    simianEngine.addEntity("Node", Node, i, i, nodes)

simianEngine.addEntity("ReportNode", ReportNode, 0, 0)

available = []

failsList = []
fails = int(nodes * args.failRate)

for i in range(0, nodes):
    available.append(i)

if args.failRate <= 1:
    for i in range(fails):
        idx = random.randrange(len(available))
        n = available[idx]
        failsList.append(n)
        available.remove(n)
        simianEngine.schedService(50, "nodeFail", "x", "Node", n)

if args.msgs > 0:
    msgID = 1
    msgGap = round((endTime - 100) / args.msgs, 2)

    if args.multipleSender == 0:
        idx = random.randrange(len(available))
        n = available[idx]

    for i in range(args.msgs):
        if args.multipleSender == 1:
            idx = random.randrange(len(available))
            n = available[idx]

        simianEngine.schedService(lookahead + 50 + (i * msgGap), "Receive", f'{msgID}-Paquete Num:{i}-0', "Node", n)
        msgID += 1

simianEngine.run()
simianEngine.exit()