import simian
from simian import Simian
import random, math, argparse, textwrap

parser = argparse.ArgumentParser(
    description='Simple Gossip Protocol Simulation.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int, help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float, help='simulation end time')
parser.add_argument("--msgs", type=int, metavar='NMSGS', default=0,help="number of broadcast messages -> default 0")
parser.add_argument('--seedR', type=int, metavar='SEED', default=10, help='seed for random')
parser.add_argument("-d", "--distance", type=float, metavar='DISTANCE', default=100.0, help="maximum distance to be neighbors")
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1, help="min delay of mailboxes")
parser.add_argument("-r", "--maxrounds", type= int, default= 10, help = "maximum number of times a message is retransmitted by nodes" )
parser.add_argument("-f", "--fanout", type= int, default= 5,help = "number of nodes that are selected as gossip targets" )
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0, help="use mpi")
parser.add_argument("--multipleSender", type=float, metavar='SENDER', default=0,help="multiple Senders for broadcast messages -> default 0-false")
args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True
nodes = args.total_nodes
lookahead = args.lookahead
maxrounds = args.maxrounds
fanout = args.fanout
random.seed(args.seedR)

simName, startTime, endTime, minDelay = str(args.total_nodes), 0, args.endtime, 0.1
simianEngine = Simian(simName, startTime, endTime, minDelay, uMPI)

# Init grid
positions = []
# Place nodes in grids
for x in range(int(math.sqrt(nodes))):
    for y in range(int(math.sqrt(nodes))):
        px = 50 + x*60 + random.uniform(-20,20)
        py = 50 + y*60 + random.uniform(-20,20)
        positions.append((px,py))

class msg2:
    def __init__(self,mID,m,round):
        self.ID = int(mID)
        self.payload = m
        self.round = int(round)

class Node(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Node, self).__init__(baseInfo)
        self.node_idx = args[0]
        self.total_nodes = args[1]
        self.peers = []
        self.receivedMsgs = {}
        self.GetPeers()

    def Receive(self, *args):
        m = args[0]
        m2 = m.split("-")
        msg = msg2(m2[0],m2[1],m2[2])
        if msg.ID not in self.receivedMsgs.keys() and msg.round < maxrounds:
            self.receivedMsgs[msg.ID] = msg.payload
            print(f"Node {self.node_idx} received message {msg.payload} at time {self.engine.now}")
            self.Gossip(msg)

    def Gossip(self,msg):
        msg.round =+ 1
        for peer in self.peers:
            self.reqService(lookahead, "Receive", ''+str(msg.ID)+"-"+self.receivedMsgs[msg.ID]+"-"+str(msg.round), "Node", peer)
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

        for x in range(rxmin,rxmax):
            for y in range(rymin,rymax):
                peer = (x * lsize) + y
                v1 = positions[self.node_idx][0]-positions[peer][0]
                v2 = positions[self.node_idx][1]-positions[peer][1]
                dist = math.sqrt(v1*v1 + v2*v2)
                if peer!=self.node_idx and dist<args.distance:
                    self.peers.append(peer)     
        random.shuffle(self.peers)
        self.peers= self.peers[:fanout]
    

for i in range(nodes):
    simianEngine.addEntity("Node", Node, i, i, nodes)

available = []
failsList = []

for i in range(0, nodes):
    available.append(i)


if args.msgs > 0:
    msgID = 1
    msgGap = round((endTime - 100) / args.msgs,2)

    if args.multipleSender == 0:
        idx = random.randrange(len(available))
        n = available[idx]

    for i in range(args.msgs):
        if args.multipleSender == 1:
            idx = random.randrange(len(available))
            n = available[idx]

        simianEngine.schedService(lookahead + 50 + (i * msgGap), "Receive", ''+str(msgID)+"-Paquete Num:"+str(i)+"-0", "Node", n)
        msgID+=1

simianEngine.run()
simianEngine.exit()