import simian
from simian import Simian
import random, math, argparse, textwrap

parser = argparse.ArgumentParser(
    description='The PHOLD model.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int,
                    help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float,
                    help='simulation end time')
parser.add_argument("-d", "--distance", type=float, metavar='DISTANCE', default=75.0,
                    help="maximum distance to be neighbors")
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1,
                    help="min delay of mailboxes")
parser.add_argument("--seedR", type=int, metavar='SEED', default=10,
                    help="seed for random")
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0,
                    help="use mpi")
parser.add_argument("--useHPV", type=int, metavar='MPI', default=0,
                    help="use HyParView")
parser.add_argument("--failRate", type=float, metavar='MPI', default=0.0,
                    help="use HyParView")
args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True

nodes = args.total_nodes
lookahead = args.lookahead
failRate = args.failRate
random.seed(args.seedR)
triggerSysReportTime = 199

if args.useHPV == 0:
    # Init grid
    positions = []
    # Place nodes in grids
    for x in range(int(math.sqrt(nodes))):
        for y in range(int(math.sqrt(nodes))):
            px = 50 + x*60 + random.uniform(-20,20)
            py = 50 + y*60 + random.uniform(-20,20)
            positions.append((px,py))
else:
    c = 1
    k = 6
    ARWL = 6
    PRWL = 3
    triggerpVMaintain = 200
    maxActiveView = math.ceil(math.log(args.total_nodes,10)) + c
    maxPassiveView = math.ceil(math.log(args.total_nodes,10) + c) * k
    aVShuffleSize = math.ceil(maxActiveView/2)
    pVShuffleSize = math.ceil(maxPassiveView/6)


simName, startTime, endTime, minDelay, useMPI, mpiLib =  str(args.total_nodes), 0, args.endtime, args.lookahead, uMPI, "/usr/lib/x86_64-linux-gnu/libmpich.so"
simianEngine = Simian(simName, startTime, endTime, minDelay, useMPI)

timeout1 = 1
timeout2 = 2

class msgGossip:
    def __init__(self,type,m,mID,round,sender):
        self.type = type
        self.payload = m
        self.ID = int(mID)
        self.round = int(round)
        self.sender = int(sender)

    def toString(self):
        return '%s-%s-%d-%d-%d'%(self.type,self.payload,self.ID,self.round,self.sender)

class msgHPV:
    def __init__(self,type,newNode,timeToLive,sender):
        self.type = type
        self.newNode = int(newNode)
        self.timeToLive = int(timeToLive)
        self.sender = int(sender)

    def toString(self):
        return '%s-%d-%d-%d'%(self.type,self.newNode,self.timeToLive,self.sender)

class msgHPVShuffle:
    def __init__(self,type,activeView,passiveView,node,timeToLive,sender):
        self.type = type
        self.activeView = eval(activeView)
        self.passiveView = eval(passiveView)
        self.node = int(node)
        self.timeToLive = int(timeToLive)
        self.sender = int(sender)

    def toString(self):
        return '%s-%s-%s-%d-%d-%d'%(self.type,str(self.activeView),str(self.passiveView),self.node,self.timeToLive,self.sender)

class msgReport:
    def __init__(self,type,msgs):
        self.type = type
        self.msgs = msgs



class ReportNode(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ReportNode, self).__init__(baseInfo)

        #report variables
        self.reliability = {}
        self.latency = {}
        self.redundancy = {}
        self.reqService(endTime, "PrintSystemReport", "none")

    def SystemReport(self,*args):
        msg = args[0]

        for m in msg.msgs:
            id = m[0]
            if id not in self.latency.keys() or self.latency[id] < m[1]:
                self.latency[id] = m[1]
            if id not in self.reliability.keys():
                self.reliability[id] = 0
            if id not in self.redundancy.keys():
                self.redundancy[id] = [0,0,0]

            self.reliability[id] += 1
            self.redundancy[id][0] += m[2]
            self.redundancy[id][1] += m[3]
            self.redundancy[id][2] += m[4]

    def PrintSystemReport(self,*args):
        for id in self.reliability.keys():
            r = self.reliability[id]
            reliability = round(r / (nodes * (1-failRate)) * 100,3)
            lat = self.latency[id]
            rmr = round((self.redundancy[id][0] / (r - 1)) - 1,3)
            self.out.write("%d--Reliability:%.3f%%   Latency:%d   RMR:%.3f        Gossip:%d   Ihave:%d   Graft:%d\n\n"%(id,reliability,lat,rmr,self.redundancy[id][0],self.redundancy[id][1],self.redundancy[id][2]))



class Node(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(Node, self).__init__(baseInfo)
        self.total_nodes = int(args[1])
        self.node_idx = int(args[0])
        self.active = True

        #plumTree variables
        self.eagerPushPeers = []
        self.lazyPushPeers = []
        self.lazyQueues = []
        self.missing = []
        self.receivedMsgs = {}
        self.timers = []

        self.report = {}

        #hyparview variables
        self.activeView = []
        self.passiveView = []

        if int(args[2]) == 0:
            self.PeersByDistance()
        else:
            # USE HYPARVIEW
            delay = 0.0001
            if self.node_idx != 0:
                contactNode = random.randrange(i)
                msg = msgHPV('JOIN',0,0,self.node_idx)
                self.activeView.append(contactNode)
                self.eagerPushPeers.append(contactNode)
                delay = 0.0001 * self.node_idx
                self.reqService(lookahead + delay, "HyParView", msg, "Node", contactNode)
            self.reqService(lookahead + delay, "TriggerPassiveViewMaintain", "none")

        self.reqService(lookahead, "TriggerSystemReport", "none")

#--------------------------------------- GOSSIP ---------------------------------------------------

    def PlumTreeGossip(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.sender)))
        if self.active:
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
                    self.missing.append((msg.ID,msg.sender,msg.round))
                    # setup timer
                    if msg.ID not in self.timers:
                        self.timers.append(msg.ID)
                        self.reqService(0, "Timer", msg.ID)

            elif msg.type =='GRAFT':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,0,1]
                else:
                    self.report[msg.ID][2] += 1

                if msg.sender not in self.eagerPushPeers:
                    self.eagerPushPeers.append(msg.sender)
                if msg.sender in self.lazyPushPeers:
                    self.lazyPushPeers.remove(msg.sender)
                if msg.ID in self.receivedMsgs.keys():
                    msgToSend = msgGossip('GOSSIP',self.receivedMsgs[msg.ID].payload,msg.ID,msg.round,self.node_idx)
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

                    self.LazyPush(msg)
                    self.EagerPush(msg)

                    if msg.sender not in self.eagerPushPeers:
                        self.eagerPushPeers.append(msg.sender)
                    if msg.sender in self.lazyPushPeers:
                        self.lazyPushPeers.remove(msg.sender)
                else:
                    if msg.sender in self.eagerPushPeers:
                        self.eagerPushPeers.remove(msg.sender)
                    if msg.sender not in self.lazyPushPeers:
                        self.lazyPushPeers.append(msg.sender)

                    msgToSend = msgGossip('PRUNE','',0,0,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='BROADCAST':
                mID = msg.ID
                msg.type = 'GOSSIP'
                self.EagerPush(msg)
                self.LazyPush(msg)
                self.receivedMsgs[mID] = msg
                self.report[msg.ID] = [0,0,0]


    def EagerPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('GOSSIP',msg.payload,msg.ID,msg.round + 1,self.node_idx)
        c = 0
        for n in self.eagerPushPeers:
            if n != sender:
                c+=1
                self.reqService(lookahead * c, "PlumTreeGossip", msgToSend, "Node", n)

    def LazyPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('IHAVE',msg.payload,msg.ID,msg.round + 1,self.node_idx)
        for n in self.lazyPushPeers:
            if n != sender:
                self.reqService(lookahead * math.log(nodes,10), "PlumTreeGossip", msgToSend, "Node", n)

    def Timer(self,*args):
        mID = args[0]
        if mID in self.timers:
            m = (mID,0,0)
            for p in self.missing:
                if p[0] == mID:
                    m = p
                    self.missing.remove(p)

            if m[1] not in self.eagerPushPeers:
                self.eagerPushPeers.append(m[1])
            if m[1] in self.lazyPushPeers:
                self.lazyPushPeers.remove(m[1])

            msgToSend = msgGossip('GRAFT','',mID,m[2],self.node_idx)
            self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", m[1])

            self.reqService(timeout2, "Timer", mID)





#--------------------------------------- PEER SELECTION ---------------------------------------------------

    def HyParView(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %d\n" % (self.node_idx, msg.type,msg.sender)))
        if msg.type =='JOIN':
            if len(self.activeView) == maxActiveView:
                self.dropRandomElementFromActiveView()
            newNode = msg.sender
            self.activeView.append(newNode)
            self.eagerPushPeers.append(newNode)
            for n in self.activeView:
                if n != newNode:
                    msgToSend = msgHPV('FORWARDJOIN',newNode,ARWL,self.node_idx)
                    self.reqService(lookahead, "HyParView", msgToSend, "Node", n)

        elif msg.type =='FORWARDJOIN':
            if msg.timeToLive == 0 or len(self.activeView) <= 1:
                self.addNodeActiveView(msg.newNode)
            else:
                if msg.timeToLive == PRWL:
                    self.addNodePassiveView(msg.newNode)
                idx = random.randrange(len(self.activeView))
                n = self.activeView[idx]
                if n == msg.sender:
                    if idx == len(self.activeView) - 1:
                        n = self.activeView[idx-1]
                    else:
                        n = self.activeView[idx+1]

                msg.timeToLive -= 1
                msg.sender = self.node_idx
                self.reqService(lookahead, "HyParView", msg, "Node", n)


        elif msg.type =='DISCONNECT':
            peer = msg.sender
            if peer in self.activeView:
                self.activeView.remove(peer)
                self.eagerPushPeers.remove(peer)

                n = - 1
                #select node from passiveView
                if len(self.passiveView) > 0:
                    idx = random.randrange(len(self.passiveView))
                    n = self.passiveView[idx]

                self.addNodePassiveView(peer)

                if n != -1:
                    msgToSend = msgHPV('NEIGHBOR',self.node_idx,len(self.activeView),self.node_idx)
                    self.reqService(lookahead, "HyParView", msgToSend, "Node", n)

        elif msg.type =='NEIGHBOR':
            res = 1
            if msg.timeToLive == 0 or len(self.activeView) != maxActiveView:
                self.addNodeActiveView(msg.newNode)
                res = 0

            msgToSend = msgHPV('NEIGHBORREPLY',self.node_idx,res,self.node_idx)
            self.reqService(lookahead, "HyParView", msgToSend, "Node", msg.sender)

        elif msg.type =='NEIGHBORREPLY':
            if msg.timeToLive == 0:
                self.addNodeActiveView(msg.newNode)
            else:
                idx = random.randrange(len(self.passiveView))
                n = self.passiveView[idx]

                msgToSend = msgHPV('NEIGHBOR',self.node_idx,len(self.activeView),self.node_idx)
                self.reqService(lookahead, "HyParView", msgToSend, "Node", n)





    def HyParViewShuffle(self, *args):
        msg = args[0]
        #self.out.write(str(self.engine.now) + (":%d rcvd msg '%s' %s %d\n" % (self.node_idx, msg.type, msg.passiveView,msg.sender)))
        if msg.type =='SHUFFLE':
            msg.timeToLive -= 1
            if msg.timeToLive != 0 and len(self.activeView) > 1:
                #reencaminhar mensagem
                idx = random.randrange(len(self.activeView))
                n = self.activeView[idx]
                if n == msg.sender:
                    if idx == len(self.activeView) - 1:
                        n = self.activeView[idx-1]
                    else:
                        n = self.activeView[idx+1]

                msg.sender = self.node_idx
                self.reqService(lookahead, "HyParViewShuffle", msg, "Node", n)
            else:
                for n in msg.activeView:
                    self.addNodePassiveView(n)

                for n in msg.passiveView:
                    self.addNodePassiveView(n)

                size = aVShuffleSize
                if len(self.activeView) < size:
                    size = len(self.activeView)
                aVShuffle = random.sample(self.activeView,size)

                size = pVShuffleSize
                if len(self.passiveView) < size:
                    size = len(self.passiveView)
                pVShuffle = random.sample(self.passiveView,size)

                msgToSend = msgHPVShuffle('SHUFFLEREPLY',str(aVShuffle),str(pVShuffle),msg.node,msg.timeToLive,self.node_idx)
                #msgToSend = 'SHUFFLEREPLY-%s-%d-%d-%d'%(payload,msg.ID,timeToLive,self.node_idx)
                self.reqService(lookahead, "HyParViewShuffle", msgToSend, "Node", msg.node)

        elif msg.type =='SHUFFLEREPLY':
            for n in msg.activeView:
                self.addNodePassiveView(n)

            for n in msg.passiveView:
                self.addNodePassiveView(n)

    def dropRandomElementFromActiveView(self,*args):
        idx = random.randrange(len(self.activeView))
        n = self.activeView[idx]
        msgToSend = msgHPV('DISCONNECT',0,0,self.node_idx)
        #msgToSend = 'DISCONNECT--0-0-'+str(self.node_idx)
        self.reqService(lookahead, "HyParView", msgToSend, "Node", n)
        self.activeView.remove(n)
        self.eagerPushPeers.remove(n)
        self.addNodePassiveView(n)

    def addNodeActiveView(self,*args):
        newNode = args[0]
        if newNode != self.node_idx and newNode not in self.activeView:
            if len(self.activeView) == maxActiveView:
                self.dropRandomElementFromActiveView()
            self.activeView.append(newNode)
            self.eagerPushPeers.append(newNode)

    def addNodePassiveView(self,*args):
        newNode = args[0]
        if newNode != self.node_idx and newNode not in self.activeView and newNode not in self.passiveView:
            if len(self.passiveView) == maxPassiveView:
                idx = random.randrange(len(self.passiveView))
                n = self.passiveView[idx]
                self.passiveView.remove(n)
            self.passiveView.append(newNode)




    def PeersByDistance(self):
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
                    self.eagerPushPeers.append(peer)



#--------------------------------------- TRIGGERS ---------------------------------------------------

    def TriggerPassiveViewMaintain(self, *args):
        if len(self.activeView) > 0:
            idx = random.randrange(len(self.activeView))
            n = self.activeView[idx]

            size = aVShuffleSize
            if len(self.activeView) < size:
                size = len(self.activeView)
            aVShuffle = random.sample(self.activeView,size)

            size = pVShuffleSize
            if len(self.passiveView) < size:
                size = len(self.passiveView)
            pVShuffle = random.sample(self.passiveView,size)

            payload = str(aVShuffle) + '|' + str(pVShuffle)

            msgToSend = msgHPVShuffle('SHUFFLE',str(aVShuffle),str(pVShuffle),self.node_idx,ARWL,self.node_idx)
            #msgToSend = 'SHUFFLE-%s-%d-%d-%d'%(payload,self.node_idx,ARWL,self.node_idx)
            self.reqService(lookahead, "HyParViewShuffle", msgToSend, "Node", n)

        self.reqService(triggerpVMaintain, "TriggerPassiveViewMaintain", "none")

    def TriggerSystemReport(self,*args):
        report = []
        for m in self.receivedMsgs.keys():
            report.append((m,self.receivedMsgs[m].round,self.report[m][0],self.report[m][1],self.report[m][2]))

        msgToSend = msgReport('reply',report)

        self.reqService(lookahead, "SystemReport", msgToSend, "ReportNode", 0)
        self.reqService(triggerSysReportTime, "TriggerSystemReport", "none")


    def printViews(self, *args):
        res = ''
        for m in self.receivedMsgs.keys():
            res += self.receivedMsgs[m].toString() + " "

        self.out.write("%d:Peers %s %s msg %s\n"%(self.node_idx,str(self.eagerPushPeers),str(self.passiveView),res))

    def nodeFail(self, *args):
        if self.active:
            self.active = False
        else:
            self.active = True




for i in range(nodes):
    simianEngine.addEntity("Node", Node, i,i,nodes,args.useHPV)

simianEngine.addEntity("ReportNode", ReportNode, 0,0)

# for i in range(nodes):
#     simianEngine.schedService(10, "printViews", "x", "Node", i)

# failsList = []
# if args.failRate <= 1:
#     fails = int(nodes * args.failRate)
#     for i in range(fails):
#         val = True
#         idx = -1
#         while val:
#             idx = random.randrange(nodes)
#             if idx not in failsList:
#                 val = False
#         failsList.append(idx)
#         simianEngine.schedService(9, "nodeFail", "x", "Node", idx)


msgID = 1
idx = random.randrange(nodes)
for i in range(20):

    delay = lookahead
    msgToSend = msgGossip('BROADCAST','Hi',msgID,0,0)
    simianEngine.schedService(lookahead + (i+1)*10, "PlumTreeGossip", msgToSend, "Node", idx)
    msgID+=1

simianEngine.run()
simianEngine.exit()
