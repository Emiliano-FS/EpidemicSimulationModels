import simian
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
parser.add_argument("--msgs", type=int, metavar='NMSGS', default=0,
                    help="number of broadcast messages -> default 0")
parser.add_argument("--updateViews", type=float, metavar='TIME', default=5,
                    help="update passive Views trigger time -> default 5")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0,
                    help="node fail rate [0.0 ... 1.0]")
parser.add_argument("--multipleSender", type=float, metavar='SENDER', default=0,
                    help="multiple Senders for broadcast messages -> default 0-false")
#parser.add_argument("--shuffleSize", type=int, metavar='SHUFFLESIZE', default=4, help="Dimple Shuffle Size")
#parser.add_argument("--maxPartialView", type=int, metavar='MAXPARTIALVIEW', default=4, help="Dimple Maximum Partial View")
#parser.add_argument("--dimpleTimeout", type=float, metavar='DIMPLETIMEOUT', default=1.0, help="DIMPLE protocol timeout")
args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True

## PLUMTREE variables
nodes = args.total_nodes
lookahead = args.lookahead
failRate = args.failRate
random.seed(args.seedR)
triggerSysReportTime = args.endtime - 1
timeout1 = args.lookahead
timeout2 = args.lookahead / 2
threshold = 3

timerPTacks = 2.5
delayLazy = 1
stabilizationTime = 49 #args.endtime

# DIMPLE variables
shuffleSize = math.floor(math.log(nodes,10))
maxPartialView = math.floor(math.log(nodes,10)) * 2 
dimpleTimer = 2.5

name = "DIMPLETests/"+"PlumTree + DIMPLE" + str(args.total_nodes)+'-FR'+str(args.failRate)+'-Seed'+str(args.seedR)+'-Sender'+str(args.multipleSender)+'-MGS'+str(args.msgs)+'-LOOKAHEAD'+str(args.lookahead)

simName, startTime, endTime, minDelay = name, 0, args.endtime, 0.1
simianEngine = Simian(simName, startTime, endTime, minDelay, uMPI)

class msgGossip:
    def __init__(self,type,m,mID,round,sender):
        self.type = type
        self.payload = m
        self.ID = int(mID)
        self.round = int(round)
        self.sender = int(sender)

    def toString(self):
        return '%s-%s-%d-%d-%d'%(self.type,self.payload,self.ID,self.round,self.sender)
    
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
    def toString(self):
        return '%s-%d'%(self.node_idx,self.age)
    
class msgReport:
    def __init__(self, type, msgs, degree):
        self.type = type
        self.msgs = msgs
        self.degree = degree

class ReportNode(simianEngine.Entity):
    def __init__(self, baseInfo, *args):
        super(ReportNode, self).__init__(baseInfo)

        #report variables
        self.reliability = {}
        self.latency = {}
        self.redundancy = {}
        self.degree = 0
        self.maxDegree = 0
        self.minDegree = 1000
        self.shortestPath = 0
        self.reqService(endTime, "PrintSystemReport", "none")

    def SystemReport(self,*args):
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
                self.redundancy[id] = [0,0,0]

            self.reliability[id] += 1
            self.redundancy[id][0] += m[2]
            self.redundancy[id][1] += m[3]
            self.redundancy[id][2] += m[4]

    def PrintSystemReport(self,*args):
        degree = round(self.degree / (nodes * (1-failRate)),2)
        #self.out.write("Degree:%.2f\n"%(degree))
        avRel = 0
        avNodes = 0
        avLat = 0
        avRmr = 0
        avGossip = 0
        avIhave = 0
        avGraft = 0
        
        avRel10 = 0
        count = 0

        for id in sorted(self.reliability.keys()):
            r = self.reliability[id]
            reliability = round(r / (nodes * (1-failRate)) * 100,3)
            lat = self.latency[id]
            if r <= 1:
                rmr = 0
            else:
                rmr = round((self.redundancy[id][0] / (r - 1)) - 1,3)

            avRel += reliability
            avNodes += r
            avLat += lat
            avRmr += rmr
            avGossip += self.redundancy[id][0]
            avIhave += self.redundancy[id][1]
            avGraft += self.redundancy[id][2]

            self.out.write("%d--Reliability:%.3f%%    Nodes:%d    Latency:%d   RMR:%.3f        Gossip:%d   Ihave:%d   Graft:%d\n\n"%(id,reliability,r,lat,rmr,self.redundancy[id][0],self.redundancy[id][1],self.redundancy[id][2]))
            if reliability > avRel10:
                avRel10 = reliability
                count += 1

            if count % 10 == 0:
                id = count / 10
                avRel10 /= 10
                #self.out.write("%d--Reliability:%.3f%%\n\n"%(id,avRel10))
                avRel10 = 0


        msgs = len(self.reliability.keys())
        if msgs > 0:
            avRel /= msgs
            avNodes /= msgs
            avLat /= msgs
            avRmr /= msgs
            avGossip /= msgs
            avIhave /= msgs
            avGraft /= msgs
            self.shortestPath /= msgs
            self.shortestPath /= (nodes * (1-failRate))

            self.out.write("AVERAGE--Reliability:%.3f%%    Nodes:%d    Latency:%.1f   RMR:%.3f        Gossip:%d   Ihave:%d   Graft:%d\n\n"%(avRel,avNodes,avLat,avRmr,avGossip,avIhave,avGraft))
        self.out.write("Degree:%.2f  min:%d    max:%d    shortest path:%.2f\n"%(degree,self.minDegree,self.maxDegree,self.shortestPath))

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
        self.timersAck = {}

        #Report Variables
        self.report = {}
        
        #DIMPLE Variables
        self.partial_view = []
        self.timerDimple = []
        delay = 10 / self.total_nodes
        delay2 = delay * self.node_idx
        if self.node_idx < 3:  # Seed nodes 0, 1, 2
            peer_ids = random.sample([i for i in range(3) if i != self.node_idx], 2)
            for peer_id in peer_ids:
                visited_list = [peer_id, self.node_idx]  # Simulate movement between them
                self.partial_view.append(partialViewEntry(peer_id, 0, visited_list))    
                self.reqService(delay2 + 1, "DimpleShuffle", "none")  
        else:
            contactNode = 0 
            msg = msgDimple('JOIN',[],self.node_idx)
            self.reqService(lookahead + delay2 , "Dimple", msg, "Node", contactNode)
            self.reqService(lookahead + delay2 + 1, "DimpleShuffle", "none")

        
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
                        if val == False and pair[0] == msg.ID and pair[1] == msg.round:
                             self.timersAck[msg.sender].remove(pair)
                             val = True

                if msg.sender in self.eagerPushPeers:
                    self.eagerPushPeers.remove(msg.sender)
                if msg.sender not in self.lazyPushPeers and any(entry.node_idx == msg.sender for entry in self.partial_view):
                    self.lazyPushPeers.append(msg.sender)

            elif msg.type =='IHAVE':
                msgToSend = msgGossip('ACK','',msg.ID,msg.round,self.node_idx)
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [0,1,0]
                else:
                    self.report[msg.ID][1] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    self.missing.append((msg.ID,msg.sender,msg.round))
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
                    msgToSend = msgGossip('GOSSIP',self.receivedMsgs[msg.ID].payload,msg.ID,msg.round,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

            elif msg.type =='GOSSIP':
                if msg.ID not in self.report.keys():
                    self.report[msg.ID] = [1,0,0]
                else:
                    self.report[msg.ID][0] += 1

                if msg.ID not in self.receivedMsgs.keys():
                    msgToSend = msgGossip('ACK','',msg.ID,msg.round,self.node_idx)
                    self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", msg.sender)

                    self.receivedMsgs[msg.ID] = msg

                    if msg.ID in self.timers:
                        self.timers.remove(msg.ID)

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

                    msgToSend = msgGossip('PRUNE','',msg.ID,msg.round,self.node_idx)
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
                        if val == False and pair[0] == msg.ID and pair[1] == msg.round:
                             self.timersAck[msg.sender].remove(pair)
                             val = True

    def EagerPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('GOSSIP',msg.payload,msg.ID,msg.round + 1,self.node_idx)
        for n in self.eagerPushPeers:
            if n != sender:
                #create timer to receive ack
                if n not in self.timersAck.keys():
                    self.timersAck[n] = []
                self.timersAck[n].append((msg.ID, msg.round + 1))
                self.reqService(lookahead * timerPTacks, "TimerAcks", (n, msg.ID, msg.round + 1))
                self.reqService(lookahead, "PlumTreeGossip", msgToSend, "Node", n)

    def LazyPush(self, msg):
        sender = msg.sender
        msgToSend = msgGossip('IHAVE',msg.payload,msg.ID,msg.round + 1,self.node_idx)
        for n in self.lazyPushPeers:
            if n != sender:
                #create timer to receive ack
                if n not in self.timersAck.keys():
                    self.timersAck[n] = []
                self.timersAck[n].append((msg.ID, msg.round + 1))
                self.reqService(lookahead * delayLazy * timerPTacks, "TimerAcks", (n, msg.ID, msg.round + 1))
                self.reqService(lookahead * delayLazy, "PlumTreeGossip", msgToSend, "Node", n)

    def Optimization(self, mID, round, sender):
        val = True
        for pair in self.missing:
            if val and pair[0] == mID:
                if pair[2] < round and round - pair[2] >= threshold:
                    val = False
                    msgToSend = msgGossip('PRUNE','',mID,round,self.node_idx)
                    msgToSend = msgGossip('GRAFT','',mID,round,self.node_idx)

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

                msgToSend = msgGossip('GRAFT','',mID,m[2],self.node_idx)
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
                selected_peers =  random.sample(self.partial_view, min(len(self.partial_view), shuffleSize))
                self.ExchangeProcedure(msg.payload, selected_peers)
                dimpleMsg = msgDimple('VIEW_EXCHANGE_RESPONSE', [selected_peers, msg.payload], self.node_idx)
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", msg.sender)
                
            elif msg.type == 'VIEW_EXCHANGE_RESPONSE':
                self.timerDimple.remove(msg.sender)
                self.ExchangeProcedure(msg.payload[0], msg.payload[1])
                #print("VIEW EXCHANGE FOR" + str(self.node_idx)+ " IS DONE")

            elif msg.type == 'REINFORCEMENT':
                if len(self.partial_view) > 0:
                    node_toreinforce = msg.payload[1]
                    if msg.payload[0] == False:
                        node_toreinforce = max(self.partial_view, key=lambda x: x.age)
                    #print("STARTED REINFORCEMENT FOR " + str(self.node_idx)+ " WITH " + str(node_toreinforce.node_idx))
                    self.timerDimple.append(node_toreinforce.node_idx)
                    self.reqService(dimpleTimer, "TimerDimple", node_toreinforce.node_idx)
                    dimpleMsg = msgDimple('REINFORCEMENT_INITIATE',partialViewEntry(self.node_idx,0,[node_toreinforce.node_idx]) , self.node_idx)
                    self.reqService(lookahead, "Dimple", dimpleMsg , "Node", node_toreinforce.node_idx)

            elif msg.type == 'REINFORCEMENT_INITIATE':
                response = self.ReinforcementInitiateProcedure(msg.payload)
                #print("I " + str(self.node_idx) + " WILL RESPOND TO YOU "+ str(msg.sender) +  " WITH THIS " + str(response))
                dimpleMsg = msgDimple('REINFORCEMENT_RESPONSE',[response, msg.payload] , self.node_idx)
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", msg.sender)
                
            elif msg.type == 'REINFORCEMENT_RESPONSE':
                self.timerDimple.remove(msg.sender)
                self.ReinforcementResponseProcedure(msg.payload[0],msg.payload[1])
                #print("FINISHED REINFORCEMENT FOR " + str(self.node_idx)+ " WITH " + str(msg.sender))
                #print("THIS IS WHAT IHAVE AFTER THE REINFORCEMENT " + str(self.partial_view))

            elif msg.type == 'JOIN':
                #print("STARTED JOIN FOR " + str(msg.sender) + " WITH " + str(self.node_idx))
                # On Q (introducer)
                join_candidates = []
                for entry in self.partial_view:
                    if len(entry.visited) >= 2 and entry.visited[-1] == self.node_idx:
                        join_candidates.append(entry.visited[-2])
                
                # Send join_candidates to P
                dimpleMsg = msgDimple('JOIN_RESPONSE', join_candidates, self.node_idx)    
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", msg.sender)
                
            elif msg.type == 'JOIN_RESPONSE':
                #print("RECEIVING JOIN CANDIDATES FOR"+ str(self.node_idx))
                join_candidates = msg.payload
                for node in join_candidates:
                    dimpleMsg = msgDimple('REINFORCEMENT', [True,partialViewEntry(node,0,0)], self.node_idx)
                    self.reqService(lookahead, "Dimple", dimpleMsg , "Node", self.node_idx)
                for node in join_candidates:
                    if node != self.node_idx:
                        self.partial_view.append(partialViewEntry(node, 0, [node, self.node_idx]))
                self.UpdatePlumTreePeers()
                #print("FINISHED JOIN FOR " + str(self.node_idx))  
                #print("THIS IS WHAT IHAVE AFTER THE JOIN " + str(self.partial_view))
    
    
    def DimpleShuffle(self, *args):
        if self.active and len(self.partial_view) > 0:
            for entry in self.partial_view:
                entry.age += 1
            oldest_entry = max(self.partial_view, key=lambda x: x.age)
            eligible_entries = [x for x in self.partial_view if x != oldest_entry]
            selected_peers = random.sample(eligible_entries, min(len(eligible_entries), shuffleSize - 1))
            selected_peers.append(partialViewEntry(self.node_idx,0,[self.node_idx]))
            dimpleMsg = msgDimple('VIEW_EXCHANGE_REQUEST', selected_peers, self.node_idx)  
            self.timerDimple.append(oldest_entry.node_idx)
            self.reqService(dimpleTimer, "TimerDimple", oldest_entry.node_idx)
            self.reqService(lookahead, "Dimple", dimpleMsg , "Node", oldest_entry.node_idx)
            #print("STARTED VIEW EXCHANGE FOR " + str(self.node_idx))
            reinforceMsg = msgDimple('REINFORCEMENT', [False,partialViewEntry(0,0,0)] , self.node_idx)
            for i in range(shuffleSize):
                self.reqService(lookahead, "Dimple", reinforceMsg , "Node", self.node_idx)
            self.reqService(5, "DimpleShuffle", "none")    

    def ReinforcementInitiateProcedure(self, received_entry):

        if received_entry.node_idx == self.node_idx:
            return random.sample([entry for entry in self.partial_view if entry.node_idx != self.node_idx],1)[0]

        for e in self.partial_view:
            if received_entry.node_idx == e.node_idx:
                return random.sample([entry for entry in self.partial_view if entry.node_idx != self.node_idx],1)[0]

        # Step 1: Fill empty slots in partial_view
        if len(self.partial_view) < maxPartialView:
            self.partial_view.append(received_entry)
            self.UpdatePlumTreePeers()
            return "NULL"

        # Step 2: Replace entries that were sent to Q (i.e., not updated)
        node_placement = random.randrange(len(self.partial_view)) 
        nodeto_replace = self.partial_view[node_placement]
        self.partial_view[node_placement] = received_entry
        self.UpdatePlumTreePeers()

        return nodeto_replace
    
    def ReinforcementResponseProcedure(self, received_entry, entry_toupdate):

        if received_entry == "NULL":
            return

        if received_entry.node_idx == self.node_idx:
            return 

        for e in self.partial_view:
            if received_entry.node_idx == e.node_idx:
                return 

        # Step 2: Replace entries that were sent to Q (i.e., not updated)
        for i, e in enumerate(self.partial_view):
            if entry_toupdate.node_idx == e.node_idx:
                self.partial_view[i] = received_entry
        self.UpdatePlumTreePeers()       


    def ExchangeProcedure(self, received_subset,subset_toupdate):
        new_entries = []

        # Filter received entries (skip self and duplicates)
        for entry in received_subset:
            if entry.node_idx == self.node_idx:
                continue
            if all(entry.node_idx != e.node_idx for e in self.partial_view):
                new_entries.append(entry)

        # Step 1: Fill empty slots in partial_view
        while len(self.partial_view) < maxPartialView and new_entries:
            new_entry = new_entries.pop(0)
            new_entry.visited.append(self.node_idx)
            self.partial_view.append(new_entry)

        # Step 2: Replace entries that were sent to Q (i.e., not updated)
        for i, e in enumerate(self.partial_view):
            if any(entry.node_idx == e.node_idx for entry in subset_toupdate) and new_entries:
                new_entry = new_entries.pop(0)
                new_entry.visited.append(self.node_idx)
                self.partial_view[i] = new_entry
        self.UpdatePlumTreePeers()

    def NodeFailure(self,node_id): 
        for entry in self.partial_view:
            if entry.node_idx == node_id:
                #print("Node Failuere detected For" + str(entry.node_idx))
                self.partial_view.remove(entry)
                self.NeighborDown(entry.node_idx)
        
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

 #--------------------------------------- TRIGGERS ---------------------------------------------------
           
    def TriggerSystemReport(self,*args):
        if self.active:
            report = []
            degree = len(self.eagerPushPeers)+len(self.lazyPushPeers)
            for m in self.receivedMsgs.keys():
                report.append((m,self.receivedMsgs[m].round,self.report[m][0],self.report[m][1],self.report[m][2]))

            msgToSend = msgReport('reply',report,degree)

            self.reqService(lookahead, "SystemReport", msgToSend, "ReportNode", 0)

    def nodeFail(self, *args):
        if self.active:
            self.active = False
        else:
            self.active = True



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
    msgGap = round((endTime - 100) / args.msgs,2)

    if args.multipleSender == 0:
        idx = random.randrange(len(available))
        n = available[idx]

    for i in range(args.msgs):
        if args.multipleSender == 1:
            idx = random.randrange(len(available))
            n = available[idx]

        delay = lookahead
        msgToSend = msgGossip('BROADCAST','Hi',msgID,0,0)
        simianEngine.schedService(lookahead + 50 + (i * msgGap), "PlumTreeGossip", msgToSend, "Node", n)
        msgID+=1

simianEngine.run()
simianEngine.exit()