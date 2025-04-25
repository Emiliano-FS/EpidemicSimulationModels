import simian
from simian import Simian
import random, math, argparse

parser = argparse.ArgumentParser(
    description='DIMPLE Membership Protocol Simulation.',
    formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument('total_nodes', metavar='NNODES', type=int, help='total number of nodes')
parser.add_argument('endtime', metavar='ENDTIME', type=float, help='simulation end time')
parser.add_argument("--msgs", type=int, metavar='NMSGS', default=0, help="number of broadcast messages -> default 0")
parser.add_argument('--seedR', type=int, metavar='SEED', default=10, help='seed for random')
parser.add_argument("-l", "--lookahead", type=float, metavar='LOOKAHEAD', default=0.1, help='min delay of mailboxes')
parser.add_argument("-r", "--maxrounds", type=int, default=math.inf, help='maximum number of times a message is retransmitted by nodes')
parser.add_argument("-f", "--fanout", type=int, default=5, help='number of nodes that are selected as gossip targets')
parser.add_argument("--useMPI", type=int, metavar='MPI', default=0, help="use mpi")
parser.add_argument("--multipleSender", type=float, metavar='SENDER', default=0, help="multiple Senders for broadcast messages -> default 0-false")
parser.add_argument("--failRate", type=float, metavar='FAILRATE', default=0.0, help="node fail rate [0.0 ... 1.0]")
#parser.add_argument("--shuffleSize", type=int, metavar='SHUFFLESIZE', default=4, help="Dimple Shuffle Size")
#parser.add_argument("--maxPartialView", type=int, metavar='MAXPARTIALVIEW', default=4, help="Dimple Maximum Partial View")
#parser.add_argument("--dimpleTimeout", type=float, metavar='DIMPLETIMEOUT', default=1.0, help="DIMPLE protocol timeout")
args = parser.parse_args()

uMPI = False
if args.useMPI == 1:
    uMPI = True

nodes = args.total_nodes
lookahead = args.lookahead
maxrounds = args.maxrounds
fanout = args.fanout
failRate = args.failRate
shuffleSize = math.floor(math.log(nodes,10))
maxPartialView = math.floor(2 * math.log(nodes,10))
dimpleTimer = 4
#dimpleTimeout = args.dimpleTimeout
random.seed(args.seedR)

name = "DIMPLETests/"+"DIMPLE" + str(args.total_nodes)+'-FR'+str(args.failRate)+'-Seed'+str(args.seedR)+'-Sender'+str(args.multipleSender)+'-MGS'+str(args.msgs)+'-LOOKAHEAD'+str(args.lookahead)

simName, startTime, endTime, minDelay = name, 0, args.endtime, 0.1
simianEngine = Simian(simName, startTime, endTime, minDelay, uMPI)

class msgGossip:
    def __init__(self,mID,m,round):
        self.ID = int(mID)
        self.payload = m
        self.round = int(round)

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
        self.reliability = {}
        self.latency = {}
        self.redundancy = {}
        self.degree = 0
        self.maxDegree = 0
        self.minDegree = 1000
        self.shortestPath = 0
        
        self.reqService(endTime , "PrintSystemReport", "none")

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

        #print(self.reliability.keys())
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
        self.total_nodes = args[1]
        self.node_idx = args[0]
        self.active = True

        #Basic Gossip Variables
        self.receivedMsgs = {}

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
                self.reqService(lookahead * 5 + delay2, "DimpleShuffle", "none")  
        else:
            contactNode = 0 
            msg = msgDimple('JOIN',[],self.node_idx)
            self.reqService( lookahead + delay2  , "Dimple", msg, "Node", contactNode)  
             
        self.reqService(endTime - 1, "TriggerSystemReport", "none")


    def Receive(self, *args):
        if not self.active:
            return
        m = args[0]
        m2 = m.split("-")
        msg = msgGossip(m2[0], m2[1], m2[2])
        if msg.ID not in self.report.keys():
            self.report[msg.ID] = 1
        else:
            self.report[msg.ID] += 1   

        if msg.ID not in self.receivedMsgs.keys() and msg.round < maxrounds:
            self.receivedMsgs[msg.ID] = msg
            self.Gossip(msg)
    

    def Gossip(self, msg):
        msg.round += 1
        for entry in self.partial_view:
            self.reqService(lookahead, "Receive", f'{msg.ID}-{self.receivedMsgs[msg.ID].payload}-{msg.round}', "Node", entry.node_idx)



#--------------------------------------- PEER SELECTION ---------------------------------------------------
    
    def Dimple(self, *args):
        msg = args[0]
        if self.active:
            if msg.type == 'VIEW_EXCHANGE_REQUEST':
                # Q receives P's subset, replies with its own
                own_subset = random.sample(self.partial_view, min(shuffleSize, len(self.partial_view)))
                self.ExchangeProcedure(msg.payload, own_subset)
                reply = msgDimple("VIEW_EXCHANGE_RESPONSE", [own_subset, msg.payload], self.node_idx)
                self.reqService(lookahead, "Dimple", reply, "Node", msg.sender)
                
            elif msg.type == 'VIEW_EXCHANGE_RESPONSE':
                self.timerDimple.remove(msg.sender)
                received_subset, sent_subset = msg.payload
                self.ExchangeProcedure(received_subset, sent_subset)

            elif msg.type == 'REINFORCEMENT':
                if len(self.partial_view) > 0:
                    node_toreinforce = max(self.partial_view, key=lambda x: x.age)
                    self.timerDimple.append(node_toreinforce.node_idx)
                    self.reqService(dimpleTimer, "TimerDimple", node_toreinforce.node_idx)
                    dimpleMsg = msgDimple('REINFORCEMENT_INITIATE',self.node_idx, self.node_idx)
                    self.reqService(lookahead, "Dimple", dimpleMsg , "Node", node_toreinforce.node_idx)

            elif msg.type == 'REINFORCEMENT_INITIATE':
                response = self.ReinforcementInitiateProcedure(msg.payload)
                dimpleMsg = msgDimple('REINFORCEMENT_RESPONSE',[response, msg.payload] , self.node_idx)
                self.reqService(lookahead, "Dimple", dimpleMsg , "Node", msg.sender)
                
            elif msg.type == 'REINFORCEMENT_RESPONSE':
                response_entry, entry_to_replace = msg.payload
                self.timerDimple.remove(msg.sender)
                self.ReinforcementResponseProcedure(response_entry,entry_to_replace)

            elif msg.type == 'JOIN':
                # JOIN Request → Q returns local view built from visited[-2] or fallback
                local_view = []
                for entry in self.partial_view:
                    if len(entry.visited) >= 2:
                        local_view.append(entry.visited[-2])
                    else:
                        local_view.append(entry.node_idx)
                
                # Send join_candidates to P
                response = msgDimple("JOIN_RESPONSE", local_view, self.node_idx)
                self.reqService(lookahead, "Dimple", response, "Node", msg.sender)
                
            elif msg.type == 'JOIN_RESPONSE':
                # Node P gets view, performs reinforcements
                candidates = msg.payload
                random.shuffle(candidates)

                #for candidate in candidates: 
                #    if candidate != self.node_idx:
                #        self.partial_view.append(partialViewEntry(candidate, 0, [self.node_idx]))
          
                for peer_id in candidates:
                    if peer_id != self.node_idx:
                        self.timerDimple.append(peer_id)
                        self.reqService(dimpleTimer, "TimerDimple", peer_id)
                        reinforce_msg = msgDimple("REINFORCEMENT_INITIATE", self.node_idx,self.node_idx)
                        self.reqService(lookahead, "Dimple", reinforce_msg, "Node", peer_id)

                delay = lookahead * (len(candidates) + 2)
                self.reqService(delay, "DimpleShuffle", "none") 
                
    
    def DimpleShuffle(self, *args):
        if not self.active or len(self.partial_view) <= 0:
            self.reqService(25, "DimpleShuffle", "none")  # Reschedule anyway
            return

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
        
        # Repeat Reinforcement l times in parallel.
        for i in range(shuffleSize):
            msg = msgDimple("REINFORCEMENT", None , self.node_idx)
            self.reqService(lookahead, "Dimple", msg, "Node", self.node_idx)

        # Reschedule next shuffle
        self.reqService(25, "DimpleShuffle", "none")


    def ReinforcementInitiateProcedure(self, received_entry_id):
        if received_entry_id == self.node_idx:
            return None  # Don't reinforce with self

        existing_ids = {entry.node_idx for entry in self.partial_view}

        # If already present, ignore
        if received_entry_id in existing_ids:
            return None

        # If space available, add directly
        if len(self.partial_view) < maxPartialView:
            new_entry = partialViewEntry(received_entry_id, 0, [received_entry_id, self.node_idx])
            self.partial_view.append(new_entry)
            return None  # No eviction needed

        # Replace a random entry
        to_replace = random.randrange(len(self.partial_view))
        evicted_entry = self.partial_view[to_replace]
        self.partial_view[to_replace] = partialViewEntry(received_entry_id, 0, [received_entry_id, self.node_idx])
        self.UpdatePlumTreePeers()
        return evicted_entry  # Send this back to P
    
    def ReinforcementResponseProcedure(self, response_entry, entry_to_replace_id):
        if response_entry is None:
            return  # Nothing to update
        
        # If P receives a valid response, P adds the node information to an empty slot if there is one
        if len(self.partial_view) < maxPartialView:
            self.partial_view.append(response_entry)
            return

        # Or replaces the entry of Q with the received information of another node.
        for i, entry in enumerate(self.partial_view):
            if entry.node_idx == entry_to_replace_id:
                self.partial_view[i] = response_entry
                


    def ExchangeProcedure(self, received_subset, sent_subset):
        # Helper: get all node IDs in the current view for fast lookup
        current_ids = {entry.node_idx for entry in self.partial_view}

        for received_entry in received_subset:
            # Ignore if it's self or already in view
            if received_entry.node_idx == self.node_idx or received_entry.node_idx in current_ids:
                continue

            # Fill empty slots in partial_view
            if len(self.partial_view) < maxPartialView:
                received_entry.visited.append(self.node_idx)
                self.partial_view.append(received_entry)
                break

            # Replace entries that were sent to Q (i.e., not updated)
            for i, e in enumerate(self.partial_view):    
                if any(entry.node_idx == e.node_idx for entry in sent_subset):
                    received_entry.visited.append(self.node_idx)
                    self.partial_view[i] = received_entry
            
       

    def NodeFailure(self, node_id):
        self.partial_view = [entry for entry in self.partial_view if entry.node_idx != node_id]
        self.NeighborDown(node_id)
        
    def TimerDimple(self, *args):
        dest = args[0]
        if dest in self.timerDimple:
            self.timerDimple.remove(dest)
            self.NodeFailure(dest)


   
 #--------------------------------------- TRIGGERS ---------------------------------------------------
           
    def TriggerSystemReport(self, *args):
        if self.active:
            report = []
            degree = len(self.partial_view)
            for m in self.receivedMsgs.keys():
                report.append((m, self.receivedMsgs[m].round, self.report[m]))

            msgToSend = msgReport('reply', report, degree)
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

available.remove(0)
available.remove(1)
available.remove(2)    

if args.failRate <= 1:
    for i in range(fails):
        idx = random.randrange(len(available))
        n = available[idx]
        failsList.append(n)
        available.remove(n)
        fail_time = 50 + random.uniform(0, 20)
        simianEngine.schedService(fail_time, "nodeFail", "x", "Node", n)

available.append(0)
available.append(1)
available.append(2)        


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