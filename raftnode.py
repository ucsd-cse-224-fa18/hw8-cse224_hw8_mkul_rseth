import rpyc
import sys
import os
import random
import re
import threading

FOLLOWER = "follower"
LEADER = "leader"
CANDIDATE = "candidate"


'''
A RAFT RPC server class.

Please keep the signature of the is_leader() method unchanged (though
implement the body of that function correctly.  You will need to add
other methods to implement ONLY the leader election part of the RAFT
protocol.
'''
class RaftNode(rpyc.Service):
	

	"""
        Initialize the class using the config file provided and also initialize
        any datastructures you may need.
	"""
	def __init__(self, config, number):
		self.nodeId = number
		fd = open(os.path.realpath(config), 'r')
		file = fd.read().splitlines()
		fd.close()
		self.numberofnodes = int(re.match("N: (\d)", file[0])[1])
		self.nodelist = []
		for i in range(self.numberofnodes) :
			matchobject = re.match("node\d: ([a-zA-Z0-9.]+):(\d+)",file[i+1])
			raftnodehost = matchobject[1]
			raftnodeport = matchobject[2]
			self.nodelist.append((raftnodehost, raftnodeport))
		self.status = FOLLOWER
		self.votedFor = None
		self.leader = None
		self.currentTerm = 0
		self.threshold = int((self.numberofnodes)/2)+1
		self.votes = 0
		print("---------------------------------------")
		self.print_info()
		self.stateFile = open("node"+str(self.nodeId)+".txt", "w+")
		self.stateFile.close()
		if self.is_non_zero_file(self.stateFile.name):
			self.currentTerm, self.votedFor, _ = self.check_state_file(self.stateFile.name)
			self.currentTerm = int(self.currentTerm)
			self.votedFor = int(self.votedFor)
		else:
			self.currentTerm = 0
			self.votedFor = None
		self.fileLock = threading.Lock()
		self.electiontimer = threading.Timer(random.randint(200, 500)/100, self.start_election)
		self.electiontimer.start()
		
	def check_state_file(self, fname):
		last = ''
		with open(fname, 'r') as fd:
			lines = fd.read().splitlines()
			last = lines[-1]
		return tuple(last.split(' '))

	def is_non_zero_file(self, fpath):
		return os.stat(fpath).st_size != 0

	def update_state_file(self, fname):
		with open(fname, 'a') as fd:
			fd.write(str(self.currentTerm) + ' ' + str(self.votedFor) + ' ' + self.status + '\n')


	def start_election(self):
		self.fileLock.acquire()
		self.votes = 1
		self.votedFor = self.nodeId
		self.status = CANDIDATE
		self.currentTerm = self.currentTerm + 1
		self.update_state_file(self.stateFile.name)
		self.fileLock.release()
		for i in range(self.numberofnodes):
			if i != self.nodeId :
				try :
					raftnodehost, raftnodeport = self.nodelist[i]
					conn = rpyc.connect(raftnodehost, raftnodeport)
					term, voteGranted = conn.root.request_vote(self.currentTerm, self.nodeId)
					if voteGranted :
						self.votes = self.votes + 1
					elif term > self.currentTerm :
						self.status = FOLLOWER
						break
				except Exception as e :
					# print("IN START ELECTION.   ", e)
					raise e
		if self.status == CANDIDATE and self.votes >= self.threshold :
			self.status = LEADER
			self.leader = self.nodeId
			print("IN START ELECTION")
			self.print_info()
			self.votes = 0
			self.votedFor = None
			self.start_append()
		else :
			self.status = FOLLOWER
			self.votedFor = None
			self.leader = None
			self.electiontimer = threading.Timer(random.randint(400, 500)/100, self.start_election)
			self.electiontimer.start()


	def start_append(self):
		print("IN START APPEND")
		self.print_info()
		for i in range(self.numberofnodes):
			if i != self.nodeId :
				try :
					raftnodehost, raftnodeport = self.nodelist[i]
					conn = rpyc.connect(raftnodehost, raftnodeport)
					term, success = conn.root.append_entries(self.currentTerm, self.nodeId)
					if term > self.currentTerm : 
						self.status = FOLLOWER
						self.leader = None
						self.currentTerm = term
				except Exception as e :
					# print("IN START APPEND.   ", e)
					raise e
		if self.status == FOLLOWER :
			self.electiontimer = threading.Timer(random.randint(400, 500)/100, self.start_election)
			self.electiontimer.start()
		else :
			self.appendtimer = threading.Timer(0.1, self.start_append)
			self.appendtimer.start()


	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
	'''
	def exposed_is_leader(self):
		if self.status == LEADER :
			return True
		return False

	def exposed_request_vote(self, term, candidateId):
		self.electiontimer.cancel()
		self.fileLock.acquire()
		voteGranted = False
		if self.votedFor is None and term > self.currentTerm and self.status == FOLLOWER :
			voteGranted = True
			self.votedFor = candidateId
			# self.currentTerm = term
			self.update_state_file(self.stateFile.name)
		print("IN REQUEST VOTES : ", candidateId, " ", term)
		self.print_info()
		self.electiontimer = threading.Timer(random.randint(400, 500)/100, self.start_election)
		self.electiontimer.start()
		self.fileLock.release()
		return self.currentTerm, voteGranted

	def exposed_append_entries(self, term, leaderId):
		self.electiontimer.cancel()
		self.fileLock.acquire()
		success = True
		if term < self.currentTerm :
			success = False
		else :
			self.leader = leaderId
			self.currentTerm = term
			self.status = FOLLOWER
			self.votedFor = None
		print("IN APPEND ENTRIES : ", leaderId, " ", term)
		self.print_info()
		self.electiontimer = threading.Timer(random.randint(400, 500)/100, self.start_election)
		self.electiontimer.start()
		self.fileLock.release()
		return term, success

	def print_info(self):
		print("Node ID : ", self.nodeId)
		print("Term : ", self.currentTerm)
		print("Voted For : ", self.votedFor)
		print("Status : ", self.status)
		print("Leader : ", self.leader)
		print("---------------------------------------")

if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], int(sys.argv[2])), port = int(sys.argv[3]))
	server.start()

