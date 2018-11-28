import rpyc
import sys
import os
from random import randint
import threading
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
	def __init__(self, config, server_no):
		config_file = os.path.realpath(config)
		file = open(config_file, r)
		content = file.read()
		file.close()
		content_list = file.split("\r\n")
		for element in content_list:
			a, b = element.split(': ')
			confdict.update({a: b})
		self.no_of_servers = confdict["N"]
		self.majority = (self.no_of_servers // 2) + 1
		del confdict["N"]
		del confdict["node"+str(server_no)]
		self.server_list = []
		for a, b in confdict.items():
			self.server_list.append(b)
		self.state = 0
		self.term = 0
		self.leaderID = -1
		self.__ID = server_no
		self.startLoop()

	'''
		state 0 = follower
		state 1 = candidate
		state 2 = leader
	'''
	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
	'''
	def exposed_is_leader(self):
		if self.leaderID == self.__ID:
			return True
		else:
			return False
	"""
		AppendEntries updates the receiver nodes with the term
		returns current term and if receiver succeeded in updating
	"""
	def exposed_AppendEntries(self, term, leaderId):
		if term > self.term && self.leaderID == leaderId:
			self.election_timeout.cancel()
			self.term = term
			return(self.term, True)
		else:
			return(self.term, False)

	def exposed_RequestVote(self, term, candidateID):
		if self.state == 0:
			if term > self.term:
				self.node_timeout.cancel()
				self.leaderID = candidateID
				self.term = term
				return(self.term, True)
			else:
				return(self.term, False)
		elif self.state == 1:
			if term >= self.term:
				self.node_timeout.cancel()
				self.leaderID = candidateID
				self.term = term
				return(self.term, True)
			else:
				return(self.term, False)
		elif self.state == 2:
			if term > self.term:
				self.node_timeout.cancel()
				self.leaderID = candidateID
				self.term = term
				return(self.term, True)
			else:
				return(self.term, False)


	'''
	self.startLoop()
	Encapsulates all the states in one big loop. We have the
	follower state running currently. The timer starts the
	election in case no RequestVote or AppendEntries is called
	'''
	def startLoop(self):
		while True:
			timeout_val = randint(0.15, 0.3)
			self.election_timeout = threading.Timer(timeout_val, self.beginElection)
			self.election_timeout.start()
	'''
	self.becomeCandidate
	State switch to Candidate and calls for an election, and checks if
	voteGranted is True (indicates it has majority vote)
	'''
	def beginElection(self):
		vote = 0
		self.state = 1
		conn_list = []
		for n in range(self.no_of_servers):
			conn_list.append(try_conn(n))
		for n in range(self.no_of_servers):
			if conn_list[n] != False:
				term, answer = conn_list[n].RequestVote(self.term, self.__ID)
				if answer:
					vote += 1		

	def startLeading(self):
		self.leaderID = self.__ID


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = sys.argv[3])
	server.start()
