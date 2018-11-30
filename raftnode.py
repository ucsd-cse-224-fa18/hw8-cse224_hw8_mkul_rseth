import rpyc
import sys
import os
from random import randint
import threading
import time
import socket
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
		file = open(config_file, 'r')
		content = file.read()
		file.close()
		content_list = content.split("\n")
		del content_list[len(content_list)-1]
		confdict = {}
		for element in content_list:
			a, b = element.split(': ')
			confdict.update({a: b})
		self.no_of_servers = int(confdict["N"])
		self.majority = (self.no_of_servers // 2) + 1
		del confdict["N"]
		del confdict["node"+str(int(server_no)-1)]
		self.server_list = []
		for a, b in confdict.items():
			self.server_list.append(b)
		self.state = 0
		self.term = 0
		self.leaderID = -1
		self.__ID = server_no

		timeout_val = randint(150, 300)
		timeout_val = timeout_val / 1000
		self.node_timeout = threading.Timer(timeout_val, self.beginElection)
		self.node_timeout.start()
		print("state: " + str(self.state))

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
		if term > self.term and self.leaderID == leaderId:
			self.node_timeout.cancel()
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
		timeout_val = randint(3000, 5000)
		timeout_val = timeout_val / 1000
		self.node_timeout = threading.Timer(timeout_val, self.beginElection)
		self.node_timeout.start()
		print("state: " + str(self.state))
	'''
	self.beginElection
	State switch to Candidate and calls for an election, and starts
	election for the leader
	'''
	def beginElection(self):
		self.term += 1
		vote = 1
		self.state = 1
		conn_list = []
		for n in range(self.no_of_servers-1):
			conn_list.append(self.try_conn(n))
		for n in range(self.no_of_servers-1):
			if conn_list[n] != False:
				try:
					term, answer = conn_list[n].RequestVote(self.term, self.__ID)
				except ConnectionRefusedError:
					term, answer = (self.term, False)
				if answer:
					vote += 1
		if vote >= self.majority:
			self.state = 2
			print("I am the leader!")
			if not self.startLeading():
				self.state = 0
				self.startLoop()
		else:
			self.startLoop()

	'''
	try_conn returns either the root RPC or False
	if connection is not working
	'''
	def try_conn(self, n):
		try:
			a, b = self.server_list[n].split(':')
			print(a, b)
			conn = rpyc.connect(a, b)
			return conn.root

		except socket.error:
			return False

	def startLeading(self):
		self.leaderID = self.__ID
		Leading = True
		conn_list = []
		while Leading:
			for n in range(self.no_of_servers-1):
				conn_list.append(self.try_conn(n))
			for n in range(self.no_of_servers-1):
				if conn_list[n] != False:
					try:
						term, answer = conn_list[n].AppendEntries(self.term, self.leaderID)
					except ConnectionRefusedError:
						term, answer = (self.term, -1)
					if not answer:
						Leading = False
						return False



if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()
