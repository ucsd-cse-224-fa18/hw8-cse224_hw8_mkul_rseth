import rpyc
import sys
import os
from random import randint
import threading
import time
import socket
'''
A RAFT RPC server class.
m
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
		self.leaderID = None
		self.ID = server_no
		self.Voted = False
		self.termLock = threading.Lock()
		self.timerThread = threading.Thread(target=self.startLoop)
		self.timerThread.start()

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
		if self.leaderID == self.ID:
			return True
		else:
			return False
	"""
		AppendEntries updates the receiver nodes with the term
		returns current term and if receiver succeeded in updating
	"""
	def exposed_AppendEntries(self, term, leaderId):
		if term == self.term:
			self.node_timeout.cancel()
			self.termLock.acquire()
			self.leaderID = leaderId
			self.termLock.release()
			return(self.term, True)
		elif term > self.term:
			self.node_timeout.cancel()
			self.termLock.acquire()
			self.term = term
			self.leaderID = leaderId
			self.termLock.release()
		else:
			return(self.term, False)

	def exposed_RequestVote(self, term, candidateID):
		if term > self.term and not self.Voted:
			self.node_timeout.cancel()
			self.termLock.acquire()
			self.term = term
			self.leaderID = candidateID
			self.Voted = True
			self.termLock.release()
			return(term, True)
		elif term == self.term and not self.Voted:
			self.node_timeout.cancel()
			self.termLock.acquire()
			self.leaderID = candidateID
			self.Voted = True
			self.termLock.release()
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
			timeout_val = randint(1500, 3000)
			timeout_val = timeout_val / 1000
			self.node_timeout = threading.Timer(float(timeout_val), self.beginElection)
			self.node_timeout.start()
			self.node_timeout.join()
			self.Voted = False
			#print("\n" + str(self.leaderID) + " is my leader with term: " + str(self.term) + " and my ID is " + str(self.ID))
#			print("ID:" + str(self.ID) + " state: " + str(self.state))
	'''
	self.beginElection
	State switch to Candidate and calls for an election, and starts
	election for the leader
	'''
	def beginElection(self):
		self.termLock.acquire()
		self.term += 1
		self.termLock.release()
		#print("Candidate term = " + str(self.term))
		vote = 1
		self.state = 1
		conn_list = []
		for n in range(self.no_of_servers-1):
			conn_list.append(self.try_conn(n))
		for n in range(self.no_of_servers-1):
			try:
				if conn_list[n] != False:
					try:
						term, answer = conn_list[n].RequestVote(self.term, self.ID)
						if term > self.term and not answer:
							self.state = 0
							self.termLock.acquire()
							self.term = term
							self.termLock.release()
							return
					except ConnectionRefusedError:
						term, answer = (self.term, False)
					if answer:
						vote += 1
			except EOFError:
				continue
		#print("\nVotes I got: " + str(vote))
		if vote >= self.majority:
			self.leaderID = self.ID
			self.state = 2
			#print("\n" + str(self.ID) + " is the leader!")
			self.startLeading()
		return

	'''
	try_conn returns either the root RPC or False
	if connection is not working
	'''
	def try_conn(self, n):
		try:
			a, b = self.server_list[n].split(':')
			conn = rpyc.connect(a, b)
			return conn.root

		except socket.error:
			return False
		except EOFError:
			return False

	def startLeading(self):
		self.leaderID = self.ID
		Leading = True
		conn_list = []
		while Leading:
			time.sleep(0.3)
			for n in range(self.no_of_servers-1):
				conn_list.append(self.try_conn(n))
			vote = 1
			for n in range(self.no_of_servers-1):
				try:
					if conn_list[n] != False:
						try:
							term, answer = conn_list[n].AppendEntries(self.term, self.leaderID)
						except ConnectionRefusedError:
							term, answer = (self.term, -1)
						except TypeError:
							term, answer = (self.term, -1)
						if term > self.term and not answer:
							Leading = False
							break
						if answer:
							vote+= 1
				except EOFError:
					continue
			if vote <= self.majority:
				#print("\n" + str(self.ID) + " not leader")
				self.state=0
				Leading = False



if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()
