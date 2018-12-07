import rpyc
import sys
import re
import threading
import random
import os

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
	def __init__(self, config, nodenumber):

		self.file = os.path.realpath(config)
		self.fd = open(self.file, 'r')
		self.filelines = self.fd.read().splitlines()
		self.fd.close()
		self.numberofnodes = int(re.match("N: (\d)", self.filelines[0])[1])
		self.nodemap = {}
		self.currentterm = 0
		self.threshold = int((self.numberofnodes+1)/2)
		self.number = nodenumber
		self.leader = None
		self.votedFor = None
		self.votes = 0
		self.status = FOLLOWER
		self.time = 2
		self.timer = threading.Timer(self.time, self.happen)
		self.timer.start()

	def happen(self):
		for i in range(self.numberofnodes) :
			self.matchobject = re.match("node(\d): ([a-zA-Z0-9.]+):(\d+)",self.filelines[i+1])
			self.raftnodenum = self.matchobject[1]
			self.raftnodehost = self.matchobject[2]
			self.raftnodeport = self.matchobject[3]
			self.nodemap[int(self.raftnodenum)]=rpyc.connect(self.raftnodehost, self.raftnodeport)
		self.timeout = random.randint(300, 500)/100
		self.timer = threading.Timer(self.timeout, self.become_candidate)
		self.timer.start()


	def become_candidate(self):
		if self.status == FOLLOWER :
			self.currentterm = self.currentterm + 1
			self.votedFor = self.number
			self.status = CANDIDATE
			self.leader = self.number
			self.votes = 1
			for i in range(self.numberofnodes):
				if self.number != i :
					threading.Thread(target = self.request_votes, args = (i,)).start()
			# 	try :
			# 		answer = False
			# 		term = self.currentterm
			# 		if number != i :
			# 			term, answer = self.nodemap[i].conn.vote(self.currentterm, self.number)
			# 	except e :
			# 		pass
			# 	if answer :
			# 		votes = votes + 1
			# 	if term > currentterm :
			# 		status = FOLLOWER
			# 		currentterm = term
			# 		break
			# if votes >= self.threshold and status == CANDIDATE :
			# 	leader = number
			# 	status = LEADER
			# 	self.timeout = 1
			# 	self.timer = threading.Timer(timeout, self.update_nodes)
			# 	self.timer.start()
			# else :
			# 	self.timeout = random.randint(300, 500)/100
			# 	self.timer = threading.Timer(timeout, self.become_candidate)
			# 	self.timer.start()


	def request_votes(self, i):
		answer = False
		term = self.currentterm
		try :
			term, answer = self.nodemap[i].root.vote(self.currentterm, self.number)
			if term > self.currentterm :
				self.status = FOLLOWER
				self.timeout = random.randint(300, 500)/100
				self.timer = threading.Timer(self.timeout, self.become_candidate)
				self.timer.start()	
			if answer :
				self.votes = self.votes + 1
				if self.votes > self.threshold and self.status == CANDIDATE :
					self.leader = self.number
					self.status = LEADER
					print(self.currentterm, "IS LEADER")
					self.leader_loop()
		except Exception as e :
			print("In request ", e)


	def leader_loop(self):
		if self.status == LEADER :
			for i in range(self.numberofnodes):
				if self.number != i :
					threading.Thread(target = self.heartbeat_counter, args = (i,)).start()

	def heartbeat_counter(self, i):
		answer = False
		term = self.currentterm
		try :
			term, answer = self.nodemap[i].root.append(self.currentterm, self.number)
			if term > self.currentterm :
				self.status = FOLLOWER
				self.timeout = random.randint(300, 500)/100
				self.timer = threading.Timer(self.timeout, self.become_candidate)
				self.timer.start()	
			if answer :
				if self.status == LEADER :
					print(self.currentterm, "IS LEADER")
					self.leader_loop()
					self.timeout = random.randint(100, 250)/100
					self.timer = threading.Timer(self.timeout, self.leader_loop)
					self.timer.start()	
		except Exception as e :
			print("In heartbeat",e)

	def exposed_append(self, term, requesternodenumber):
		if self.status == FOLLOWER:
			if self.currentterm <= term:
				self.votedFor = None
				self.leader = requesternodenumber
				self.timeout = random.randint(300, 500)/100
				self.timer = threading.Timer(self.timeout, self.become_candidate)
				self.timer.start()
				return True, self.currentterm
		if self.status == LEADER and self.currentterm < term:
			self.leader = requesternodenumber
			self.timeout = random.randint(300, 500)/100
			self.timer = threading.Timer(self.timeout, self.become_candidate)
			self.timer.start()
			return True, self.currentterm
		if self.status == CANDIDATE:
			if self.currentterm <= term:
				self.status = FOLLOWER
				self.leader = requesternodenumber
				self.timeout = random.randint(300, 500)/100
				self.timer = threading.Timer(self.timeout, self.become_candidate)
				self.timer.start()
				return True, self.currentterm


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

	def exposed_vote(self, term, requesternodenumber):
		if self.status == FOLLOWER and votedFor == None:
			if self.currentterm <= term:
				self.currentterm = term
				self.leader = requesternodenumber
				self.timeout = random.randint(300, 500)/100
				self.timer = threading.Timer(self.timeout, self.become_candidate)
				self.timer.start()
				return True, self.currentterm
			else :
				return False, self.currentterm
		else :
			return False, self.currentterm


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()
