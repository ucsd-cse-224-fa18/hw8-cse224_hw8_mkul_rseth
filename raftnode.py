import rpyc
import sys
import os
import threading
from random import randint
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
		del confdict["node"+str(int(server_no))]
		self.server_list = []
		for a, b in confdict.items():
			self.server_list.append(b)
		self.stateFile = open("node"+str(server_no)+".txt", "w+")
		self.stateFile.close()
		if self.is_non_zero_file(self.stateFile.name):
			self.currentTerm, self.votedFor = self.check_state_file(self.stateFile.name)
			self.currentTerm = int(currentTerm)
			self.votedFor = int(votedFor)
		else:
			self.currentTerm = -1
			self.votedFor = None
		self.leaderID = None
		self.ID = server_no
		self.stop_leader = threading.Event()
		self.fileLock = threading.Lock()
		self.timerThread = threading.Thread(target=self.NodeBegin)
		self.timerThread.start()

	def check_state_file(self, fname):
		last = ''
		with open(fname, "rb") as f:
			lines = f.read().splitlines()
			last = lines[-1]
		return tuple(last.split(','))

	def is_non_zero_file(self, fpath):
		return os.stat(fpath).st_size != 0

	def exposed_is_leader(self):
		if self.leaderID == self.ID:
			return True
		else:
			return False

	def update_state_file(self, fname):
		with open(fname, 'a') as fp:
			fp.write(str(self.currentTerm) + ',' + str(self.votedFor) + '\n')

	def NodeBegin(self):
		while True:
			timeout_val = randint(20, 40)
			timeout_val = timeout_val / 10
			self.stop_leader.clear()
			self.Voted = False
			self.node_timeout = threading.Timer(float(timeout_val), self.beginElection)
			self.node_timeout.start()
			self.node_timeout.join()

	def beginElection(self):
		self.fileLock.acquire()
		self.currentTerm += 1
		self.votedFor = self.ID
		self.Voted = True
		self.update_state_file(self.stateFile.name)
		self.fileLock.release()
		vote = 1
		for n in range(self.no_of_servers-1):
			try:
				a, b = self.server_list[n].split(':')
				conn = rpyc.connect(a, b).root
				term, answer = conn.RequestVote(self.currentTerm, self.ID)
			except ConnectionRefusedError:
				term, answer = (self.currentTerm, False)
			except EOFError:
				term, answer = (self.currentTerm, False)
			except socket.error:
				term, answer = (self.currentTerm, False)
			if answer:
				vote += 1
		if vote > self.majority:
			self.leaderID = self.ID
			self.leaderLoop()
		else:
			#print("exit thread")
			pass

	def leaderLoop(self):
		while True:
			if self.stop_leader.is_set():
				#print("leader break event!")
				self.stop_leader.clear()
				break
			vote = 1
			for n in range(self.no_of_servers-1):
				try:
					a, b = self.server_list[n].split(':')
					conn = rpyc.connect(a, b).root
					term, answer = conn.AppendEntries(self.currentTerm, self.ID)
				except ConnectionRefusedError:
					term, answer = (self.currentTerm, False)
				except EOFError:
					term, answer = (self.currentTerm, False)
				except socket.error:
					term, answer = (self.currentTerm, False)
				if answer:
					vote += 1
			if vote < self.majority:
				break
		#print("broken!")
		self.NodeBegin()

	def exposed_AppendEntries(self, term, leaderId):
		if term < self.currentTerm:
			return (self.currentTerm, False)
		else:
			self.fileLock.acquire()
			self.node_timeout.cancel()
			self.stop_leader.set()
			self.currentTerm = term
			self.leaderID = leaderId
			self.fileLock.release()
			#print("\n" + str(self.leaderID) + " is my leader with term: " + str(term) + " and my ID is " + str(self.ID))
			return (self.currentTerm, True)

	def exposed_RequestVote(self, term, candidateId):
		if term > self.currentTerm and not self.Voted:
			self.fileLock.acquire()
			self.node_timeout.cancel()
			self.currentTerm = term
			self.votedFor = candidateId
			self.update_state_file(self.stateFile.name)
			self.Voted = True
			self.fileLock.release()
			return(self.currentTerm, True)
		else:
			return (self.currentTerm, False)

if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()
