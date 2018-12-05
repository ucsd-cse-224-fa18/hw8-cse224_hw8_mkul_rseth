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
		del confdict["node"+str(int(server_no)-1)]
		self.server_list = []
		for a, b in confdict.items():
			self.server_list.append(b)
		self.currentTerm = -1
		self.leaderID = None
		self.ID = server_no
		self.votedFor = None
		self.stop_leader = threading.Event()
		self.fileLock = threading.Lock()
		self.timerThread = threading.Thread(target=self.NodeBegin)
		self.timerThread.start()

	def exposed_is_leader(self):
		if self.leaderID == self.ID:
			return True
		else:
			return False

	def NodeBegin(self):
		while True:
			timeout_val = randint(20, 50)
			timeout_val = timeout_val / 10
			self.stop_leader.clear()
			self.Voted = False
			self.votedFor = None
			self.node_timeout = threading.Timer(float(timeout_val), self.beginElection)
			self.node_timeout.start()
			self.node_timeout.join()

	def beginElection(self):
		self.fileLock.acquire()
		self.currentTerm += 1
		self.votedFor = self.ID
		self.Voted = True
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
			print("exit thread")
			pass

	def leaderLoop(self):
		while True:
			if self.stop_leader.is_set():
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
		print("broken!")
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
			print("\n" + str(self.leaderID) + " is my leader with term: " + str(term) + " and my ID is " + str(self.ID))
			return (self.currentTerm, True)

	def exposed_RequestVote(self, term, candidateId):
		if term > self.currentTerm and not self.Voted:
			self.fileLock.acquire()
			self.node_timeout.cancel()
			self.currentTerm = term
			self.votedFor = candidateId
			self.Voted = True
			self.fileLock.release()
			return(self.currentTerm, True)
		else:
			return (self.currentTerm, False)

	def stop(self):
		self.stop_leader.wait(5)


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = int(sys.argv[3]))
	server.start()
