import rpyc
import sys
import os
from random import randint
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
		del confdict["N"]
		del confdict["node"+str(server_no)]
		self.server_list = []
		for a, b in confdict.items():
			self.server_list.append(b)
		self.state_follower()
	'''
        x = is_leader(): returns True or False, depending on whether
        this node is a leader

        As per rpyc syntax, adding the prefix 'exposed_' will expose this
        method as an RPC call

        CHANGE THIS METHOD TO RETURN THE APPROPRIATE RESPONSE
	'''
	def exposed_is_leader(self):
		return False
	"""
		AppendEntries updates the receiver nodes with the term
		returns current term and if receiver succeeded in updating
	"""
	def exposed_AppendEntries(self, term, leaderId):

		return (term, success)

	def state_leader(self):
		pass

	def state_candidate(self):
		pass

	def state_follower(self):
		pass


if __name__ == '__main__':
	from rpyc.utils.server import ThreadPoolServer
	server = ThreadPoolServer(RaftNode(sys.argv[1], sys.argv[2]), port = sys.argv[3])
	server.start()
