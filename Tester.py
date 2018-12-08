import raftnode
import rpyc
from rpyc.utils.server import ThreadPoolServer
import threading
import time
from random import randint
import os

def main():
    x = [
    ThreadPoolServer(raftnode.RaftNode('config.txt', 0), port = 5001),
    ThreadPoolServer(raftnode.RaftNode('config.txt', 1), port = 5002),
    ThreadPoolServer(raftnode.RaftNode('config.txt', 2), port = 5003),
    ThreadPoolServer(raftnode.RaftNode('config.txt', 3), port = 5004),
    ThreadPoolServer(raftnode.RaftNode('config.txt', 4), port = 5005),
    ]

    server_list = []
    for server in x:
        thread1 = threading.Thread(target=server.start)
        server_list.append(thread1)
        thread1.start()
    # test 1
    while True:
        y = int(input("Leader ID: "))
        print(str(rpyc.connect('localhost', 5000+y).root.is_leader()))

def main2():
    bashCommand = "python3 raftnode.py config.txt 0 5001 &"
    os.system(bashCommand)
    bashCommand = "python3 raftnode.py config.txt 1 5002 &"
    os.system(bashCommand)
    bashCommand = "python3 raftnode.py config.txt 2 5003 &"
    os.system(bashCommand)
    bashCommand = "python3 raftnode.py config.txt 3 5004 &"
    os.system(bashCommand)
    bashCommand = "python3 raftnode.py config.txt 4 5005 &"
    os.system(bashCommand)

main2()
