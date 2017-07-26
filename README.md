let Redis support the Raft Consensus Algorithm.

the following files are added to implement raft algorithm:

raft_cluster.c  \
raft.h      \
raft_log.h   
raft_private.h  
raft_server_properties.c \
raft_cluster.h  \
raft_log.c  \
raft_node.c  \
raft_server.c

raft_cluster.c is the entry and init for raft

It uses LMDB for storing data,

building:
make

running:
cd src

start three shell terminal and init the process respectively:

terminal 1: ./server --raft-enabled yes --port 8000 --loglevel verbose
 
terminal 2: ./server --raft-enabled yes --port 8001 --loglevel verbose
 
terminal 3: ./server --raft-enabled yes --port 8002 --loglevel verbose

note:
1. after the starting of the process, the leader will be elected based on 
   the raft consensus algorithm, 
2. the leader, follower and candidate is changing dynamically according to 
   the cluster node status. 
3. the cluster currently only spy three nodes, it will be configurabe by 
   user in future version if has.
