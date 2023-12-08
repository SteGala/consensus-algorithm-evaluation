#!/usr/bin/env python
from __future__ import print_function
import random

import sys
import threading
import time

from pysyncobj.syncobj import SyncObj, SyncObjConf, replicated

data = {}

class KVStorage(SyncObj):
    def __init__(self, selfAddress, partnerAddrs):
        cfg = SyncObjConf(dynamicMembershipChange = True)
        partners = ['localhost:%d' % int(p) for p in partnerAddrs]
        super(KVStorage, self).__init__("localhost:" + selfAddress, partners, cfg)
        self.__data = {}

    @replicated
    def set(self, key, value):
        self.__data[key] = value

    @replicated
    def pop(self, key):
        self.__data.pop(key, None)

    def get(self, key):
        return self.__data.get(key, None)
    
    def getMessageCount(self):
        # call the same function i the base class
        return super(KVStorage, self).getMessageCount()



def main(port, ports, JOBS, id):
    _g_kvstorage = None
    global data
    
    data[id] = {}
    
    selfAddr = port
    if selfAddr == 'readonly':
        selfAddr = None
    partners = ports

    _g_kvstorage = KVStorage(selfAddr, partners)
    
    count = 0
    while count < JOBS:
        jobName = "job_" + str(count)
        if jobName not in data[id]:
            data[id][jobName] = {}
            data[id][jobName]["bet"] = random.random()        

        if _g_kvstorage.get(jobName) is None or data[id][jobName]["bet"] > _g_kvstorage.get(jobName):
            _g_kvstorage.set(jobName, data[id][jobName]["bet"])
            
        count+=1
        time.sleep(1)
                
    for j in data[id]:
        if data[id][j]["bet"] == _g_kvstorage.get(j):
            data[id][j]["winner"] = id
        else:
            data[id][j]["winner"] = -1
    
    data[id]["message_count"] = _g_kvstorage.getMessageCount()
    
if __name__ == '__main__':
    NODES = 10
    JOBS = 20
    threads = []
    ports = [str(20000 + i) for i in range(NODES)]
    
    print(f"Running RAFT with {NODES} nodes and {JOBS} jobs.")
    
    for i in range(NODES):
        tmp = []
        for j in range(NODES):
            if j != i:
                tmp.append(ports[j])

        x = threading.Thread(target=main, args=(ports[i],tmp, JOBS, i))
        x.start()
        threads.append(x)
    
    for t in threads:
        t.join()
                
    for j in range(JOBS):
        winner = []
        for i in range(NODES):
            if data[i]["job_" + str(j)]["winner"] != -1:
                winner.append(data[i]["job_" + str(j)]["winner"])
        if len(winner) != 1:
            print(f"Inconsistency detected. Multiple nodes winner: {winner}")
        else:
            print(f"Job {j} winner: node {winner[0]}")
            
    message_count = 0
    for i in range(NODES):
        message_count += data[i]["message_count"] 
        
    print(f"Total message count: {message_count}")   
        
        
