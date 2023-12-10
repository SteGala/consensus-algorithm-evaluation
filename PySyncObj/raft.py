#!/usr/bin/env python
from __future__ import print_function
import random

import sys
sys.path.append("/mnt/c/Users/galax/Desktop/Github Projects/consensus-algorithm-evaluation/PySyncObj/")
import threading
import time
import pandas as pd
import numpy as np

from pysyncobj.syncobj import SyncObj, SyncObjConf, replicated

data = {}
barrier = None
DISPATCH_TIMEOUT = 0.5

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



def main(port, ports, JOBS, id, e):
    _g_kvstorage = None
    global data
    global barrier
    
    bid = {}
    
    selfAddr = port
    if selfAddr == 'readonly':
        selfAddr = None
    partners = ports

    _g_kvstorage = KVStorage(selfAddr, partners)
    barrier.wait()
    
    count = 0
    while count < JOBS and not e.is_set():
        jobName = "job_" + str(count) + "_" + str(id)
        
        bid[jobName] = random.random()        

        _g_kvstorage.set(jobName, bid[jobName])
            
        count+=1
    
        time.sleep(DISPATCH_TIMEOUT)
        
    data[id] = {}
    data[id]["message_count"] = _g_kvstorage.getMessageCount()
    
    if e.is_set():
        return
    
    for j in range(JOBS):
        best_bet = -1
        
        for n in range(len(ports)+1):
            jobName = "job_" + str(j) + "_" + str(n)
            val = _g_kvstorage.get(jobName)
            if val is not None:
                if val > best_bet:
                    best_bet = val
        
        myJobName = "job_" + str(j) + "_" + str(id)
        final_job_name = "job_" + str(j)
        data[id][final_job_name] = {}
        if best_bet == bid[myJobName]:
            data[id][final_job_name]["winner"] = id
        else:
            data[id][final_job_name]["winner"] = -1
    
def test(run, n_jobs, n_nodes, n_failures):
    NODES = n_nodes
    JOBS = n_jobs
    
    threads = []
    events = []
    ports = [str((20000 + i)+n_nodes*int(run)) for i in range(NODES)]
    
    failure_nodes = random.sample(range(NODES), int(n_failures * n_nodes))
    
    global barrier
    barrier = threading.Barrier(n_nodes) 
    
    # print()
    # print(f"Running RAFT with {NODES} nodes and {JOBS} jobs.")
    
    for i in range(NODES):
        tmp = []
        e = threading.Event()
        for j in range(NODES):
            if j != i:
                tmp.append(ports[j])

        x = threading.Thread(target=main, args=(ports[i], tmp, JOBS, i, e))
        x.start()
        threads.append(x)
        events.append(e)
    
    posiible_time_failure = np.arange(DISPATCH_TIMEOUT, DISPATCH_TIMEOUT*n_jobs - DISPATCH_TIMEOUT, DISPATCH_TIMEOUT)
    time.sleep(random.choice(posiible_time_failure))
    for n in failure_nodes:
        events[n].set() 
    
    for t in threads:
        t.join()
            
    # for key in data:
    #     print(f"Node {key} bids: {data[key]}")
        
    res = {}
    count_assigned, count_unassigned = 0, 0
    for j in range(JOBS):
        winner = []
        res[str(j)] = {}
        for i in range(NODES):
            if i not in failure_nodes and data[i]["job_" + str(j)]["winner"] != -1:
                winner.append(data[i]["job_" + str(j)]["winner"])
        if len(winner) != 1:
            #print(f"Inconsistency detected. Multiple nodes winner: {winner}")
            res[str(j)]["final_node_allocation"] = -1
            count_unassigned += 1
        else:
            #print(f"Job {j} winner: node {winner[0]}")
            if len(winner) == 0:
                res[str(j)] ["final_node_allocation"] = -1
                count_unassigned += 1
            else:
                res[str(j)]["final_node_allocation"] = winner[0]
                count_assigned += 1
            
    #pd.DataFrame(res).T.to_csv(f"raft_{run}_jobs_report_" + str(n_failures) + "_failures.csv", )
            
    message_count = 0
    for i in range(NODES):
        message_count += int(data[i]["message_count"])
        
    return message_count, count_assigned, count_unassigned
    #print(f"Total message count: {message_count}")   
        
        
