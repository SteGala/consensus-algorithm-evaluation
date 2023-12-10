import sys
from Plebiscito.plebiscito import test as test_plebiscito
from PySyncObj.raft import test as test_pysyncobj
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.ticker as ticker
from tqdm import tqdm
import seaborn as sns
import os.path

def save_msg_data(failure):
    data_msg = pd.DataFrame()
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_ring_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_ring_msg[failure]
    tmp["Algorithms"] = ["Plebi ring topology" for _ in range(len(plebiscito_ring_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_complete_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_complete_msg[failure]
    tmp["Algorithms"] = ["Plebi complete topology" for _ in range(len(plebiscito_complete_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(raft_msg[failure]))]
    tmp["Message Overhead"] = raft_msg[failure]
    tmp["Algorithms"] = ["RAFT" for _ in range(len(raft_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    if os.path.isfile("msg.csv"):
        data_msg.to_csv("msg.csv", mode='a', index=False, header=False)
    else:
        data_msg.to_csv("msg.csv", index=False)
        
def save_job_data(failure):
    data_assigned = pd.DataFrame()
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_ring_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_ring_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi ring topology" for _ in range(len(plebiscito_ring_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_complete_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_complete_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi complete topology" for _ in range(len(plebiscito_complete_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(raft_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in raft_jobs[failure][0]]
    tmp["Algorithms"] = ["RAFT" for _ in range(len(raft_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
        
    if os.path.isfile("msg.csv"):
        data_assigned.to_csv("jobs.csv", mode='a', index=False, header=False)
    else:
        data_assigned.to_csv("jobs.csv", index=False)
        
def plot_msg():  
    data_msg = pd.read_csv("msg.csv")   
    pl = sns.lineplot(
        data=data_msg,
        x="Node Failure (%)", y="Message Overhead", hue="Algorithms", style="Algorithms",
        markers=True, dashes=False
    )

    figure = pl.get_figure()  
    figure.tight_layout()  
    figure.savefig("raft_vs_plebiscito_msg.png")
    plt.clf()


def plot_jobs():
    data_assigned = pd.read_csv("jobs.csv")
    pl = sns.lineplot(
        data=data_assigned,
        x="Node Failure (%)", y="Job assigned (%)", hue="Algorithms", style="Algorithms",
        markers=True, dashes=False
    )

    figure = pl.get_figure()  
    figure.tight_layout()  
    figure.savefig("raft_vs_plebiscito_jobs.png")

if __name__ == "__main__":
    # execute only functions
    if len(sys.argv) > 1:
        for function in sys.argv[1:]:
            globals()[function]()
        sys.exit(0)
    
    NODES = 10
    JOBS = 10
    RUNS = 5
    FAILURES = [0, .1, .2, .3, .4, .5] # percentage of failures [0, 1]
    EDGE_TO_ADD = 0

    raft_msg = {}
    raft_jobs = {}
    plebiscito_ring_msg = {}
    plebiscito_ring_jobs = {}
    plebiscito_complete_msg = {}
    plebiscito_complete_jobs = {}
    count = 0

    with tqdm(total=RUNS*len(FAILURES)) as pbar:
        for f in FAILURES:
            raft_msg[f] = []
            plebiscito_ring_msg[f] = []
            plebiscito_complete_msg[f] = []
            
            raft_jobs[f] = [[], []]
            plebiscito_ring_jobs[f] = [[], []]
            plebiscito_complete_jobs[f] = [[], []]
            
            for i in range(RUNS):
                
                pbar.set_description(f"Running Plebiscito ring")        
                msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "ring_graph")
                plebiscito_ring_msg[f].append(msg)
                plebiscito_ring_jobs[f][0].append(a_jobs)
                plebiscito_ring_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running Plebiscito complete")
                msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "complete_graph")
                plebiscito_complete_msg[f].append(msg)
                plebiscito_complete_jobs[f][0].append(a_jobs)
                plebiscito_complete_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running RAFT")
                msg, a_jobs, u_jobs = test_pysyncobj(str(count), JOBS, NODES, f)
                raft_msg[f].append(msg)
                raft_jobs[f][0].append(a_jobs)
                raft_jobs[f][1].append(u_jobs)
                
                count += 1
                pbar.update(1)
        
            save_job_data(f)
            save_msg_data(f)


