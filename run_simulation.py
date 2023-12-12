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
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_10_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_prob_10_msg[failure]
    tmp["Algorithms"] = ["Plebi 0.1 prob topology" for _ in range(len(plebiscito_prob_10_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_30_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_prob_30_msg[failure]
    tmp["Algorithms"] = ["Plebi 0.3 prob topology" for _ in range(len(plebiscito_prob_30_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_50_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_prob_50_msg[failure]
    tmp["Algorithms"] = ["Plebi 0.5 prob topology" for _ in range(len(plebiscito_prob_50_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_70_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_prob_70_msg[failure]
    tmp["Algorithms"] = ["Plebi 0.7 prob topology" for _ in range(len(plebiscito_prob_70_msg[failure]))]
    data_msg = pd.concat([data_msg, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_90_msg[failure]))]
    tmp["Message Overhead"] = plebiscito_prob_90_msg[failure]
    tmp["Algorithms"] = ["Plebi 0.9 prob topology" for _ in range(len(plebiscito_prob_90_msg[failure]))]
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
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_10_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_prob_10_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi 0.1 prob topology" for _ in range(len(plebiscito_prob_10_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_30_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_prob_30_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi 0.3 prob topology" for _ in range(len(plebiscito_prob_30_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_50_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_prob_50_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi 0.5 prob topology" for _ in range(len(plebiscito_prob_50_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_70_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_prob_70_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi 0.7 prob topology" for _ in range(len(plebiscito_prob_70_jobs[failure][0]))]
    data_assigned = pd.concat([data_assigned, pd.DataFrame(tmp)])
    
    tmp = {}
    tmp["Node Failure (%)"] = [failure*100 for _ in range(len(plebiscito_prob_90_jobs[failure][0]))]
    tmp["Job assigned (%)"] = [100*(x/(JOBS)) for x in plebiscito_prob_90_jobs[failure][0]]
    tmp["Algorithms"] = ["Plebi 0.9 prob topology" for _ in range(len(plebiscito_prob_90_jobs[failure][0]))]
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
    print(data_assigned["Node Failure (%)"].value_counts())
    pl = sns.lineplot(
        data=data_assigned,
        x="Node Failure (%)", y="Job assigned (%)", hue="Algorithms", style="Algorithms",
        markers=True, dashes=False, ci=75
    )

    figure = pl.get_figure()  
    figure.tight_layout()  
    figure.savefig("raft_vs_plebiscito_jobs.png")
    
def clean_data_structures():
    raft_msg[f] = []
    plebiscito_prob_10_msg[f] = []
    plebiscito_prob_30_msg[f] = []
    plebiscito_prob_50_msg[f] = []
    plebiscito_prob_70_msg[f] = []
    plebiscito_prob_90_msg[f] = []
    
    raft_jobs[f] = [[], []]
    plebiscito_prob_10_jobs[f] = [[], []]
    plebiscito_prob_30_jobs[f] = [[], []]
    plebiscito_prob_50_jobs[f] = [[], []]
    plebiscito_prob_70_jobs[f] = [[], []]
    plebiscito_prob_90_jobs[f] = [[], []]

if __name__ == "__main__":
    # execute only functions
    if len(sys.argv) > 1:
        for function in sys.argv[1:]:
            globals()[function]()
        sys.exit(0)
    
    NODES = 10
    JOBS = 10
    RUNS = 30
    FAILURES = [0.3] # percentage of failures [0, 1]
    EDGE_TO_ADD = 0

    raft_msg = {}
    raft_jobs = {}
    plebiscito_prob_10_msg = {}
    plebiscito_prob_10_jobs = {}
    plebiscito_prob_30_msg = {}
    plebiscito_prob_30_jobs = {}
    plebiscito_prob_50_msg = {}
    plebiscito_prob_50_jobs = {}
    plebiscito_prob_70_msg = {}
    plebiscito_prob_70_jobs = {}
    plebiscito_prob_90_msg = {}
    plebiscito_prob_90_jobs = {}
    count = 0

    with tqdm(total=RUNS*len(FAILURES)) as pbar:
        for f in FAILURES:           
            for i in range(RUNS):
                clean_data_structures()
                
                # pbar.set_description(f"Running Plebiscito probability 0.1")        
                # msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "probability_graph", 0.1)
                # plebiscito_prob_10_msg[f].append(msg)
                # plebiscito_prob_10_jobs[f][0].append(a_jobs)
                # plebiscito_prob_10_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running Plebiscito probability 0.3")        
                msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "probability_graph", 0.3)
                plebiscito_prob_30_msg[f].append(msg)
                plebiscito_prob_30_jobs[f][0].append(a_jobs)
                plebiscito_prob_30_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running Plebiscito probability 0.5")        
                msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "probability_graph", 0.5)
                plebiscito_prob_50_msg[f].append(msg)
                plebiscito_prob_50_jobs[f][0].append(a_jobs)
                plebiscito_prob_50_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running Plebiscito probability 0.7")        
                msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "probability_graph", 0.7)
                plebiscito_prob_70_msg[f].append(msg)
                plebiscito_prob_70_jobs[f][0].append(a_jobs)
                plebiscito_prob_70_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running Plebiscito probability 0.9")        
                msg, a_jobs, u_jobs = test_plebiscito(str(count), JOBS, NODES, f, EDGE_TO_ADD, "probability_graph", 0.9)
                plebiscito_prob_90_msg[f].append(msg)
                plebiscito_prob_90_jobs[f][0].append(a_jobs)
                plebiscito_prob_90_jobs[f][1].append(u_jobs)
                
                pbar.set_description(f"Running RAFT")
                msg, a_jobs, u_jobs = test_pysyncobj(str(count), JOBS, NODES, f)
                raft_msg[f].append(msg)
                raft_jobs[f][0].append(a_jobs)
                raft_jobs[f][1].append(u_jobs)
                
                count += 1
                pbar.update(1)
        
                save_job_data(f)
                save_msg_data(f)


