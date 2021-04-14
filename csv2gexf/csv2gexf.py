import sys,pprint
from _gexf import Gexf,GexfImport
import dask.dataframe as dd
import pandas as pd
import numpy as np

def open_csv(file):
    try:
        df = dd.read_csv(file).compute()
        return df
    except Exception as e:
        print(str(e))


def csv2gexf(file):
    df = open_csv(file)
    #print(df.T[3][' Source IP'])
    my_queues1 = df.T.values[1]#源地址
    my_queues2 = df.T.values[3]#目的地址
    List=[]
    for i, j in zip(my_queues1,my_queues2):
        x = (i,j)
        List.append(x)
    temp1 = np.array(List)#边的数组，有重复

    nodes = {}
    for e in temp1:
        if e[0] not in nodes.keys():
            nodes[e[0]]={}
            nodes[e[0]][e[1]] = 1
        elif e[1] not in nodes[e[0]].keys():
            nodes[e[0]][e[1]] = 1
        else:
            nodes[e[0]][e[1]] += 1
        if e[1] not in nodes.keys():
            nodes[e[1]]={}

    # test helloworld.gexf
    gexf1 = Gexf("Gephi.org","A Web network")
    graph=gexf1.addGraph("directed","static","A Web network")
    atr1 = graph.addNodeAttribute(force_id="modularity_class",title='Modularity Class',type='string')
    
    kn,ke = 0,0
    for node in nodes.keys():
        tmp = graph.addNode(node,node,r="130",g="206",b="235")
        tmp.addAttribute(atr1,str(kn))
        kn += 1 
    
    for node,edge in nodes.items():
        for dst in edge.keys():
            graph.addEdge(str(ke),node,dst,weight=str(edge[dst]))
            ke += 1
  
    output_file=open(".\data2.gexf","wb")
    gexf1.write(output_file)

if __name__ =='__main__':
    csv2gexf(r"E:\XZL\FinalProject\copy2.csv")