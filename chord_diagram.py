from pymongo import MongoClient
import pandas as pd
import json
from bson.son import SON
import pprint

df_nodes = pd.read_csv('nodes.csv', sep=',')
df_edges = pd.read_csv('edges.csv', sep=',')
i = 1
data = []
print(df_edges.head())
for idx, row in df_nodes.iterrows():
    i += 1
    # print(row[4])
    nodename = row[0]
    modclass = row[3]
    outlist = []
    df_temp = df_edges[df_edges['Source'] == row[0]]
    # print(df_temp.head())
    for idx2, row2 in df_temp.iterrows():
        outlist.append(row2['Target'])
    data.append({'name': nodename, "mod_class": modclass,  'outs': outlist})

with open('mytext.txt', 'w') as f:
    f.writelines(str(data))
# pprint.pprint(data)
