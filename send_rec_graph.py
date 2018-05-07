import pymongo
import nltk
from pymongo import MongoClient
import datetime
import networkx as nx
import pandas as pd

cn = MongoClient('localhost')
db = cn.enron_mail_2
G = nx.DiGraph()


counter = 1
enron_emails = db.mail.find(
    {"recipients": {"$ne": None}})

for document in enron_emails:
    sender = document["sender"].split('@')[0]
    counter += 1
    if document["message_of_interest"] == True:
            moi_weight = 1
    else:
        moi_weight = 0

    if counter % 100 == 0:
        print(datetime.datetime.utcnow(), counter)

    for rec in document["recipients"]:
        rec_split = rec.split('@')[0]
        if G.has_edge(sender, rec_split):
            # we added this one before, just increase the weight by one
            G[sender][rec_split]['weight'] += 1
            G[sender][rec_split]['moi_weight'] += moi_weight
        else:
            # new edge. add with weight=1
            G.add_edge(sender, rec_split, weight=1, moi_weight= moi_weight)
print("done writing edges")
nx.write_gexf(G, "enron.gexf")
