"""
Load data from mongo and input into networkx directed graph, then output to gephi. This is 
done on a limited subset of the enron emails, circa 2K emails
"""

import pymongo
import nltk
from pymongo import MongoClient
import datetime
import networkx as nx
import pandas as pd

# connect to mongo
cn = MongoClient('localhost')
db = cn.enron_mail_2
G = nx.DiGraph()

# counter to log progress
counter = 1
enron_emails = db.mail.find(
    {"recipients": {"$ne": None}})

# loop through all emails
for document in enron_emails:
    # get rid of the stuff after @ as enron.com, net can confuse things
    sender = document["sender"].split('@')[0]
    counter += 1
    # add 2 sets of weights, one for normal emails and one for messages of interest
    if document["message_of_interest"] == True:
        moi_weight = 1
    else:
        moi_weight = 0

    # print every 100 emails
    if counter % 100 == 0:
        print(datetime.datetime.utcnow(), counter)

    # loop over all recipients, doing the same thing
    for rec in document["recipients"]:
        rec_split = rec.split('@')[0]
        if G.has_edge(sender, rec_split):
            # we added this one before, just increase the weight by one
            G[sender][rec_split]['weight'] += 1
            G[sender][rec_split]['moi_weight'] += moi_weight
        else:
            # new edge. add with weight=1
            G.add_edge(sender, rec_split, weight=1, moi_weight=moi_weight)
print("done writing edges")
# output to gephi
nx.write_gexf(G, "enron.gexf")
