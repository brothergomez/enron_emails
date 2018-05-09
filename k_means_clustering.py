"""
Analyiis of Enron emails, with a focus on Tim Belden
Code to load tim beldens contacts, then get a list of their contacts whcih in turn we get emails from. 
This amounts to a "neighbours of neighbours" search and will return a large subset of the enron mails. 

Then performs a k means clustering and outputs the location of a few interesting parties emails, as well
as the topics of each of the clusters. 
"""
import pymongo
from pymongo import MongoClient

import numpy as np
import pandas as pd
from time import time
import sys

import matplotlib.pyplot as plt
import seaborn as sns
sns.set_style("whitegrid")
import colorsys

from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.pipeline import Pipeline
from sklearn.decomposition import PCA
from sklearn.feature_extraction import text


# Some general inputs:
# persons_of_interest is a list including tim belden, our focus,
# and some other executives implicated in the scandal.

persons_of_interest = ["ken.lay@enron.com",
                       "tim.belden@enron.com",
                       "jeff.skilling@enron.com",
                       "richard.shapiro@enron.com",
                       "kenneth.lay@enron.com",
                       "klay@enron.com"]
#  and the numebr of clusters:
N_CLUSTERS = 10

# Wrap everything in a if __name__ == "__main__":
# to avoid threading problems

if __name__ == "__main__":
    # Connect to mongodb, and use the enron_mail db. This is all 500k+ emails

    cn = MongoClient("localhost")
    db = cn.enron_mail

    # Get all of beldens contact from mongo
    belden_contacts = db.messages.find({"$or": [{"senders": "tim.belden@enron.com"},
                                                {"recipients": "tim.belden@enron.com"}],
                                        "senders": {"$ne": None}, "recipients": {"$ne": None}},
                                       {"_id": 0, "recipients": 1, "senders": 1})

    # we get a set of emails, and add to each of the recipients and
    # senders from each of beldens contacts, to get contacts of contacts
    emails = set()

    for doc in belden_contacts:
        recs = doc["recipients"]
        senders = doc["senders"]
        emails = emails.union([rec.replace("\n\t", "") for rec in recs])
        emails = emails.union([senders])
    print(len(emails), "Emails in belden\'s contacts")

    # get all of the messages from  beldens contacts, and their contact"s emails
    belden_emails = db.messages.find({"$or": [{"senders": {"$in": list(emails)}},
                                              {"recipients": {"$in": list(emails)}}]},
                                     {"_id": 1, "text": 1, "recipients": 1, "senders": 1})

    print(belden_emails.count(), "emails in beldens contacts\' contacts")

    # put it into a dataframe to feed into scikit learn
    emails_df = pd.DataFrame(list(belden_emails))
    print(emails_df.shape)
    # print(emails_df.head(5))

    # use a term frequency vectorizer
    vect = TfidfVectorizer(sublinear_tf=True,
                           min_df=0.05, max_df=0.5,
                           stop_words="english",
                           token_pattern=r"\b[A-Za-z]{3}[A-Za-z]*\b")
    # k-means clustering with N_CLUSTERS, 50 iterations should do it. Initialise 10 times to make sure
    clf = KMeans(n_clusters=N_CLUSTERS,
                 random_state=0,
                 max_iter=50,
                 init="k-means++",
                 n_init=10,
                 verbose=True,
                 n_jobs=-1)

    print("Clustering data with %s" % clf)
    # run clustering, takes about 20 minutes on a fast PC.
    t0 = time()
    word_vec = vect.fit_transform(emails_df["text"])
    labels = clf.fit_predict(word_vec)
    print("done in %0.3fs" % (time() - t0))
    word_vec_2d = word_vec.todense()

    emails_df["label"] = labels

    # get the centroids of the clusters and order them
    order_centroids = clf.cluster_centers_.argsort()[
        :, ::-1]
    terms = vect.get_feature_names()

    # get the top terms for each of the clusters
    for i in range(N_CLUSTERS):
        print("Cluster %d:" % i, end="")
        for ind in order_centroids[i, :N_CLUSTERS]:
            print(" %s" % terms[ind], end="")
        print()

    # for our list of interesting persons, we see how many emails they have in each cluster
    for persons in persons_of_interest:
        summary_df = emails_df[(emails_df["senders"] == persons) |
                               (emails_df["senders"].str.contains(persons) == True)]
        summary_df = summary_df.groupby(["label"]).size()
        print(persons + "\"categories:")
        print(summary_df)

    # do a principal component analysis on the clusters to reduce down to 2 dimensions
    pca = PCA(n_components=2).fit(word_vec_2d)
    datapoint = pca.transform(word_vec_2d)
    datapoint_df = pd.DataFrame(datapoint, columns=["x", "y"])

    # add in the labels data to colour by the cluster labels
    datapoint_df["label"] = labels

    # colour pallette
    clrs = list(sns.color_palette("Set2", N_CLUSTERS))
    colours = [clrs[i] for i in labels]

    # get the locations of the centroid poiunts and print, for reference
    X = clf.cluster_centers_

    centroidpoint = pd.DataFrame(pca.transform(X), columns=["x", "y"])
    print(centroidpoint)

    # plot the graph as a scatter, hopefully they are arranges in a clustering
    p1 = sns.lmplot(data=datapoint_df, x="x", y="y", fit_reg=False,
                    hue="label", scatter_kws={"s": 10})

    plt.show()
