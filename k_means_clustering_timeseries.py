"""
Performs a k means clustering and creates a graph over time of the topics
"""
import pymongo
from pymongo import MongoClient

import numpy as np
import pandas as pd
from time import time


import matplotlib.pyplot as plt
import seaborn as sns

from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfVectorizer


#  and the numebr of clusters:
N_CLUSTERS = 5

# Wrap everything in a if __name__ == "__main__":
# to avoid threading problems

if __name__ == "__main__":
    # Connect to mongodb, and use the enron_mail_2 db. This is  5k emails

    cn = MongoClient("localhost")
    db = cn.enron_mail_2

    # get all of the messages
    emails = db.mail.find()

    # put it into a dataframe to feed into scikit learn
    emails_df = pd.DataFrame(list(emails))
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
        for ind in order_centroids[i, :10]:
            print(" %s" % terms[ind], end="")
        print()

    # arrange the summary dataframe to pivot bymonth and date
    summary_df = emails_df
    summary_df["date"] = pd.to_datetime(summary_df["date"])
    summary_df["date"] = summary_df["date"].dt.strftime('%Y-%m')
    summary_df = pd.pivot_table(
        summary_df, values="_id", index="date", columns="label", aggfunc="count").fillna(0)
    print(summary_df)
    # set plotting options and plot the rolling 3 month average
    sns.set_style("darkgrid")
    plt.plot(summary_df.rolling(summary_df, 3, center=False).mean())
    axes = plt.gca()
    axes.set_ylim([0, 450])
    plt.xticks(rotation=90)
    plt.show()

    summary_df.to_csv("./Outputs/TimeSeriesClusters.csv")
