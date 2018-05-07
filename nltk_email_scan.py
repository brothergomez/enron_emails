"""
Code to intesect the emails with a lsit of interesting topics, then flag each with 
a "message of interest : True value/ Takes about 20 minutes to run on the whole dataset
"""

import pymongo
import nltk
from pymongo import MongoClient
import datetime
import email

# connect
cn = MongoClient("localhost")
db = cn.enron_mail_2
counter = 1
# open line by line words of interest
word_lines = set(line.strip() for line in open("words_of_interest.txt"))

# loop through all mail items
for document in db.mail.find():
    mail = document["text"]
    # remove all text after the "To:" string, hopefully this removes forwarded emails and old emails
    #
    mail = mail.split("To:")[0]

    counter += 1
    # tokenize mail to hopefully split out words
    doc_words = set(nltk.word_tokenize(mail))

    # count and print every 100 emails
    if counter % 100 == 0:
        print(datetime.datetime.utcnow(), counter)
    message_of_interest = (len(doc_words.intersection(word_lines)) > 0)

    db.mail.update_one({
        "_id": document["_id"]
    }, {
        "$set": {
            "message_of_interest": message_of_interest
        }
    }, upsert=False)
    # print(doc_words.intersection(word_lines))


print("Finished updating:")
