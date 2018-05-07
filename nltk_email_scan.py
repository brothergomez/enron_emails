import pymongo
import nltk
from pymongo import MongoClient
import datetime
import email


cn = MongoClient('localhost')
db = cn.enron_mail_2
counter = 1

word_lines = set(line.strip() for line in open('words_of_interest.txt'))


for document in db.mail.find():
    mail = document["text"]
    mail = mail.split('To:')[0]

    counter += 1
    doc_words = set(nltk.word_tokenize(mail))

    #print(doc_words.intersection(word_lines))
    if counter % 100 == 0:
        print(datetime.datetime.utcnow(), counter)
    message_of_interest = (len(doc_words.intersection(word_lines)) > 0)
    
    db.mail.update_one({
        '_id': document['_id']
    }, {
        '$set': {
            'message_of_interest': message_of_interest
        }
    }, upsert=False)
    # print(doc_words.intersection(word_lines))


print("Finished updating:")
