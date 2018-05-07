from pymongo import MongoClient
from dateutil import parser
import datetime

cn = MongoClient('localhost')
db = cn.enron_mail
messages = db.messages
counter = 1

for document in db.messages.find():
    #print(document["_id"])
    counter =+ 1
    try:
        db.messages.update_one({
            '_id': document['_id']
            }, {
            '$set': {
                'message_date': parser.parse(document["message_date"])
            }
        }, upsert=False)
    except:
        pass
    #print(document["message_date"])
    if counter % 100 == 0:
        print("{0} {1}".format(counter, datetime.datetime.now()))
    
