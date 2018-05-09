
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pymongo import MongoClient
import datetime
import numpy as np

# connect
cn = MongoClient("localhost")
db = cn.enron_mail_2
counter = 1
sid = SentimentIntensityAnalyzer()
avg_sent = 0

# loop through all mail items with one recipient

for document in db.mail.find({"recipients":{"$size":1}}):
    mail = document["text"]
    # remove all text after the "To:" string, hopefully this removes forwarded emails and old emails
    #
    mail = mail.split("To:")[0]

    counter += 1
    if counter % 100 == 0:
        print(datetime.datetime.utcnow(), counter)
    
    
    ss = sid.polarity_scores(mail)
    avg_sent = (avg_sent + ((ss['compound']-avg_sent)/counter))
    print(avg_sent)

    db.mail.update_one({
        "_id": document["_id"]
    }, {
        "$set": {
            "sentiment": float(ss['compound'])
        }
    }, upsert=False)
    

summary = db.mail.aggregate([  
   {  
      "$match":{  
         "recipients":{  
            "$size":1
         }
      }
   },
    {  
      "$project":{  
         "_id":0,
         "recipients":1,
         "sentiment":1
      }
   },
   {  
      "$unwind":"$recipients"
   },
   {  
      "$group":{  
         "_id":"$recipients",
         "avgsent":{  
            "$avg":"$sentiment"
         }
      }
   },
   {
       "$sort": {"avgsent": 1}
   },
   {
       "$limit" :10
   }
])

