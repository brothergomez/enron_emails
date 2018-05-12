
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from pymongo import MongoClient
import datetime
import numpy as np
import pandas as pd

# connect
cn = MongoClient("localhost")
db = cn.enron_mail
counter = 1
sid = SentimentIntensityAnalyzer()
avg_sent = 0

# loop through all mail items with one recipient

for document in db.messages.find({"recipients":{"$size":1}}):
    mail = document["text"]
    # remove all text after the "To:" string, hopefully this removes forwarded emails and old emails
    #
    mail = mail.split("To:")[0]

    counter += 1
    if counter % 100 == 0:
        print(datetime.datetime.utcnow(), counter)
        print(avg_sent)
    
    ss = sid.polarity_scores(mail)
    avg_sent = (avg_sent + ((ss['compound']-avg_sent)/counter))
    

    db.messages.update_one({
        "_id": document["_id"]
    }, {
        "$set": {
            "sentiment": float(ss['compound'])
        }
    }, upsert=False)

summary = db.messages.aggregate([  
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
         },
         "count":{
             "$sum":1
         }
      }
   },
   {
       "$sort": {"avgsent": 1}
   }
])

df = pd.DataFrame(list(summary))
print(df.head())

df.to_csv('sentiment emails.csv')