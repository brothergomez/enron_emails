from pymongo import MongoClient
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt

# connect
cn = MongoClient("localhost")
db = cn.enron_mail_2
counter = 1

docs = db.mail.find({"recipients": {"$size": 1}}, {"sentiment": 1, "date": 1})
df = pd.DataFrame(list(docs))
df.to_csv("sentiment.csv")


df = df.groupby("date")["sentiment"].rolling(7).mean().to_frame()

#df.set_index("date", inplace=True)
print(df.head(20))

stocks = pd.read_csv("enronstockchart.csv")
# print(stocks.head())

stocks["Date"] = pd.to_datetime(stocks["Date"])
#stocks["Date"] = stocks["Date"].dt.strftime(r'%Y-%m-%d')
stocks = stocks.groupby("Date")["Close"].agg({"Close": "mean"}).reset_index()
stocks.set_index("Date", inplace=True)
print(stocks.head())

merge = pd.merge(df, stocks, how='inner', left_index=True, right_index=True)
#summary_df = pd.concat([df, stocks], join_axes=[df.date, stocks.Date])

merge.to_csv("sentiment_vs_stock.csv")