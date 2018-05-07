
import nltk
from pymongo import MongoClient
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
import datetime
from os import path
from os import stat
from wordcloud import WordCloud, STOPWORDS
from PIL import Image
import matplotlib.pyplot as plt

# connect to mongo
cn = MongoClient('localhost')
db = cn.enron_mail_2
counter = 1

f = open("all_text.txt", "a+")

if stat("all_text.txt").st_size == 0:
    for document in db.mail.find():
        mail = document["text"]
        # remove all text after the "To:" string, hopefully this removes forwarded emails and old emails
        #
        mail = mail.split("To:")[0]
        mail = nltk.word_tokenize(mail)
        #print(mail)
        mail = [
            word for word in mail if word not in stopwords.words('english')]
        #print(mail)
        counter += 1
        # tokenize mail to hopefully split out words
        if counter % 100 == 0:
            print(datetime.datetime.utcnow(), counter)

        doc_words = f.write(" ".join(mail))


text = open("all_text.txt").read()

enron_mask = np.array(Image.open('EnronLogo.png'))
wc = WordCloud(background_color="white", max_words=2000, mask=enron_mask)

wc.generate(text)
wc.to_file("enron_logo_wc.png")

plt.imshow(wc, interpolation='bilinear')
plt.axis("off")
plt.figure()
plt.imshow(enron_mask, interpolation='bilinear')
plt.axis("off")
plt.show()
