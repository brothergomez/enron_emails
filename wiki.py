'''
Code to get the wiki article on Enron, then  get all named entities using the stanford
named entity recognition tool. We get organisations and people only, then manually
remove first names and common place names. 
'''

import wikipedia
from nltk.tag import pos_tag
from nltk.tag.stanford import StanfordNERTagger
from nltk import sent_tokenize
from nltk.tokenize import word_tokenize
import re
import os
import pprint

# Was having issues with java so explicitly set java home
# , you will need to edit this based on java version
java_path = r"C:/Program Files/Java/jdk1.8.0_151/bin/java.exe"
os.environ['JAVAHOME'] = java_path

# give stanfored NER paths to the gz and jar required
GZ_LOC = os.path.join(r'C:/Users/broth/Enron/stanford-ner/classifiers',
                      r'english.all.3class.distsim.crf.ser.gz')
JAR_LOC = os.path.join(r'C:/Users/broth/Enron/stanford-ner',
                       r'stanford-ner.jar')
print(JAR_LOC)
print(GZ_LOC)

# get the text contents of the wiki page
wikipage = wikipedia.WikipediaPage('Enron Scandal')
wikitext = wikipage.content

# run the NERtagger
st = StanfordNERTagger(GZ_LOC, JAR_LOC)

POI = set()
# For each entity, add it to set if it's a pers or organisation, hopefully this outputs
# interesting stuff
for sent in sent_tokenize(wikitext):
    tokens = word_tokenize(sent)
    tags = st.tag(tokens)
    for tag in tags:
        if (tag[1] == 'PERSON') or (tag[1] == 'ORGANIZATION'):
            POI.add(tag[0])

pprint.pprint(POI)
