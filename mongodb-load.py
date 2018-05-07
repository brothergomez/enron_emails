import os
import datetime
from pymongo import MongoClient
from email.parser import Parser
from dateutil import parser as date_parser

# gist taken from https://gist.github.com/k0emt/1120767 with updates to parse dates 
# and to process senders and recipients
# you will need an unzipped folder with the enron emails dataset

__author__ = 'k0emt'
MAIL_DIR_PATH = 'C:/Users/broth/Downloads/enron_mail_20150507.tar/maildir'
PREFIX_TRIM_AMOUNT = len(MAIL_DIR_PATH) + 1
MAX_USER_RUN_LIMIT = 0
MAX_USER_EMAILS_PER_FOLDER_FILE_LIMIT = 0
counter = 1

# open file as raw text and use eamil parser to parse info
def get_file_contents(file_to_open_name):
    data_file = open(file_to_open_name)
    try:
        data=data_file.read()
        email = Parser().parsestr(data)
    finally:
        data_file.close()
    return email

# save to database with required fields, recipients will save as array
def save_to_database(mailbox_owner_name, sub_folder, file_name, message_contents):
    document = {"mailbox": mailbox_owner_name,
                "subFolder": sub_folder,
                "filename": file_name,
                "message_date": date_parser.parse(message_contents["message_date"]),
                "senders": message_contents['from'],
                "recipients": message_contents['to'],
                "subject": message_contents['subject'].replace('\n', ''),
                "text": message_contents.get_payload()}

    messages = db.messages
    messages.insert(document)
    return

cn = MongoClient('localhost')
db = cn.enron_mail
print("database initialized {0}".format(datetime.datetime.now()))

# all the mail folders
user_counter = 0
previous_owner = ""

for root, dirs, files in os.walk(MAIL_DIR_PATH, topdown=False):
    directory = root[PREFIX_TRIM_AMOUNT:]

    # extract mail box owner
    parts = directory.split('/', 1)
    mailbox_owner = parts[0]

    if previous_owner != mailbox_owner:
        previous_owner = mailbox_owner
        user_counter += 1

    # sub-folder info
    if 2 == len(parts):
        subFolder = parts[1]
    else:
        subFolder = ''

    # files in each mail folder
    folder_email_counter = 0

    for file in files:

        # get the file contents
        name_of_file_to_open = "{0}/{1}".format(root, file)
        contents = get_file_contents(name_of_file_to_open)
        save_to_database(mailbox_owner, subFolder, file, contents)

        folder_email_counter += 1
        counter += 1
        if counter % 100 == 0:
            print("{0} {1}".format(counter, datetime.datetime.now()))

        if MAX_USER_EMAILS_PER_FOLDER_FILE_LIMIT > 0 and MAX_USER_EMAILS_PER_FOLDER_FILE_LIMIT == folder_email_counter:
            break

    if MAX_USER_RUN_LIMIT > 0 and MAX_USER_RUN_LIMIT == user_counter:
        print("Maximum users limit {0} met.".format(MAX_USER_RUN_LIMIT))
        break

db.close
print("database closed {0}".format(datetime.datetime.now()))
print("{0} total records processed".format(counter - 1))