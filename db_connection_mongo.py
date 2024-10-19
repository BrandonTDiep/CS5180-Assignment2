#-------------------------------------------------------------------------
# AUTHOR: Brandon Diep
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Connects to my DB and performs CRUD operations
# FOR: CS 5180 - Assignment #2
# TIME SPENT: 2 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import re
from datetime import datetime

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "CPP"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:

        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db

    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    text = re.sub(r'[^\w\s]', "", docText)
    text = text.lower()
    num_char = 0
    for char in (list(text.replace(" ", ""))):
        num_char += 1
    terms = text.split()

    dictTerm = {}
    for term in terms:
        if term in dictTerm:
            dictTerm[term] = dictTerm[term] + 1
        else:
            dictTerm[term] = 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    termList = []
    for term, termCount in dictTerm.items():
        termList.append({"term": term, "term_count": termCount, "num_chars": len(term)})
    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "_id": int(docId),
        "text": docText,
        "title": docTitle,
        "num_chars": num_char,
        "date": datetime.strptime(docDate, "%Y-%m-%d"),
        "category": docCat,
        "terms": termList
    }

    # insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": int(docId)})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)
    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here

    pipeline = [
        {"$unwind": {"path": "$terms"}},
        {"$sort": {"terms.term": 1}}
    ]

    terms = col.aggregate(pipeline)

    dicIndex = {}

    for term in terms:
        if dicIndex.get(term['terms']['term']) is None:
            dicIndex[term['terms']['term']] = term['title'] + ":" + str(term['terms']['term_count'])
        else:
            dicIndex[term['terms']['term']] += ", " + term['title'] + ":" + str(term['terms']['term_count'])

    return dicIndex
