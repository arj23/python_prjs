import pymongo
import urllib

username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

db_address = 'mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin'

def connect_to_mongodb (db_url, db_name):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name]


def main():
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    sw_collection  = db_obj['sw_collection']
    table5  = db_obj['table5']
    bullish_collection  = db_obj['bullish_msg']
    bearish_collection = db_obj['bearish_msg']
    result = sw_collection.find()

    term_list = []
    calculated_SW = []
    L1_SW = []

    for message in result :
        term_list.append(message['term'])
        calculated_SW[message['term']] = message['calculated_SW']
        L1_SW[message['term']] = message['L1_SW']

    result = bullish_collection.find()
    count = bullish_collection.count()
    correct_0 = 0
    correct_L2 = 0
    classified_0 = 0
    classified_L2 = 0

    for message in result :
        message = ' ' + message + ' '
        sw_0 = 0;
        sw_L2 = 0;
        for term in term_list:
            term = ' ' + term + ' '
            if term in message:
                sw_0 += calculated_SW[term]
                sw_L2 += L1_SW[term]
        if(sw_0 > 0) :
            correct_0 += 1
        if(sw_L2 > 0) :
            correct_L2 += 1
        if(sw_0 != 0) :
            classified_0 += 1
        if (sw_L2 != 0):
            classified_L2 += 1

    table5.insert({'correct_0': correct_0, 'correct_L2': correct_L2, 'classified_0': classified_0, 'classified_L2': classified_L2})
