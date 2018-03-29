import pymongo
import random
import urllib

username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

db_address = 'mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin'

def connect_to_mongodb (db_url, db_name):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name]


def main() :
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    bullish_collection  = db_obj['bullish_msg']
    bullish_collection_sample  = db_obj['bullish_msg_sample']
    bearish_collection = db_obj['bearish_msg']
    bearish_collection_sample  = db_obj['bearish_msg_sample']
    for i in range(75000) :
        count = bullish_collection.count()
        result = bullish_collection.find()[random.randrange(count)]
        bullish_collection_sample.insert(result)
        bullish_collection.remove(result)
        count = bearish_collection.count()
        result = bearish_collection.find()[random.randrange(count)]
        bearish_collection_sample.insert(result)
        bearish_collection.remove(result)


if __name__ == "__main__": main()
