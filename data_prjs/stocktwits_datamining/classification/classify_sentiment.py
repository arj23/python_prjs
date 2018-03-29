import pymongo
import re
import urllib

username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

db_address = 'mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin'

def connect_to_mongodb (db_url, db_name):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name]

def message_cleaning(message) :
    message_ = ""
    message_ += message
    message_ = message_.replace("not " , "negtag_")
    message_ = message_.replace("no " , "negtag_")
    message_ = message_.replace("none " , "negtag_")
    message_ = message_.replace("neither " , "negtag_")
    message_ = message_.replace("never " , "negtag_")
    message_ = message_.replace("neither " , "negtag_")
    message_ = message_.replace("nobody " , "negtag_")
    message_ = message_.replace(" a " , " ")
    message_ = message_.replace(" an " , " ")
    message_ = message_.replace(" the " , " ")
    message_ = message_.replace(":(" , "emojineg")
    message_ = message_.replace(":-(" , "emojineg")
    message_ = message_.replace(":)" , "emojipos")
    message_ = message_.replace(":-)" , "emojipos")
    message_ = re.sub(r"\$\S+",'cashtag', message_)
    message_ = re.sub(r"\@\S+",'usertag', message_)
    message_ = re.sub(r"https://\S+",'linktag', message_)
    message_ = re.sub(r"http://\S+",'linktag', message_)

    return  message_

def test() :
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    msg_collection = db_obj['suggested_msg']
    bullish_collection  = db_obj['bullish_msg']
    bearish_collection = db_obj['bearish_msg']
    bullish_collection.remove({})
    bearish_collection.remove({})
    message = ":) my proe a vendore the pure $1Sfg @RSDF not happen neither receive no good an example http://stackoverflow.com/questions/4643142/regex-to-test-if-string-begins-with-http-or-https  @mamad :(  "
    message_ = message_cleaning(message)
    print(message_)

def main():
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    msg_collection = db_obj['suggested_msg']
    bullish_collection  = db_obj['bullish_msg']
    bearish_collection = db_obj['bearish_msg']
    bullish_collection.remove({})
    bearish_collection.remove({})

    result_curser = msg_collection.find()

    for message in result_curser :
        if (message['entities']['sentiment']):
            if(message['entities']['sentiment']['basic'] == 'Bullish') :
                bullish_collection.insert({'user' : message['user']['username'] , 'created_at' : message['created_at'] , 'body' : message['body'], 'processed_body' : message_cleaning(message['body'] )})
            if(message['entities']['sentiment']['basic'] == 'Bearish') :
                bearish_collection.insert({'user' : message['user']['username'] , 'created_at' : message['created_at'] , 'body' : message['body'], 'processed_body' : message_cleaning(message['body'] )})



if __name__ == "__main__": main()
