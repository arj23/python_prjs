import pymongo
import requests
import json
import datetime
import time
import re
import urllib

username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

db_address = 'mongodb://'+ username +':' + password + '@88.99.153.217/admin?authSource=admin'
token_list = ['94b81109ac49597b357c75c5870526e3894877d8',
              '75c44c9341cf26a5383dfd76b687ee30817dd601',
              '7e297ff94f94fc1dd3d8047afc6be52bc0080ace']
def connect_to_mongodb (db_url, db_name):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name]

def is_json(myjson):
  try:
    json_object = json.loads(myjson)
  except ValueError:
    return False
  return True

def get_messages_from_stocktwits(token,last_message_id=None):
    api_url = ''
    if last_message_id is None:
        api_url = 'https://api.stocktwits.com/api/2/streams/all.json?access_token={}&filter=top&max'.format(token)
    else:
        api_url = 'https://api.stocktwits.com/api/2/streams/all.json?access_token={}&filter=top&max={}'.format(token,last_message_id)
    try:
        req = requests.get(api_url)
        if is_json(req.text) :
            data = json.loads(req.text)
            return data.get('messages')
    except requests.exceptions.ConnectionError:
        return None
    return None

def str_datetime_to_obj_datetime(datetime_str):
    date_str = datetime_str.split("T")[0];
    time_str = datetime_str.split("T")[1];
    year = date_str.split("-")[0]
    month = date_str.split("-")[1]
    day = date_str.split("-")[2]
    hour = time_str.split(":")[0]
    minute = time_str.split(":")[1]
    second = time_str.split(":")[2].split("Z")[0]
    date_time = datetime.datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
    return date_time

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
    message_ = re.sub(r"\b[0-9]+\b",'numbertag', message_)

    return  message_

def main() :
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    msgs_collection = db_obj['all_msgs']
    bullish_collection = db_obj['bullish_collection']
    bearish_collection = db_obj['bearish_collection']

    max_count = 1000000
    print(max_count)
    message_count = 0
    low_id = 0;
    token_list_id =0;
    while (message_count < max_count):
        if low_id is 0:
            print("there is no message in db to get lowest id")
            result = msgs_collection.find().sort("id", pymongo.ASCENDING).limit(1);
            for r in result:
                low_id = r['id']
        messages = get_messages_from_stocktwits(token_list[token_list_id], low_id)
        if messages is None or len(messages) == 0:
            print('Receive no message from request, trying again...')
            token_list_id += 1
            if token_list_id == len(token_list) :
                token_list_id = 0
            time.sleep(10)
            continue
        for message in messages:
            date_str = message['created_at'];
            datetime_obj = str_datetime_to_obj_datetime(date_str)
            # print(datetime_obj)
            clean_message = message_cleaning(message['body'])
            low_id = message['id']
            if (message['entities']['sentiment']):
                if (message['entities']['sentiment']['basic'] == 'Bullish'):
                    bullish_collection.insert({'id': message['id'],
                                               'user_username': message['user']['username'],
                                               'user_id': message['user']['id'],
                                               'created_at': datetime_obj,
                                               'body': message['body'],
                                               'processed_body': clean_message})
                    msgs_collection.insert({'id': message['id'],
                                            'user_username': message['user']['username'],
                                            'user_id': message['user']['id'],
                                            'created_at': datetime_obj,
                                            'body': message['body'],
                                            'processed_body': clean_message,
                                            'sentiment': 'Bullish'})

                elif (message['entities']['sentiment']['basic'] == 'Bearish'):
                    bearish_collection.insert({'id': message['id'],
                                               'user_username': message['user']['username'],
                                               'user_id': message['user']['id'],
                                               'created_at': datetime_obj,
                                               'body': message['body'],
                                               'processed_body': clean_message})
                    msgs_collection.insert({'id': message['id'],
                                            'user_username': message['user']['username'],
                                            'user_id': message['user']['id'],
                                            'created_at': datetime_obj,
                                            'body': message['body'],
                                            'processed_body': clean_message,
                                            'sentiment': 'Bearish'})
            else:
                msgs_collection.insert({'id': message['id'],
                                        'user_username': message['user']['username'],
                                        'user_id': message['user']['id'],
                                        'created_at': datetime_obj,
                                        'body': message['body'],
                                        'processed_body': clean_message,
                                        'sentiment': 'None'})


            message_count += 1;
        # print("{} Messages added to database...".format(message_count));
        # time.sleep(3)
    print("Threshold of messages satisfied")

if __name__ == "__main__": main()
