import pymongo
import datetime
import requests
import json
import urllib
username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

db_address = 'mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin'

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


def connect_to_mongodb(db_url, db_name, db_collection):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name][db_collection]


def get_messages_from_stocktwits(last_message_id=None):
    api_url = ''
    if last_message_id is None:
        api_url = 'https://api.stocktwits.com/api/2/streams/suggested.json?filter=top&max'
    else:
        api_url = 'https://api.stocktwits.com/api/2/streams/suggested.json?filter=top&max={}'.format(last_message_id)
    req = requests.get(api_url)
    data = json.loads(req.text)
    return data.get('messages')


def main():
    max_count = 30
    print(max_count)
    username = 'admin'
    password = urllib.parse.quote_plus('abc!@#QWE')

    msgs_collection = connect_to_mongodb('mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin', 'stocktwits', 'suggested_msg')
    message_count = 0
    low_id = 0;
    while (message_count < max_count):
        result = msgs_collection.find().sort("id", pymongo.ASCENDING).limit(1);
        for r in result:
            low_id = r['id']
        if low_id is 0:
            print("there is no message in db to get lowest id")
            return
        messages = get_messages_from_stocktwits(low_id)
        if messages is None or len(messages) == 0:
            print('Receive no message from request, trying again...')
            continue
        for message in messages:
            date_str = message['created_at'];
            message['created_at'] = str_datetime_to_obj_datetime(date_str)
            print(str(str_datetime_to_obj_datetime(date_str)))
            msgs_collection.insert(message)
            message_count += 1;
        print("{} Messages added to database...".format(message_count));
    print("Threshold of messages satisfied")


if __name__ == "__main__": main()
