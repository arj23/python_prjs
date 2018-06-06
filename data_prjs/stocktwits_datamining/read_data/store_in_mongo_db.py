import pymongo
import datetime
import sys
import requests
import json
from pandas import DataFrame
import pandas as pd

from collections import OrderedDict


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
    max_count = 20000
    print(max_count)
    messages = get_messages_from_stocktwits()
    if messages is None or len(messages) == 0:
        print('Receive no message from request, parsing finished')
        return
    msgs_collection = connect_to_mongodb('mongodb://137.74.100.108', 'stocktwits', 'suggested_msg')
    message_count = 0
    last_id_from_db = False;
    chech_all_messages = False
    result = msgs_collection.find({"id": messages[-1]['id']})
    if result.count() != 0 :
        chech_all_messages = True;
    for message in messages:
        if chech_all_messages is True :
            result = msgs_collection.find({"id": message['id']})
            if result.count() == 0:
                date_str = message['created_at'];
                message['created_at'] = str_datetime_to_obj_datetime(date_str)
                print(str(str_datetime_to_obj_datetime(date_str))+ 'checked')
                msgs_collection.insert(message)
                message_count += 1;
                last_id_from_db = False;
            else:
                last_id_from_db = True;
        else :
            date_str = message['created_at'];
            message['created_at'] = str_datetime_to_obj_datetime(date_str)
            print(str_datetime_to_obj_datetime(date_str))
            msgs_collection.insert(message)
            message_count += 1;
    print("{} Messages added to database...".format(message_count));
    low_id = 0;
    if (last_id_from_db == True):
        result = msgs_collection.find().sort("id", pymongo.ASCENDING).limit(1);
        for r in result:
            low_id = r['id']
    else:
        low_id = messages[-1]['id']
    calc_low_id = False
    while (message_count < max_count):
        if (calc_low_id == True):
            if (last_id_from_db == True):
                result = msgs_collection.find().sort("id", pymongo.ASCENDING).limit(1);
                for r in result:
                    low_id = r['id']
            else:
                low_id = messages[-1]['id']
        messages = get_messages_from_stocktwits(low_id)
        if messages is None or len(messages) == 0:
            print('Receive no message from request, trying again...')
            last_id_from_db = True;
            continue
        chech_all_messages = False
        result = msgs_collection.find({"id": messages[-1]['id']})
        if result.count() != 0:
            chech_all_messages = True;
        for message in messages:
            if chech_all_messages is True :
                result = msgs_collection.find({"id": message['id']})
                if result.count() == 0:
                    date_str = message['created_at'];
                    message['created_at'] = str_datetime_to_obj_datetime(date_str)
                    print(str(str_datetime_to_obj_datetime(date_str)) + 'checked')
                    msgs_collection.insert(message)
                    message_count += 1;
                    last_id_from_db = False;
                else:
                    last_id_from_db = True;
            else:
                date_str = message['created_at'];
                message['created_at'] = str_datetime_to_obj_datetime(date_str)
                print(str_datetime_to_obj_datetime(date_str))
                msgs_collection.insert(message)
                message_count += 1;
        print("{} Messages added to database...".format(message_count));
        calc_low_id = True
    print("Threshold of messages satisfied")

def test():
    api_url = 'https://api.stocktwits.com/api/2/watchlists/static/symbols/create.json?symbols=AUDJPY&initial_list=false'
    req = requests.post(api_url)
    print(req)
if __name__ == "__main__": test()
