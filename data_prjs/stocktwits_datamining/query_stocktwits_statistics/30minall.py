import pymongo
import datetime
import urllib
import time
import json

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

def obj_datetime_to_str_datetime(datetime_obj):
    date_str = str(datetime.date.year) + '-' + str(datetime.date.month) + '-' + str(datetime.date.day)
    time_str = str(datetime.time.hour) + ':' + str(datetime.time.minute) + ':' + str(datetime.time.second)
    datetime_str = date_str + 'T' + time_str + 'Z'
    return datetime_str

def connect_to_mongodb(db_url, db_name, db_collection):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name][db_collection]

def main():
    msgs_collection = connect_to_mongodb(db_address, 'stocktwits', 'suggested_msg')
    last_time = datetime.datetime(2017,12,31,23,59,59)
    first_time = last_time - datetime.timedelta(minutes=30)
    result = msgs_collection.find().sort("created_at", pymongo.ASCENDING).limit(1);
    for r in result:
        last_time = r['created_at']
    result = msgs_collection.find().sort("created_at", pymongo.DESCENDING).limit(1);
    for r in result:
        first_time = r['created_at']

    print("first record time : {}".format(first_time))
    print("last record time : {}".format(last_time))
    min_msg_count = 1000000000000
    max_msg_count = 0
    mean_msg_count = 0
    all_msg_count = 0
    loop_count = 0
    while last_time < first_time :
        result_curser = msgs_collection.find({'created_at': {'$lt': first_time, '$gte': first_time - datetime.timedelta(minutes=30)}})
        count = result_curser.count();
        if count is 0:
            result = msgs_collection.find({'created_at': {'$lt': first_time}})
            if result.count() is 0:
                break;
        all_msg_count += count;
        if min_msg_count > count:
            min_msg_count = count
        if max_msg_count < count:
            max_msg_count = count
        first_time = first_time - datetime.timedelta(minutes=30)
        print("Count = {}".format(count))
        loop_count += 1;
    mean_msg_count = all_msg_count/loop_count
    print("Mean = {}".format(mean_msg_count))
    print("Min = {}".format(min_msg_count))
    print("Max = {}".format(max_msg_count))
    print("Number of all messages = {}".format(all_msg_count))
if __name__ == "__main__": main()
