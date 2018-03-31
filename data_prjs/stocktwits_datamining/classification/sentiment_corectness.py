import pymongo
import urllib
import csv
import random
username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

db_address = 'mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin'

def connect_to_mongodb (db_url, db_name):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name]

def read_lexicon(csv_path) :
    lexicon = {}
    with open(csv_path, newline='') as csvfile:
        spamreader = csv.reader(csvfile)
        index = 0;
        for row in spamreader:
            index += 1
            if index is 1 :
                continue
            words = row[0].split(';')
            lexicon[words[0]] = words[1]
    return lexicon

def main():
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    sw_collection  = db_obj['sw_collection']
    table5  = db_obj['table5']
    bullish_collection  = db_obj['bullish_msg']
    bearish_collection = db_obj['bearish_msg']
    result = sw_collection.find()
    lexicon_L2 = read_lexicon("l2_lexicon.csv")
    L1_term_list = []
    calculated_SW = {}
    L1_SW = {}

    for message in result :
        L1_term_list.append(message['term'])
        calculated_SW[message['term']] = message['calculated_SW']
        L1_SW[message['term']] = message['L1_SW']

    bullish_correct_0 = 0
    bullish_classified_0 = 0
    bullish_correct_L2 = 0
    bullish_classified_L2 = 0
    bullish_correct_L1 = 0
    bullish_classified_L1 = 0

    count_bullish = bullish_collection.count()
    count = bullish_collection.count()
    for i in range(76000):
        r = bullish_collection.find()[random.randrange(count)]
        message = r['processed_body']
        message = ' ' + message + ' '
        sw_0 = 0
        sw_L1 = 0
        sw_L2 = 0
        for term in L1_term_list:
            term_ = term
            term = ' ' + term + ' '
            if term in message:
                sw_0 += calculated_SW[term_]
                sw_L1 += float(L1_SW[term_])
        for l2_term in lexicon_L2:
            term_ = l2_term
            l2_term = ' ' + l2_term + ' '
            if l2_term in message:
                if(lexicon_L2[term_] == 'positive') :
                    sw_L2 += 1
                if(lexicon_L2[term_] == 'negative') :
                    sw_L2 -= 1
        if(sw_0 > 0) :
            bullish_correct_0 += 1
        if(sw_0 != 0) :
            bullish_classified_0 += 1
        if(sw_L1 > 0) :
            bullish_correct_L1 += 1
        if (sw_L1 != 0):
            bullish_classified_L1 += 1
        if(sw_L2 > 0) :
            bullish_correct_L2 += 1
        if (sw_L2 != 0):
            bullish_classified_L2 += 1

    table5.insert({'bullish_correct_0': bullish_correct_0, 'bullish_classified_0': bullish_classified_0, 'bullish_correct_L1': bullish_correct_L1, 'bullish_classified_L1': bullish_classified_L1 , 'bullish_correct_L2': bullish_correct_L2, 'bullish_classified_L2': bullish_classified_L2 ,
                   'bullish_correct_percentage': bullish_correct_0/count_bullish, 'bullish_correct_L1_percentage': bullish_correct_L1/count_bullish,
                   'bullish_claasified_percentage': bullish_classified_0 / count_bullish, 'bullish_classified_L1_percentage': bullish_classified_L1 / count_bullish})

    bearish_correct_0 = 0
    bearish_classified_0 = 0
    bearish_correct_L2 = 0
    bearish_classified_L2 = 0
    bearish_correct_L1 = 0
    bearish_classified_L1 = 0

    result_bearish = bearish_collection.find()
    count_bearish = bearish_collection.count()
    for r in result_bearish :
        message = r['processed_body']
        message = ' ' + message + ' '
        sw_0 = 0
        sw_L1 = 0
        sw_L2 = 0
        for term in L1_term_list:
            term_ = term
            term = ' ' + term + ' '
            if term in message:
                sw_0 += calculated_SW[term_]
                sw_L1 += float(L1_SW[term_])
        for l2_term in lexicon_L2:
            term_ = l2_term
            l2_term = ' ' + l2_term + ' '
            if l2_term in message:
                if(lexicon_L2[term_] == 'positive') :
                    sw_L2 += 1
                if(lexicon_L2[term_] == 'negetive') :
                    sw_L2 -= 1
        if(sw_0 < 0) :
            bearish_correct_0 += 1
        if(sw_0 != 0) :
            bearish_classified_0 += 1
        if(sw_L1 < 0) :
            bearish_correct_L1 += 1
        if (sw_L1 != 0):
            bearish_classified_L1 += 1
        if(sw_L2 < 0) :
            bearish_correct_L2 += 1
        if (sw_L2 != 0):
            bearish_classified_L2 += 1

    table5.insert({'bearish_correct_0': bearish_correct_0, 'bearish_classified_0': bearish_classified_0, 'bearish_correct_L1': bearish_correct_L1, 'bearish_classified_L1': bearish_classified_L1 , 'bearish_correct_L2': bearish_correct_L2, 'bearish_classified_L2': bearish_classified_L2 ,
                   'bearish_correct_percentage': bearish_correct_0/count_bearish, 'bearish_correct_L1_percentage': bearish_correct_L1/count_bearish,
                   'bearish_claasified_percentage': bearish_classified_0 / count_bearish, 'bearish_classified_L1_percentage': bearish_classified_L1 / count_bearish})


if __name__ == "__main__": main()
