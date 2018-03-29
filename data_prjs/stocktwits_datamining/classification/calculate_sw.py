import pymongo
import urllib
import csv
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


def main() :
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    bullish_collection_sample  = db_obj['bullish_msg_sample']
    bearish_collection_sample  = db_obj['bearish_msg_sample']
    sw_collection  = db_obj['sw_collection']
    lexicon_L1 = read_lexicon("l1_lexicon.csv")
    term_list = []
    n_pos_list = []
    n_neg_list = []
    sw_list = []
    for term in lexicon_L1:
        term_list.append(term)
        term = ' ' + term + ' '
        n_pos = 0
        n_neg = 0
        result = bullish_collection_sample.find()
        for r in result:
            message = r['processed_body']
            message = ' ' + message + ' '
            if term in message:
                n_pos += 1;
                print(n_pos)
        n_pos_list.append(n_pos)
        result = bearish_collection_sample.find()
        for r in result:
            message = r['processed_body']
            message = ' ' + message + ' '
            if term in message:
                n_neg += 1;
        n_neg_list.append(n_neg)
        sw = (n_pos-n_neg)/(n_pos+n_neg)
        sw_list.append(sw)
        sw_collection.insert({'term': term, 'n_total': n_neg+n_pos, 'n_pos': n_pos, 'n_neg': n_neg, 'SW': sw})
        print(term)
    print(len(lexicon_L1))
    lexicon_L2 = read_lexicon("l2_lexicon.csv")
    print(len(lexicon_L2))

if __name__ == "__main__": main()
