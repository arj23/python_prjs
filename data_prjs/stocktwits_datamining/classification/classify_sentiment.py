# You need to install scikit-learn:
# sudo pip install scikit-learn
#
# Dataset: Polarity dataset v2.0
# http://www.cs.cornell.edu/people/pabo/movie-review-data/
#
# Full discussion:
# https://marcobonzanini.wordpress.com/2015/01/19/sentiment-analysis-with-python-and-scikit-learn
import pymongo
import urllib
import random
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn import svm
from sklearn.metrics import classification_report
import time

username = 'admin'
password = urllib.parse.quote_plus('abc!@#QWE')

def connect_to_mongodb (db_url, db_name):
    connection = pymongo.MongoClient(db_url)
    return connection[db_name]

db_address = 'mongodb://'+ username +':' + password + '@137.74.100.108/admin?authSource=admin'

if __name__ == '__main__':
    classes = ['pos', 'neg']
    db_obj = connect_to_mongodb(db_address, 'stocktwits')
    bullish_collection_sample  = db_obj['bullish_msg_sample']
    bearish_collection_sample  = db_obj['bearish_msg_sample']
    bullish_collection  = db_obj['bullish_msg']
    bearish_collection  = db_obj['bearish_msg']
    table5  = db_obj['table5']

    # Read the data
    train_data = []
    train_labels = []
    test_data = []
    test_labels = []

    result = bullish_collection_sample.find()
    for m in result :
        train_data.append(m['processed_body'])
        train_labels.append("pos")

    result = bearish_collection_sample.find()
    for m in result :
        train_data.append(m['processed_body'])
        train_labels.append("neg")

    result = bullish_collection.find()
    i = 0
    for m in result :
        test_data.append(m['processed_body'])
        test_labels.append("pos")
        i = i + 1
        if i is 75000 :
            break

    result = bearish_collection.find()
    i = 0
    for m in result :
        test_data.append(m['processed_body'])
        test_labels.append("neg")
        i = i + 1
        if i is 75000 :
            break

    # Create feature vectors
    vectorizer = TfidfVectorizer(min_df=5,
                                 max_df = 0.8,
                                 sublinear_tf=True,
                                 use_idf=True)
    train_vectors = vectorizer.fit_transform(train_data)
    test_vectors = vectorizer.transform(test_data)

    # Perform classification with SVM, kernel=rbf
    classifier_rbf = svm.SVC()
    t0 = time.time()
    classifier_rbf.fit(train_vectors, train_labels)
    t1 = time.time()
    prediction_rbf = classifier_rbf.predict(test_vectors)
    t2 = time.time()
    time_rbf_train = t1-t0
    time_rbf_predict = t2-t1

    # # Perform classification with SVM, kernel=linear
    # classifier_linear = svm.SVC(kernel='linear')
    # t0 = time.time()
    # classifier_linear.fit(train_vectors, train_labels)
    # t1 = time.time()
    # prediction_linear = classifier_linear.predict(test_vectors)
    # t2 = time.time()
    # time_linear_train = t1-t0
    # time_linear_predict = t2-t1
    #
    # # Perform classification with SVM, kernel=linear
    # classifier_liblinear = svm.LinearSVC()
    # t0 = time.time()
    # classifier_liblinear.fit(train_vectors, train_labels)
    # t1 = time.time()
    # prediction_liblinear = classifier_liblinear.predict(test_vectors)
    # t2 = time.time()
    # time_liblinear_train = t1-t0
    # time_liblinear_predict = t2-t1

    table5.insert({'Training_time_for_SVC(kernel=rbf)': time_rbf_train, 'Prediction_time_for_SVC(kernel=rbf)': time_rbf_predict,
                   'Results_for_SVC(kernel=rbf)':classification_report(test_labels, prediction_rbf)})
    # Print results in a nice table
    print("Results for SVC(kernel=rbf)")
    print("Training time: %fs; Prediction time: %fs" % (time_rbf_train, time_rbf_predict))
    print(classification_report(test_labels, prediction_rbf))
    # print("Results for SVC(kernel=linear)")
    # print("Training time: %fs; Prediction time: %fs" % (time_linear_train, time_linear_predict))
    # print(classification_report(test_labels, prediction_linear))
    # print("Results for LinearSVC()")
    # print("Training time: %fs; Prediction time: %fs" % (time_liblinear_train, time_liblinear_predict))
    # print(classification_report(test_labels, prediction_liblinear))
    i = 0

for t in test_data:
    table5.insert({'body' : test_data[i], 'label': test_labels[i], 'c_label': prediction_rbf[i]})
    i = i+1
    # 'Training_time_for_SVC(kernel=linear)': time_linear_train, 'Prediction_time_for_SVC(kernel=linear)': time_linear_predict,
    # 'Results_for_SVC(kernel=linear)': classification_report(test_labels, prediction_linear),
    # 'Training_time_for_LinearSVC()': time_liblinear_train, 'Prediction_time_for_LinearSVC()': time_liblinear_predict,
    # 'Results_for_LinearSVC()': classification_report(test_labels, prediction_liblinear)


