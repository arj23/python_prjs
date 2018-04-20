from __future__ import print_function
from collections import defaultdict
from nltk.classify.util import apply_features, accuracy as eval_accuracy
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import (BigramAssocMeasures, precision as eval_precision,
recall as eval_recall, f_measure as eval_f_measure)
from nltk.probability import FreqDist
from nltk.sentiment.util import save_file, timer
class SentimentAnalyzer(object):

    def __init__(self, classifier=None):
        self.feat_extractors = defaultdict(list)
        self.classifier = classifier

    def all_words(self, documents, labeled=None):
        all_words = []
        if labeled is None:
            labeled = documents and isinstance(documents[0], tuple)
        if labeled == True:
            for words, sentiment in documents:
                all_words.extend(words)
        elif labeled == False:
            for words in documents:
                all_words.extend(words)
        return all_words

