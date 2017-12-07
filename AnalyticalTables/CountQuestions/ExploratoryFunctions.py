"""
Script: ExploratoryFunctions.py
Author: Andrew Voorhees

Contains functions used by the notebooks for preprocessing and data exploration. Final function is imported in
both ClassifierFunctions and GenEngagementScores.
"""

import pandas as pd
import numpy as np
import pickle

# Used to break a sentence into individual words
from nltk import word_tokenize
# List of low information words
from nltk.corpus import stopwords
# A popular stemming algorithm
from nltk.stem import SnowballStemmer

# I just use this for finding punctuation
import string

# Transforms series of strings into a bag of word matrix and finds Tfidf scores
from sklearn.feature_extraction.text import TfidfVectorizer

# Used for visualization
import scipy.stats as stats

context_features = ['committee_position_prev',
                    'committee_position',
                    'committee_position_next',

                    'simple_label_prev',
                    'simple_label',
                    'simple_label_next'
                    ]

eng_features = ['word_count',
                'prev_word_count',
                'next_word_count',
                'uid_count',
                '?_count',
                '?_ratio'
               ]

text_features = ['text',
                 'text_next',
                 'text_prev']

# from wikipedia
interrogative_words = set(['which',
                           'what',
                           'whose',
                           'who',
                           'whom',
                           'where',
                           'whither',
                           'whence',
                           'when',
                           'how',
                           'why',
                           'whether'])


def get_classes(row):
    """Combines all text classes into my three core classes: engagement, procedural, topic_info.
       Meant to apply over the dataframe"""
    out = None
    if row.engagement | row.question:
        out = 'engagement'
    elif row.procedural | row.procedural_question:
        out = 'procedural'
    # Every row should have one of these labels
    assert out
    return out


def process_text(text, stemmer=None, translator=None, stop_words=None):
    "Removes stopwords and punctuation. Then stems the word"
    text = text.translate(translator)
    tokens = word_tokenize(text)
    return ' '.join([stemmer.stem(w) for w in tokens if stemmer.stem(w) not in stop_words])


def only_q_words(s, interrogative_words=None):
    """One liner for removing all non-question words from text"""
    return ' '.join([w.lower() for w in word_tokenize(s) if w.lower() in interrogative_words])


def vectorize_text(vect, field, data, prepend):
    "Vectorizes text and appends new fields to data. Returns new dataframe and the names of the columns"
    scores = vect.fit_transform(data[field])
    scores = pd.DataFrame(scores.toarray(), columns=vect.get_feature_names())

    scores_cols = [prepend + c for c in scores.columns]
    scores.columns = scores_cols

    data = pd.concat([data, scores], axis=1)

    return data, scores_cols


def run_preprocess_nb_code(data, labeled_data=False):
    """Just all the code found in the preprocessing notebook necessary to generate the final data. Rather than
       pickling the data and features dictionary at the end, just returns them. """

    if labeled_data:
        data['label'] = data.apply(lambda row: get_classes(row), axis=1)
        data['binary_label'] = [0 if lab == 'procedural' else 1 for lab in data.label]

    # A nested dictionary containing lists of all the features columns names in the "data" dataframe
    features_dict = {}

    # Hot encoding for 'context' features
    d = {}
    features_dict['context_features'] = d
    # context_features is a global
    for feat in context_features:
        dummies = pd.get_dummies(data[feat])
        dummies.columns = [feat + '_' + col for col in dummies]

        d[feat] = list(dummies.columns)
        data = pd.concat([data, dummies], axis=1)

    data['word_count'] = data.text.apply(lambda t: len(word_tokenize(t)))
    data['prev_word_count'] = data.text_prev.apply(lambda t: len(word_tokenize(t)))
    data['next_word_count'] = data.text_next.apply(lambda t: len(word_tokenize(t)))

    data['uid_count'] = data.uids.apply(len)
    data['?_count'] = data.text.apply(lambda t: t.count('?'))
    # Ratio of question marks to total words
    data['?_ratio'] = data['?_count'] / data.word_count

    features_dict['eng_features'] = eng_features

    return data, features_dict