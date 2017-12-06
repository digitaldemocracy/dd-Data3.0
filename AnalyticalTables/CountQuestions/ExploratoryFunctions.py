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
    elif row.topic_intro:
        out = 'topic_intro'
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


def run_preprocess_nb_code():
    """Just all the code found in the preprocessing notebook necessary to generate the final data"""
    data = pd.read_csv('merged_labeled_data.csv')
    data.uids = data.uids.apply(eval)

    data['label'] = data.apply(lambda row: get_classes(row), axis=1)

    # A nested dictionary containing lists of all the features columns names in the "data" dataframe
    features_dict = {}

    data['word_count'] = data.text.apply(lambda t: len(word_tokenize(t)))
    data['prev_word_count'] = data.text_prev.apply(lambda t: len(word_tokenize(t)))
    data['next_word_count'] = data.text_next.apply(lambda t: len(word_tokenize(t)))

    data['uid_count'] = data.uids.apply(len)
    data['?_count'] = data.text.apply(lambda t: t.count('?'))
    # Ratio of question marks to total words
    data['?_ratio'] = data['?_count'] / data.word_count

    features_dict['eng_features'] = eng_features

    # objects needed for text processing
    stop_words = set(stopwords.words('english'))
    stemmer = SnowballStemmer("english")
    translator = str.maketrans(dict.fromkeys(string.punctuation))

    # creates processed versions of the three texts
    text_features = ['text', 'text_next', 'text_prev']
    features_dict['text_feaures'] = text_features

    processed_text_features = []
    for feat in text_features:
        feat = 'processed_' + feat
        processed_text_features.append(feat)
        data[feat] = data.text.apply(process_text,
                                     stemmer=stemmer,
                                     translator=translator,
                                     stop_words=stop_words)

    features_dict['processed_text_features'] = processed_text_features

    q_text_features = []
    for feat in text_features:
        feat = 'q_' + feat
        q_text_features.append(feat)
        data[feat] = data.text.apply(only_q_words, interrogative_words=interrogative_words)

    features_dict['q_text_features'] = q_text_features

    tfidf_features = {}
    for feat in text_features:
        feat = 'processed_' + feat
        data, tfidf_score_cols = vectorize_text(TfidfVectorizer(),
                                                feat,
                                                data,
                                                'tfidf_score_' + feat)
        tfidf_features[feat] = tfidf_score_cols

    features_dict['tfidf_features'] = tfidf_features

    q_tfidf_features = {}
    for feat in text_features:
        feat = 'q_' + feat
        data, tfidf_score_cols = vectorize_text(TfidfVectorizer(),
                                                feat,
                                                data,
                                                'tfidf_score_' + feat)
        q_tfidf_features[feat] = tfidf_score_cols

    features_dict['q_tfidf_features'] = q_tfidf_features

    # bigrams
    bi_tfidf_features = {}
    for feat in text_features:
        data, tfidf_score_cols = vectorize_text(TfidfVectorizer(ngram_range=(2, 2)),
                                                feat,
                                                data,
                                                'tfidf_score_' + feat)
        bi_tfidf_features[feat] = tfidf_score_cols

    features_dict['bi_tfidf_features'] = bi_tfidf_features

    # trigrams
    tri_tfidf_features = {}
    for feat in text_features:
        data, tfidf_score_cols = vectorize_text(TfidfVectorizer(ngram_range=(2, 2)),
                                                feat,
                                                data,
                                                'tfidf_score_' + feat)
        tri_tfidf_features[feat] = tfidf_score_cols

    features_dict['tri_tfidf_features'] = tri_tfidf_features

    # quad-grams
    quad_tfidf_features = {}
    for feat in text_features:
        data, tfidf_score_cols = vectorize_text(TfidfVectorizer(ngram_range=(2, 2)),
                                                feat,
                                                data,
                                                'tfidf_score_' + feat)
        quad_tfidf_features[feat] = tfidf_score_cols

    features_dict['quad_tfidf_features'] = quad_tfidf_features

    return data, features_dict