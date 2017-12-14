# Basic Imports
import pickle
import pandas as pd
import numpy as np

# Create features imports
from sklearn.feature_extraction.text import CountVectorizer
from nltk.corpus import stopwords
from nltk import word_tokenize
from nltk.stem import SnowballStemmer
import string
from sklearn.feature_extraction.text import TfidfVectorizer

from nltk.tokenize import sent_tokenize

import nltk

# Sklearn Imports
from sklearn.naive_bayes import MultinomialNB
from sklearn.naive_bayes import GaussianNB
from sklearn.metrics import accuracy_score
from sklearn.cross_validation import train_test_split;
from sklearn.cross_validation import cross_val_score
from sklearn.cross_validation import LeaveOneOut
from sklearn.metrics import confusion_matrix
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import LinearSVC

# Stanford NLP
from pycorenlp import StanfordCoreNLP

def assess_results(model, X, y, data):
    out = {}
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, 
                                                    random_state=0, 
                                                    train_size=.8)
    if isinstance(model, GaussianNB):
        X_train = X_train.toarray()
        X_test = X_test.toarray()
        
    model.fit(X_train, y_train)
    y_pred = model.predict(X_test)    
        
    mat = confusion_matrix(y_test, y_pred, labels=[True, False])
    sns.heatmap(mat.T, square=True, annot=True, fmt='d', cbar=False,
     xticklabels=[True, False], yticklabels=[True, False])
    plt.xlabel('true label')
    plt.ylabel('predicted label');
   

    # True Positive
    idx = (y_test == True) & (y_test == y_pred)
    raw_indices = y_test[idx].index
    out['true_positive'] = data[data.index.isin(raw_indices)]
    
    
    # False Positive
    idx = (y_test == False) & (y_test != y_pred)
    raw_indices = y_test[idx].index
    out['false_positive'] = data[data.index.isin(raw_indices)]
    
    # True Negative
    idx = (y_test == False) & (y_test == y_pred)
    raw_indices = y_test[idx].index
    out['true_negative'] = data[data.index.isin(raw_indices)]
    
    # False Negative
    idx = (y_test == True) & (y_test != y_pred)
    raw_indices = y_test[idx].index
    out['false_negative'] = data[data.index.isin(raw_indices)]
    
    return out 


def preprocess_text(text, stemmer=None, translator=None, stop_words=None):
    "Removes stopwords and punctuation. Then stems the word"
    text = text.translate(translator)
    tokens = word_tokenize(text)
    return ' '.join([stemmer.stem(w) for w in tokens if stemmer.stem(w) not in stop_words])
#     return ' '.join([w for w in tokens if stemmer.stem(w) not in stopwords])


def get_word_freq(processed_text):
    "Returns a series of words with their overall frequency"
    vect = TfidfVectorizer()
    _ = vect.fit(processed_text)

    bag_of_words = vect.transform(processed_text)
    word_counts = np.mean(bag_of_words, axis=0)

    inv_vocabulary = {v: k for k, v in vect.vocabulary_.items()}
    d = {}
    for idx, count in enumerate(np.array(word_counts)[0]):
        word = inv_vocabulary[idx]
        d[word] = count

    return pd.Series(d).sort_values(ascending=False)


def stanford_parse(text, nlp=None, syntactic_targets=None):
    "Breaks the text down into syntactic tags of interest"
    assert nlp is not None
    assert syntactic_targets is not None
    
    sententences = sent_tokenize(text)
    
    all_parts = []
    sentiments = []
    for sentence in sententences:
        output = nlp.annotate(sentence, properties={
          'annotators': 'parse, sentiment',
          'outputFormat': 'json'
        })
        
        if output != 'CoreNLP request timed out. Your document may be too long.':            
            p_tree = output['sentences'][0]['parse']
            sentiments.append(output['sentences'][0]['sentiment'])

            all_parts += [w for w in word_tokenize(p_tree) if w in syntactic_targets]

    parts_str = ' '.join(all_parts)
    
    return parts_str



def print_parse_tree(sent, nlp):
    output = nlp.annotate(sent, properties={
        'annotators': 'parse, sentiment',
        'outputFormat': 'json'
        })
    print(output['sentences'][0]['parse'])
    
    
def vectorize_text(vect, field, data, prepend):
    "Vectorizes text and appends new fields to data. Returns new dataframe and the names of the columns"
    scores = vect.fit_transform(data[field])
    scores = pd.DataFrame(scores.toarray(), columns=vect.get_feature_names())
    
    scores_cols = [prepend + c for c in scores.columns]
    scores.columns = scores_cols
    
    data = pd.concat([data, scores], axis=1)
    
    return data, scores_cols