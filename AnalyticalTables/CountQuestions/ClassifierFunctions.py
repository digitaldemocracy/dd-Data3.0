"""
Script: ClassifierFunctions.py
Author: Andrew Voorhees

Contains functions and imports used by the classifier notebooks. The final function is used by GenEngagementScores.py
for counting engagements.
"""

import pandas as pd
import numpy as np
import pickle

# Used for assessing model
from sklearn.model_selection import cross_validate
# Used for K-fold cross validation
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.model_selection import StratifiedKFold
from sklearn.model_selection import train_test_split
# For tuning hyperparameters
from sklearn.model_selection import GridSearchCV

# Evaluation metrics
from sklearn.metrics import confusion_matrix
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.metrics import accuracy_score

# classifiers
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
from sklearn.naive_bayes import MultinomialNB

# Used to create my voting classifier
from sklearn.base import BaseEstimator

# Transforms series of strings into a bag of word matrix and finds Tfidf scores
from sklearn.feature_extraction.text import TfidfVectorizer

from sklearn.pipeline import Pipeline

# Allows me to "pretty print" dictionaries when assessing pipelines
from pprint import pprint

# Used for the heat map
import seaborn as sns
import matplotlib.pyplot as plt

# Used for saving the model at the end
from sklearn.externals import joblib

from ExploratoryFunctions import run_preprocess_nb_code

# Set so it's easy to replicate
RANDOM_STATE = 42


class MyVotingClassifier(BaseEstimator):
    """A voting classifier which takes a weighted average of the predicted proabilities to make a prediction. Implemented
       so that each classifier can take unique features, something I couldn't workout how to do in sklearn.

       Attributes:
           classifiers: A list of tuples with the estimators as the first element and the columns used by the estimator as
                        the second element.
           weights: A list which assigns weights for each classifier when averaging the prediction probabilities
    """

    def __init__(self, classifiers=None, weights=None):
        self.classifiers = classifiers
        self.weights = weights

    def fit(self, X, y=None):
        if self.classifiers is not None:
            for clf, cols in self.classifiers:
                # select just the columns we want
                X_sub = X[cols]
                clf.fit(X_sub, y)
        else:
            pass

        return self

    def predict(self, X):

        # I don't know why, but this needs to added here. You can't seem to set weights in __init__ and
        # use GridSearchCV
        if self.weights is None:
            self.weights = [1 / len(self.classifiers) for i in range(len(self.classifiers))]

        if self.classifiers is not None:
            probabilities = []
            for clf, cols in self.classifiers:
                X_sub = X[cols]
                prob = [pair[1] for pair in clf.predict_proba(X_sub)]
                probabilities.append(prob)

            probabilities = pd.DataFrame(probabilities).T
            predicted = probabilities.apply(lambda row: np.round((row * self.weights).sum()), axis=1)
        else:
            # always just predict 0
            predicted = [0 for i in range(len(X))]

        return predicted


def model_metrics(X, y, pipeline):
    """Prints out scoring metrics from cross-validation"""
    scoring = {'acr': 'accuracy',
               'prec': 'precision',
               'rec': 'recall',
               'f1': 'f1'}

    scores = cross_validate(pipeline, X, y,
        scoring=scoring,
        cv=RepeatedStratifiedKFold(n_splits=3, random_state = RANDOM_STATE))

    acr = scores['test_acr']
    prec = scores['test_prec']
    rec = scores['test_rec']
    f1 = scores['test_f1']

    print('SCORES:\n')
    print('Average Accuracy:', acr.mean())
    print('Average Precision: ', prec.mean())
    print('Average Recall:', rec.mean())
    print('Average F1: ', f1.mean())


def show_confusion_matrix(X, y, model):
    """Plots the confusion matrix for the model as a heatmap"""
    N = 10

    tot_mat = np.array([[0, 0],
                        [0, 0]])
    # Repeat N times for more consistent results
    for i in range(N):
        # Making the confusion matrix
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.3, random_state=RANDOM_STATE)

        model.fit(X_train, y_train)
        predicted = model.predict(X_test)
        tot_mat += confusion_matrix(y_test, predicted)

    sns.heatmap(tot_mat.T, square=True, annot=True, fmt='d', cbar=False,
     xticklabels=('Procedural', 'Engagement'), yticklabels=('Procedural', 'Engagement'))
    plt.xlabel('true label')
    plt.ylabel('predicted label')


def print_model_info(grid):
    """Prints out the parameters for a pipeline"""
    print('Pipeline Parameters:')
    pprint(grid.best_params_)
    print()


def evaluate_model(model, param_grid, X, y, optimize='f1'):
    """Gives a full evaluation of a given model"""
    grid = GridSearchCV(model, param_grid, scoring=optimize,
                        cv=RepeatedStratifiedKFold(n_splits=3, random_state=RANDOM_STATE))
    grid.fit(X, y);
    model = grid.best_estimator_

    print()
    model_metrics(X, y, model)
    print()

    show_confusion_matrix(X, y, model)

    print_model_info(grid)

    return grid


def get_context_features(features_dict):
    """Helper for generate_final_classifier. Gets the features used by clf_context"""
    model_features = []

    # context features w/o simple label
    top_features = ['committee_position_prev',
                    'committee_position',
                    'committee_position_next',

                    'simple_label_prev',
                    'simple_label_next'
                    ]
    for feat in top_features:
        # adds every feature in the bottom level list
        model_features += features_dict['context_features'][feat]

    extra_features = ['word_count',
                      '?_count']
    model_features += extra_features

    return model_features


def generate_final_classifier(data_file):
    """Trains and returns the final classifier with the optimal hyperparameters hard-coded in. This is done because
       pickling sklearn estimators and then switching btw different architectures can cause problems.

       Arguments:
           data_file: A string with the path to the data file
   """
    data = pd.read_csv(data_file)
    data.uids = data.uids.apply(eval)

    data, features_dict = run_preprocess_nb_code(data, labeled_data=True)

    pipeline = [('vect', TfidfVectorizer(encoding='unicode',
                                         ngram_range=(1,1),
                                         max_df=.5)),
                ('clf', MultinomialNB())]
    clf_text = Pipeline(pipeline)

    clf_context = SVC(probability=True, C=1, gamma=.2)
    context_features = get_context_features(features_dict)

    classifiers = [(clf_text, 'text'),
                   (clf_context, context_features)]

    model = MyVotingClassifier(classifiers=classifiers, weights=[.9, .1])

    model.fit(data, data.binary_label)

    return model


