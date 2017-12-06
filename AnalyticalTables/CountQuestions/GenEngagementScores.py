"""
File: GetEngagementScores.py
Author: Andrew Voorhees

Description:
    - Creates the tables:
        EngagementScoresHid_analyt
        EngagementScores_analyt

This script does the actual work of using the classifier to label all candidate interactions as engagement or not
and uses those labels to count engagements for legislators. Note that the classifier is retrained within this script
because pickling classifiers and switching architectures can cause problems. This means that the script depends on the
labeled data being available to it.
"""

import pandas as pd
import pickle
import os
import pymysql

from ClassifierFunctions import generate_final_classifier
from ExploratoryFunctions import run_preprocess_nb_code
from conn_info import CONN_INFO

# file path to the training data
train_file = 'merged_labeled_data.csv'
# Path to the processed utterances
data_file = 'refined_utterances.csv'


def get_predictions():
    """Returns a dataframe with the predictions for each utterance as the rows"""
    model = generate_final_classifier(train_file)

    data = pd.read_csv(data_file, encoding='latin1')
    # don't need the second return eleemnt
    data, _ = run_preprocess_nb_code(data)

    data['class'] = model.predict(data)
    data['class'] = data['class'].apply(lambda x: 'procedural' if x == 0 else 'engagement')

    # Actually, I just want engagements
    cols = ['pid', 'did']
    data = data.loc[data['class'] == 'engagement', cols]

    return data


def add_hid(data, cnxn):
    """Adds hearing id and session year info to the dataframe and returns it"""
    q = """SELECT h.hid, bd.did, h.session_year 
           FROM Hearing h
               JOIN BillDiscussion bd 
               ON h.hid = bd.hid
        """
    hearings = pd.read_sql(q, cnxn)

    data = data.merge(hearings, on='did')

    return data


def main():
    cnxn = pymysql.connect(**CONN_INFO)

    data = get_predictions()
    data = add_hid(data, cnxn)

    groups = data.groupby(['pid', 'hid', 'session_year'])
    eng_hid_df = groups.apply(len).reset_index().rename(columns={0: 'engagements'})

    groups = data.groupby(['pid', 'session_year'])
    eng_df = groups.apply(len).reset_index().rename(columns={0: 'engagements'})

    eng_hid_df.to_sql('EngagementScoresHid_analyt', cnxn, if_exists='replace', index=False, flavor='mysql')
    eng_df.to_sql('EngagementScores_analyt', cnxn, if_exists='replace', index=False, flavor='mysql')

    cnxn.close()


if __name__ == '__main__':
    main()
