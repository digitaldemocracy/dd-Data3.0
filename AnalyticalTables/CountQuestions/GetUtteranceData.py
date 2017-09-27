import numpy as np
import pandas as pd
import os
import pymysql
import pickle


CONN_INFO = {
             'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'DDDB2016Aug',
             'user': 'dbMaster',
             'passwd': os.environ['DBMASTERPASSWORD']
             }


def combine_leg_utterances(df):
    """Logic saved for later just in case"""
    # Now we should have a dataframe of all utterances and their successive utterance

    # Next we're going to narrow it down to strings of leg utterances followed by non-leg
    # I know this looks stupid, but I actually think that subsetting repeatedly is the fastest way
    size = len(df.index)
    # one bigger just to keep the loop logic simple
    prev_size = size + 1

    # Holy shit, I never use while loops in Python
    while prev_size > size:
        idx = (df.PersonType_next == 'Other') | (df.pid == df.pid_next)
        df = df[idx]
        prev_size = size
        size = len(df.index)

    pass


def get_utterances(cnxn):
    """Gets the utterance data and returns the dataframe"""
    # Hearings in December are actually the next legislative year and PersonClassifications is
    # too stupid to know that
    q = '''SELECT u.*, 
        IF(month(h.date) != 12, 
            year(h.date),
            year(h.date) + 1) AS specific_year
          FROM currentUtterance u 
              JOIN Video v 
                  ON u.vid = v.vid
              JOIN Hearing h 
                  ON v.hid = h.hid
          WHERE h.state = "CA" '''
    data = pd.read_sql(q, cnxn)

    return data


def get_classifications(cnxn):
    """Gets the classification info from the database and returns the dataframe"""
    q = '''select pid, PersonType, specific_year, session_year, state
         from PersonClassifications'''
    classifications_df = pd.read_sql(q, cnxn)

    # split for readability
    f = lambda t: 'Legislator' if 'Legislator' in t else 'Other'
    d = {t: f(t) for t in classifications_df.PersonType.unique()}

    classifications_df['PersonType'] = classifications_df.PersonType.map(d)

    return classifications_df


def structure_utterances(data, classifications_df):
    """Returns final dataframe of structured uttterances"""
    cols = ['pid',
            'text',
            'pid_next',
            'text_next']

    # We want to worry only about one video at a time
    df_lst = []
    for g, g_df in data.groupby('vid'):
        # Ordering utterances by time is probably the best way to determine succession
        # For some reason ordering by uid gives me funny behavior
        g_df.sort_values('time', inplace=True)

        # want to know the pid and the text of the succeeding utterance
        g_df['idx1'] = list(range(len(g_df)))
        g_df['idx2'] = g_df.idx1 + 1

        g_df = g_df.merge(classifications_df, how='left', on=['pid', 'specific_year'], suffixes=['', '_'])
        g_df.loc[pd.isnull(g_df['PersonType']), 'PersonType'] = 'Other'

        # subsetting for a faster join
        g_df_legs = g_df[g_df.PersonType == 'Legislator']
        df = g_df_legs.merge(g_df, left_on='idx2', right_on='idx1', suffixes=['', '_next'])

        # If more than 10 seconds passsed, we're going to call that not successive
        df = df[df.endTime > df.time_next - 10]

        # Final subset, only utterances from legs followed by non-legs
        idx = (df.PersonType == 'Legislator') & (df.PersonType_next == 'Other')
        df = df[idx]

        df_lst.append(df[cols])

    data = pd.concat(df_lst)

    pickle.dump(data, open('data.p', 'wb'))
    return data



def main():
    cnxn = pymysql.connect(**CONN_INFO)

    data = get_utterances(cnxn)
    classifications_df = get_classifications(cnxn)

    data = structure_utterances(data, classifications_df)
    pickle.dump(data, open('final_data.p', 'wb'))

    cnxn.close()

if __name__ == '__main__':
    main()
