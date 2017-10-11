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
        idx = (df.simple_label_next != 'Legislator') | (df.pid == df.pid_next)
        df = df[idx]
        prev_size = size
        size = len(df.index)

    pass


def get_utterances(cnxn):
    """Gets the utterance data and returns the dataframe"""
    # Hearings in December are actually the next legislative year and PersonClassifications is
    # too stupid to know that
    q = '''SELECT DISTINCT u.*, 
        IF(month(h.date) != 12, 
            year(h.date),
            year(h.date) + 1) AS specific_year,
          IF(s.pid is null, 'Not In Committee', s.position) as committee_position, 
          c.name as committee_name
          FROM currentUtterance u 
              JOIN Video v 
                  ON u.vid = v.vid
              JOIN Hearing h 
                  ON v.hid = h.hid
              JOIN CommitteeHearings ch 
                ON ch.hid = h.hid
              LEFT JOIN servesOn s 
                ON s.cid = ch.cid
                  AND s.pid = u.pid
              JOIN Committee c 
                ON c.cid = ch.cid
          WHERE h.state = "CA" '''
    data = pd.read_sql(q, cnxn)

    return data


def get_classifications(cnxn):
    """Gets the classification info from the database and returns the dataframe"""
    q = '''select pid, PersonType, specific_year, session_year, state
         from PersonClassifications'''
    classifications_df = pd.read_sql(q, cnxn)

    d = {
          'Former Legislator': 'Legislator'
          # 'General Public': 'Witness',
          # 'Legislative Analyst Office': 'Witness',
          # 'Legislative Staff': 'Staff',
          # 'Legislator': 'Legislator',
          # 'Lobbyist': 'Witness',
          # 'State Agency Representative': 'Witness',
          # 'State Constitutional Office': 'Witness',
          # 'Unlabeled': 'Unlabeled'
        }
    classifications_df['simple_label'] = classifications_df.PersonType.map(d)

    # Does not take into consideration whether one type of label should trump another
    classifications_df.drop_duplicates(subset=['pid', 'specific_year'], inplace=True)

    return classifications_df


def structure_utterances(data, classifications_df):
    """Returns final dataframe of structured uttterances"""

    # Relabel null text to blank string
    data.loc[pd.isnull(data.text), 'text'] = ''

    # As multiple committees can appear in the same hearing, we can't be certain who the chair is. This
    # handles this by labeling a member a 'possible chair' if they are a committee chair in one of the
    # committees
    s = data.groupby('uid').apply(len)
    s = s[s > 1]

    idx = (data.uid.isin(s.index)) & (data.committee_position != 'Chair')
    data.loc[idx, 'committee_position'] = 'Possible Chair'

    # Drops committee chair as utterances are usually procedural. Creates succession issues but this
    # is handled later
    data = data[data.committee_position != 'Chair']

    data = data.drop_duplicates(subset=['uid', 'committee_position'])

    df_lst = []
    for g, g_df in data.groupby('vid'):

        # Ordering utterances by time is probably the best way to determine succession
        # For some reason ordering by uid gives me funny behavior
        g_df.sort_values('time', inplace=True)

        # want to know the pid and the text of the succeeding utterance
        g_df['idx1'] = list(range(len(g_df)))
        g_df['idx2'] = g_df.idx1 + 1

        g_df = g_df.merge(classifications_df, how='left', on=['pid', 'specific_year'], suffixes=['', '_'])

        g_df.loc[pd.isnull(g_df['PersonType']), 'simple_label'] = 'Unlabeled'

        g_df_legs = g_df[g_df.PersonType == 'Legislator']

        df = g_df_legs.merge(g_df, left_on='idx2', right_on='idx1', suffixes=['', '_next'])

        # If more than 5 seconds passsed, we're going to call that not successive
        df = df[df.endTime > df.time_next - 5]

        # limit yourself only to possible options
        idx = (df.simple_label_next != 'Legislator') | (df.pid == df.pid_next)
        df = df[idx]

        # Ensures that strings of utterances by the same legislator are combined
        first = True
        out_lst = []
        for idx, row in df.iterrows():
            if first or row['pid'] != prev_pid:
                # get rid of the series of utterances and start over
                uids = set()
                full_text = []

            uids.add(row['uid'])
            full_text.append(row['text'])

            if row['simple_label_next'] != 'Legislator':
                # Add the series of utterances
                d = {'uids': uids,
                     'pid': row['pid'],
                     'text': ', '.join(full_text),
                     'pid_next': row['pid_next'],
                     'text_next': row['text_next'],
                     'simple_label_next': row['simple_label_next']}
                out_lst.append(d)

                # reset utterances and text
                uids = set()
                full_text = []

            prev_pid = row['pid']
            first = False

        # Adding to list to be concatenated later
        df = pd.DataFrame(out_lst)
        df['vid'] = g
        df_lst.append(df)

    data = pd.concat(df_lst)

    return data



def main():
    cnxn = pymysql.connect(**CONN_INFO)

    data = get_utterances(cnxn)
    pickle.dump(data, open('raw_utterances.p', 'wb'))

    classifications_df = get_classifications(cnxn)

    data = structure_utterances(data, classifications_df)
    pickle.dump(data, open('refined_utterances.p', 'wb'))

    cnxn.close()

if __name__ == '__main__':
    main()
