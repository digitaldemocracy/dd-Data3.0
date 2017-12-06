import numpy as np
import pandas as pd
import os
import pymysql
import pickle
import re


CONN_INFO = {
             'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'DDDB2016Aug',
             'user': 'dbMaster',
             'passwd': os.environ['DBMASTERPASSWORD']
             }


def get_utterances(cnxn):
    """Queries the db for utterance data."""
    # Hearings in December are actually the next legislative year and PersonClassifications is
    # too stupid to know that
    q = '''SELECT DISTINCT u.*, 
        IF(month(h.date) != 12, 
            year(h.date),
            year(h.date) + 1) AS specific_year,
          IF(s.pid is null, 'Not In Committee', s.position) as committee_position, 
          a.pid is not null as bill_author,
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
              LEFT JOIN BillDiscussion bd 
                ON bd.did = u.did
              LEFT JOIN authors a 
                ON bd.bid = a.bid
                  AND u.pid = a.pid
          WHERE h.state = "CA" '''
    data = pd.read_sql(q, cnxn)

    return data


def get_classifications(cnxn):
    """Gets the classification info from the database and returns the dataframe"""
    q = '''select pid, PersonType, specific_year, session_year, state
         from PersonClassifications'''
    classifications_df = pd.read_sql(q, cnxn)

    d = {
        'Former Legislator': 'Legislator',
        'General Public': 'General Public',
        'Legislative Analyst Office': 'LAO',
        'Legislative Staff': 'Staff',
        'Legislator': 'Legislator',
        'Lobbyist': 'Lobbyist',
        'State Agency Representative': 'State Agency Rep',
        'State Constitutional Office': 'State Const Office',
        'Unlabeled': 'Unlabeled'
    }
    classifications_df['simple_label'] = classifications_df.PersonType.map(d)

    # Does not take into consideration whether one type of label should trump another
    classifications_df.drop_duplicates(subset=['pid', 'specific_year'], inplace=True)

    return classifications_df


def add_simple_labels(df, classifications_df):
    """Adds simplified class labels for the speaker of a given utterances"""
    df = df.merge(classifications_df, how='left', on=['pid', 'specific_year'],
                  suffixes=['', '_duplicate'])

    keep_cols = [col for col in df.columns if '_duplicate' not in col]
    df = df[keep_cols]

    df.loc[pd.isnull(df['PersonType']), 'simple_label'] = 'Unlabeled'
    df.loc[df.bill_author, 'simple_label'] = 'Bill Author'

    return df


def check_position(g_df):
    "Checks if the dataframe cotains a Chair, Vice-Chair, or Member. Meant to be used with an apply"
    size_cond = g_df.committee_position.nunique() > 1

    if not size_cond:
        return pd.Series({'chair_flag': False,
                          'vice_flag': False,
                          'member_flag': False
                          })
    else:
        chair_cond = (g_df.committee_position == 'Chair').sum() > 0
        vice_cond = (g_df.committee_position == 'Vice-Chair').sum() > 0
        co_cond = (g_df.committee_position == 'Co-Chair').sum() > 0
        mem_cond = (g_df.committee_position == 'Member').sum() > 0

        return pd.Series({'chair_flag': chair_cond | co_cond,
                          'vice_flag': vice_cond,
                          'member_flag': mem_cond})


def fix_multiple_positions(data):
    """The join with CommitteeHearings results in doubling up of uids, as multiple committees can appear in the same
       hearing. We can't be certain who the chair is. This function handles this by labeling a member a 'possible chair'
       (or vice-chair) if they are a committee chair in one of the committees. It also overrides non-member
       classifications as members if they appear in one committee and not the other. Finally it dedupes the dataframe
       so uid can act as a key.

       Arguments:
           data: Dataframe containing all the utterances and committee labels

        Returns:
            The deduped dataframe with clean committee position labels
    """
    groups = data.groupby('uid')

    is_pos_df = groups.apply(check_position)

    chair_uids = is_pos_df[is_pos_df.chair_flag].index
    vice_uids = is_pos_df[is_pos_df.vice_flag].index
    mem_uids = is_pos_df[is_pos_df.member_flag].index

    chair_idx = data.uid.isin(chair_uids)
    vice_idx = data.uid.isin(vice_uids)
    mem_idx = data.uid.isin(mem_uids)

    # Member if going to overridde non-member
    data.loc[mem_idx, 'committee_position'] = 'Member'
    data.loc[vice_idx, 'committee_position'] = 'Possible Vice-Chair'
    data.loc[chair_idx, 'committee_position'] = 'Possible Chair'

    data = data.drop_duplicates(subset=['uid', 'committee_position'])

    return data


def process_utterances(data, classifications_df):
    """Cleans the raw utterance data and merges in classification info.
        Args:
            data: The raw data read from the db or a file
            classifications_df: Dataframe containing the classifications for each pid

        Returns:
            The cleaned dataframe joined with classifications
    """
    # Changes 0 and 1 values to Python booleans
    data['bill_author'] = data.bill_author.apply(lambda x: False if x == 0 else True)
    # Relabel null text to blank string
    data.loc[pd.isnull(data.text), 'text'] = ' '

    # Replaces PersonType with simplified labels
    data = add_simple_labels(data, classifications_df)

    # Handles doubling up from join with CommitteeHearings
    data = fix_multiple_positions(data)

    return data


def load_data(cnxn=None, utr_file=None, clf_file=None):
    """Loads and processes the data either from a the db or from a file.

        Args:
            cnxn: The connection to the db
            utr_file: File pointer to the utterance data
            clf_file: File pointer to the person classification data

        Returns:
            A dataframe containing the cleaned and processed data

    """
    # TODO circle back and add a connection assertion

    data = get_utterances(cnxn)
    classifications_df = get_classifications(cnxn)
    pickle.dump(data, open('raw_utterances.p', 'wb'))
    pickle.dump(classifications_df, open('classifications_df.p', 'wb'))

    data = process_utterances(data, classifications_df)

    return data


def subset_utterances(g_df):
    """Sorts and subsets utterance dataframe to include only instances of a legislator interacting
       with a witness of bill author. Intended to be run only on utterances of the same video.

       Arguments:
           g_df: Dataframe containing utterances from the same video

        Returns:
            The processed dataframe
    """
    # Ordering utterances by time is probably the best way to determine succession
    # For some reason ordering by uid gives me funny behavior
    g_df.sort_values('time', inplace=True)

    # want to know the pid and the text of the preceding and succeeding utterances
    g_df['idx_cur'] = [x + 1 for x in range(len(g_df))]  # Not zero indexed
    g_df['idx_prev'] = g_df.idx_cur - 1
    g_df['idx_next'] = g_df.idx_cur + 1

    g_df_legs = g_df[g_df.simple_label == 'Legislator']

    # I suppose it's worth noting that the very first and very last utterance in a video won't be counted
    # no matter what. The joins just throw them out even if they are legs
    df = g_df_legs.merge(g_df, left_on='idx_next', right_on='idx_cur', suffixes=['', '_next'])
    df = df.merge(g_df, left_on='idx_prev', right_on='idx_cur', suffixes=['', '_prev'])

    # If more than 5 seconds passsed, we're going to call that not successive
    df = df[df.endTime > df.time_next - 5]
    # TODO, I'm not going to explicitly throw out utterances where there is a long pause between
    # the previous and the current. It might be worth doing this building the classifier though

    # I want to make sure the succeeding utterance is either not a legislator or the same person
    idx = (df.simple_label_next != 'Legislator') | (df.pid == df.pid_next)
    df = df[idx]

    return df


def combine_leg_utterances(df):
    """Combines all series of utterances by a single legislator into single blocks of text.
       Returns a dataframe with the legislator's utterance, the previous utterance, and
       the next utterance.

       Arguments:
           df: Dataframe of utterances that have already been subsetted and sorted

        Returns:
            Dataframe with concatenated utterances
    """
    # Ensures that strings of utterances by the same legislator are combined
    first = True
    out_lst = []
    for idx, row in df.iterrows():
        if first or row['pid'] != prev_pid:
            # get rid of the series of utterances and start over
            uids = set()
            full_text = []
            # Want to make sure I retain info for the previous utterance
            first_row = {k: v for k, v in row.items() if re.match(r'([\w_]+)(_prev\Z)', k)}

        uids.add(row['uid'])
        full_text.append(row['text'])

        if row['simple_label_next'] != 'Legislator':
            # Add the series of utterances
            d = {'uids': uids,
                 'uid_prev': first_row['uid_prev'],
                 'uid_next': row['uid_next'],

                 'pid_prev': first_row['pid_prev'],
                 'pid': row['pid'],
                 'pid_next': row['pid_next'],

                 'committee_position_prev': first_row['committee_position_prev'],
                 'committee_position': row['committee_position'], # This should always be legislator
                 'committee_position_next': row['committee_position_next'],

                 'text_prev': first_row['text_prev'],
                 'text': ', '.join(full_text),
                 'text_next': row['text_next'],

                 'simple_label_prev': first_row['simple_label_prev'],
                 'simple_label': row['simple_label'],
                 'simple_label_next': row['simple_label_next'],

                 'did': row['did'],
                 'did_next': row['did_next']
                 }
            out_lst.append(d)

            # reset utterances and text
            uids = set()
            full_text = []

        prev_pid = row['pid']
        first = False

    df = pd.DataFrame(out_lst)
    return df


def structure_utterances(data):
    """Returns final dataframe meant for classification. Rows contain concatenated leg utterances."""

    df_lst = []
    for vid, g_df in data.groupby('vid'):
        print('vid:', vid)
        df = subset_utterances(g_df)
        df = combine_leg_utterances(df)
        df['vid'] = vid

        df_lst.append(df)

    data = pd.concat(df_lst)

    # last minute additions:
    # 2998 is the committee secretary and we want to ignore them
    data = data[data.pid_next != 2998]
    # Want to remove the cases where we switch bill discussions
    data = data[data.did == data.did_next]

    # Drops committee chair as utterances are usually procedural.
    data = data[data.committee_position != 'Chair']

    return data


def main():
    cnxn = pymysql.connect(**CONN_INFO)

    # Grabs data from the db and processes it
    data = load_data(cnxn)

    # Structures data so that it is ready to be labeled
    data = structure_utterances(data)
    data.to_csv('refined_utterances.csv', index=False)

    cnxn.close()

if __name__ == '__main__':
    main()
