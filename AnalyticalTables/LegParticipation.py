"""
File: LegParticipation.py
Author: Andrew Voorhees

Description:
- Creates the tables:
 LegParticipationVerbal
 LegParticipationBills

- These tables are distinct from the original "LegParticipation" table, mostly because I didn't
  want to mess with that. Tables contain info that answers questions 19-23 in the "Analytical Queries"
  document. Because the Utterance table is so large, I found it was much faster to read data into
  memory and run the joins from there.
"""


import os
import numpy as np
import pymysql
import pandas as pd


CONN_INFO = {
             'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'DDDB2016Aug',
             'user': 'dbMaster',
             'passwd': 'BalmerPeak'
             }


def read_data(conn):
    """Reads data from db and creates necessary dataframe"""
    q = 'SELECT * FROM currentUtterance'
    u_df = pd.read_sql(q, conn)

    q = 'SELECT * FROM Legislator'
    leg_df = pd.read_sql(q, conn)

    q = 'SELECT * FROM Video'
    vid_df = pd.read_sql(q, conn)

    q = 'SELECT * FROM Hearing'
    hear_df = pd.read_sql(q, conn)

    df = u_df.merge(leg_df, on='pid', suffixes=('', '_leg'))
    df = df.merge(vid_df, on='vid', suffixes=('', '_vid'))
    df = df.merge(hear_df, on='hid', suffixes=('', '_hear'))

    # Want to exclude blank utterances
    df = df[pd.notnull(df.text)]

    df['word_count'] = df.text.apply(lambda s: len(s.split()))
    df['time_in_hours'] = (df.endTime - df.time) / 3600

    return df


def add_com_data(df, conn):
    """Adds committee info to utterance df. Why is this a separate action? Because
       joining committee hearings can duplicate utterance rows. This is resolved by a group by
       in the calculations."""

    q = 'SELECT * FROM CommitteeHearings'
    com_df = pd.read_sql(q, conn)

    df = df.merge(com_df, on='hid', suffixes=('', '_com'))

    return df


def calc_verbal_participation(df, group_key):
    """Calculates verbal participation metrics and appends them to dataframe. Returns the
       modified dataframe"""
    group_key = group_key
    all_key = [e for e in group_key if e != 'pid']

    group = df.groupby(group_key)
    df = group[['word_count', 'time_in_hours']].apply(np.sum).reset_index()

    avg_df = df.groupby(all_key)['word_count', 'time_in_hours'].apply(np.mean).reset_index()

    df.rename(columns={'word_count': 'leg_word_count',
                       'time_in_hours': 'leg_time_in_hours'}, inplace=True)

    avg_df.rename(columns={'word_count': 'avg_word_count',
                           'time_in_hours': 'avg_time_in_hours'}, inplace=True)

    df = df.merge(avg_df, on=all_key)

    return df


def create_statements(row, info, leg_info):
    """Meant to be applied to a row of a dataframe. Merges utterance text together based on timestamps
       and updates running word count and time count totals."""
    if row['time'] == info['end_time']:
        info['text'] = info['text'] + ' ' + row['text']
        info['end_time'] = row['endTime']
    else:
        leg_info['total_time'] += info['end_time'] - info['start_time']
        leg_info['total_words'] += len(info['text'].split())
        leg_info['total_stmts'] += 1

        info['start_time'] = row['time']
        info['end_time'] = row['endTime']
        info['text'] = row['text']


def calc_avg_statement_len(df, tbl_key):
    """Given a dataframe of utterances the key we want to average over (eg all utterances,
       utterances at the committee level) calculates the average utterance length in terms of
       time and word count.

       tbl_key: Should be of a collection (eg list)
       """
    all_leg_info = dict()
    for tup, g_df in df.groupby(tbl_key + ['vid']):
        # This is done like this so our key can be used for a generic set up groupings
        leg_key = tuple((k, v) for k, v in zip(tbl_key, tup[:-1]))

        # Checks if the legislator has been encountered before
        if leg_key not in all_leg_info:
            all_leg_info[leg_key] = {'total_time': 0, 'total_words': 0, 'total_stmts': 0}
        leg_info = all_leg_info[leg_key]

        g_df = g_df.sort_values('time')

        row = g_df.iloc[0]
        info = {
            'start_time': row['time'],
            'end_time': row['endTime'],
            'text': row['text']
        }
        g_df[1:].apply(lambda row: create_statements(row, info, leg_info), axis=1)

        # The last row won't be added in the apply
        leg_info['total_time'] += info['end_time'] - info['start_time']
        leg_info['total_words'] += len(info['text'].split())
        leg_info['total_stmts'] += 1

    rows = []
    for key, info in all_leg_info.items():
        row = {k: v for k, v in key}

        row['avg_time_in_sec'] = info['total_time'] / info['total_stmts']
        row['avg_word_count'] = info['total_words'] / info['total_stmts']
        rows.append(row)

    return pd.DataFrame(rows)


def write_tbl(df, conn, tbl_name, pk):
    """Writes table to db and adds pk. The primary key needs to be a list"""
    df.to_sql(tbl_name, conn, flavor='mysql', if_exists='replace')

    c = conn.cursor()

    pk_str = '(' + ','.join(pk) + ')'
    q = 'ALTER TABLE {} ADD PRIMARY KEY {}'.format(tbl_name, pk_str)

    c.execute(q)


def main():
    conn = pymysql.connect(**CONN_INFO)

    df = read_data(conn)
    df_com = add_com_data(df, conn)

    by_leg_key = ['pid', 'session_year']
    by_com_key = ['pid', 'cid', 'session_year']

    verb_df = calc_verbal_participation(df, by_leg_key)
    verb_df_com = calc_verbal_participation(df_com, by_com_key)

    write_tbl(verb_df, conn, 'LegParticipationVerbal_analyt', by_leg_key)
    write_tbl(verb_df_com, conn, 'LegParticipationVerbalCom_analyt', by_com_key)

    stmt_df = calc_avg_statement_len(df, by_leg_key)
    stmt_df_com = calc_avg_statement_len(df_com, by_com_key)

    write_tbl(stmt_df, conn, 'LegParticipationStmtLen_analyt', by_leg_key)
    write_tbl(stmt_df_com, conn, 'LegParticipationStmtLenCom_analyt', by_com_key)

    conn.close()


if __name__ == '__main__':
    main()