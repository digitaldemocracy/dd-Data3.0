import os, os.path
import pymysql
import pandas as pd
from datetime import datetime
import requests
import zipfile
from io import BytesIO

DATA_DIR = 'BillAnalysisOut'

CONN_INFO = {'host': 'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             #'host': 'dev.digitaldemocracy.org',
             'port': 3306,
             'db': 'DDDB2016Aug',
             # 'db': 'AndrewTest',
             #'user': 'dbMaster',
             'user': 'scripts',
             'passwd': os.environ['DBMASTERPASSWORD']}

BILL_PREPEND = 'CA_'


"""Gets bill info out of the db. Returns a dataframe"""
def load_bill_info(cursor):

    query = """SELECT bd.did, bd.bid, h.hid, h.date
               FROM BillDiscussion bd
                  JOIN Hearing h
                  ON bd.hid = h.hid
                WHERE h.state = 'CA' """
    cursor.execute(query)
    tuples = [[did, bid, hid, date] for did, bid, hid, date in cursor]

    query = """SELECT bid
               FROM Bill
               WHERE state = 'CA'"""
    cursor.execute(query)

    bill_set = set([bid[0] for bid in cursor])

    return pd.DataFrame(tuples, columns=['did', 'bid', 'hid', 'date']), bill_set

"""Gets org info out of the db. Returns a dataframe"""
def load_org_info(cursor):

    query = """SELECT oid, name
               FROM Organizations"""
    cursor.execute(query)
    tuples = [[oid, name] for oid, name in cursor]

    return pd.DataFrame(tuples, columns=['oid', 'name'])

def get_file_data(f_name):
    data = {}
    data_lst = f_name.split('_')
    if data_lst[2].lower().replace('.csv', '') == 'support':
        data['alignment'] = 'For'
    elif data_lst[2].lower().replace('.csv', '') == 'oppose':
        data['alignment'] = 'Against'
    else:
        assert False

    data['bid'] = 'CA_' + data_lst[0]
    date_str = data_lst[1]
    data['date'] = datetime.strptime(date_str, '%Y-%m-%d').date()

    return data

"""Gets the oid for an organization given an org name. Inserts a new org if one cannot be found"""
def get_oid(cursor, org_df, org_name):
    oid_out = None
    matched_rows = org_df[org_df.name.str.lower() == org_name.lower()]
    if len(matched_rows.index) > 0:
        oid_out = matched_rows.iloc[0]['oid']
    else:
        stmt = """INSERT INTO Organizations
                  (name, analysis_flag, source)
                  VALUES
                  ("%s", True, "Alignment Meter")"""
        cursor.execute(stmt % org_name)
        oid_out = cursor.lastrowid
        org_df.loc[len(org_df.index)] = [oid_out, org_name]

    assert oid_out
    return oid_out


"""Returns: The hid of the hearing this bill analysis should be associated with"""
def match_hearing(file_info, bill_info_df, bill_set):

    # Wouldn't it be nice to live in a world with clean data?
    if 'DWDA' not in file_info['bid']:
        assert file_info['bid'] in bill_set

    possible_hearings_df = bill_info_df[(bill_info_df.bid == file_info['bid']) &
                                        (bill_info_df.date >= file_info['date'])]

    if len(possible_hearings_df.index) > 0:

        matched_hearing = possible_hearings_df[possible_hearings_df['date'] == possible_hearings_df['date'].min()]
        if len(matched_hearing.index) > 1 and len(matched_hearing.date.unique()) > 1:
            assert False
        return matched_hearing.iloc[0]['hid']
    else:
        return None


"""Gets the 'session year' for any given year"""
def get_session_year(year):
    if year % 2 == 1:
        return year
    else:
        return year - 1


"""Inserts an alignment into the db"""
def insert_alignment(cursor, bid, hid, oid, alignment, date):
    # Makes these work with pymysql
    oid = int(oid)
    if hid:
        hid = int(hid)

    session_year = get_session_year(date.year)


    stmt = """INSERT INTO OrgAlignments
              (bid, hid, oid, alignment, analysis_flag, alignment_date, session_year)
              VALUES
              (%s, %s, %s, %s, True, %s, %s)"""

    try:
        cursor.execute(stmt, (bid, hid, int(oid), alignment, date, session_year))
    except pymysql.err.IntegrityError as e:
        # just means it's a repeat
        # print('Repeat Values')
        # print(stmt % (bid, hid, oid, alignment, date, session_year))
        pass

"""Filters obviously bad organization names"""
def filter_org_names(org_name):
    org_name = org_name.replace('<u>', '')
    org_name = org_name.replace('<', '')
    org_name = org_name.replace('>', '')
    org_name = org_name.replace('\\', '')
    org_name = org_name.replace('"', "'")
    org_name = org_name.strip()

    if len(org_name) < 4:
        return None
    if len(org_name.split(' ')) > 8:
        # print(org_name)
        return None
    elif org_name.lower() == 'california':
        return None
    elif org_name.lower() == 'affiliate':
        return None
    elif org_name.lower() == 'AFSCME'.lower():
        return None
    elif org_name.lower() == 'AFL-CIO'.lower():
        return None
    elif org_name.lower() == 'Agencies'.lower():
        return None
    elif org_name.lower() == 'County'.lower():
        return None
    elif org_name.lower() == 'Date of Hearing'.lower():
        return None
    elif 'verified (' in org_name.lower():
        return None
    elif 'analysis prepared by' in org_name.lower():
        return None
    elif 'arguments in support' in org_name.lower():
        return None
    else:
        return org_name


def main():
    with pymysql.connect(**CONN_INFO) as cursor:

        bill_info_df, bill_set = load_bill_info(cursor)
        org_df = load_org_info(cursor)

        for root, _, files in os.walk(DATA_DIR):
            file_num = 0
            for f in files:
                file_num += 1
                if '.csv' in f:

                    full_path = os.path.join(root, f)
                    f_obj = open(full_path, 'r', encoding='utf-8', errors='ignore')
                    # reader = csv.reader(f_obj, delimiter=',')
                    rows = [row for row in f_obj.read().split(',\n') if row.strip() != '']
                    file_info = get_file_data(f)

                    # sigh, sometimes the bills might not be inserted yet
                    if file_info['bid'] in bill_set:
                        hid = match_hearing(file_info, bill_info_df, bill_set)

                        for org_name in rows:
                            org_name = filter_org_names(org_name[1:-1])
                            if org_name:
                                oid = get_oid(cursor, org_df, org_name)

                                insert_alignment(cursor, file_info['bid'], hid, oid, file_info['alignment'],
                                                 file_info['date'])

                    f_obj.close()


if __name__ == '__main__':
    main()
