import os, os.path
import pymysql
import csv
import pandas as pd
import datetime
import pickle

DATA_DIR = 'BillAnalysisText'

CONN_INFO = {'host': 'digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

BILL_PREPEND = 'CA_20152016'


# Returns a pandas dataframe of (hid, bid, date) columns
def load_bill_info(cursor):

    query = """SELECT bd.bid, h.hid, h.date
               FROM BillDiscussion bd
                  JOIN Hearing h
                  ON bd.hid = h.hid
                WHERE h.state = 'CA' """
    cursor.execute(query)
    tuples = [[bid, hid, date] for bid, hid, date in cursor]

    query = """SELECT bid
               FROM Bill
               WHERE state = 'CA'"""
    cursor.execute(query)

    bill_set = set([bid[0] for bid in cursor])

    return pd.DataFrame(tuples, columns=['bid', 'hid', 'date']), bill_set

def load_org_info(cursor):

    query = """SELECT oid, name
               FROM Organizations"""
    cursor.execute(query)
    tuples = [[oid, name] for oid, name in cursor]

    return pd.DataFrame(tuples, columns=['oid', 'name'])

def get_file_data(f_name):
    data = {}
    if 'support' in f_name.lower():
        data['alignment'] = 'For'
    elif 'oppose' in f_name.lower():
        data['alignment'] = 'Against'
    else:
        assert False

    splits = f_name.split('_')
    bill_type, bill_num, bill_date = splits[0], splits[1], splits[2]
    session = '0'
    if 'X' in bill_type:
        bill_type, session = tuple(bill_type.split('X'))

    data['bid'] = BILL_PREPEND + session + bill_type + bill_num

    date_info = bill_date.split('-')
    data['date'] = datetime.date(int(date_info[2]), int(date_info[0]), int(date_info[1]))

    return data

# Gets the oid for an organization given an org name. Inserts a new org if one cannot be found
def get_oid(cursor, org_df, org_name):
    oid_out = None
    matched_rows = org_df[org_df.name.str.lower() == org_name.lower()]
    if len(matched_rows.index) > 0:
        oid_out = matched_rows.iloc[0]['oid']
    else:
        stmt = """INSERT INTO Organizations
                  (name, analysis_flag)
                  VALUES
                  ("%s", True)"""
        cursor.execute(stmt % org_name)
        oid_out = cursor.lastrowid
        org_df.loc[len(org_df.index)] = [oid_out, org_name]

    assert oid_out
    return oid_out


# Returns: The hid of the hearing this bill analysis should be associated with
def match_hearing(file_info, bill_info_df, bill_set):

    # Wouldn't it be nice to live in a world with clean data?
    if 'DWDA' not in file_info['bid']:
        assert file_info['bid'] in bill_set

    possible_hearings_df = bill_info_df[(bill_info_df.bid == file_info['bid']) &
                                        (bill_info_df.date >= file_info['date'])]

    if len(possible_hearings_df.index) > 0:

        matched_hearing = possible_hearings_df[possible_hearings_df['date'] == possible_hearings_df['date'].min()]
        # I resent having to comment this out
        # assert len(matched_hearing) == 1
        return matched_hearing.iloc[0]['hid']
    else:
        return None


def insert_alignment(cursor, bid, hid, oid, alignment):

    stmt = """INSERT INTO OrgAlignments
              (bid, hid, oid, alignment, analysis_flag)
              VALUES
              ("%s", %s, %s, "%s", True)"""

    try:
        cursor.execute(stmt % (bid, hid, oid, alignment))
    except pymysql.err.IntegrityError:
        pass
        # print('Repeat Values')

def filter_org_names(org_name):
    org_name = org_name.replace('<u>', '')
    org_name = org_name.replace('<', '')
    org_name = org_name.replace('>', '')
    org_name = org_name.replace('\\', '')
    org_name = org_name.replace('"', "'")
    org_name = org_name.strip()

    if len(org_name) < 4:
        return None
    if len(org_name.split(' ')) > 13:
        print(org_name)
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
    elif 'verified (' in org_name.lower():
        return None
    elif 'analysis prepared by' in org_name.lower():
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
                    hid = match_hearing(file_info, bill_info_df, bill_set)

                    if hid:
                        for org_name in rows:
                            org_name = filter_org_names(org_name[1:-1])
                            if org_name:
                                oid = get_oid(cursor, org_df, org_name)

                                insert_alignment(cursor, file_info['bid'], hid, oid, file_info['alignment'])

                    f_obj.close()
        print('blah')


if __name__ == '__main__':
    main()