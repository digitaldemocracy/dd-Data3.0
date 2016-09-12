'''
File: ExtractLegStaffData.py
Author: Andrew Voorhees
Date: 3/27/2016
Description:
- Goes through the file LegStaffForm700Data.csv and places the data into DDDB2015Dec
- Fills table LegStaffData

Sources:
- LegStaffForm700Data.csv
'''

import pymysql
import datetime
import pickle
import pandas as pd
import pandas.io.sql as pdsql
import numpy as np
from dateutil.relativedelta import relativedelta

from MatchingFunctions import clean_date
from MatchingFunctions import cmp_names

CONN_INFO = {'host': 'digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2015Dec',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}
DATA_DIR = 'GiftData'
DATA_FILE = 'GiftData.csv'

APPENDED_INFO = set(['vanilla_match',
                     # 'name_match',
                     # 'full_date_match',
                     'multi_match',
                     # 'fuzzy_date_match',
                     'full_leg_match',
                    ])
                 # 'leg_pid',
                 # 'staff_pid',
                 # 'matched_staff_first',
                 # 'matched_staff_middle',
                 # 'matched_staff_last',
                 # 'leg_first',
                 # 'leg_middle',
                 # 'leg_last',
                 # 'leg_pid',
                 # ]


# Adds column headers to you gift data df
def add_headers(gift_df):
    # gift_df.drop(gift_df.columns[list(range(26,35))], axis=1, inplace=True)
    col_names = ['year_filed',
                 'agency_name',
                 'last_name',
                 'first_name',
                 'person_type',
                 'position',
                 'district_number',
                 'jurisdiction',
                 'D_source_name',
                 'D_source_city',
                 'D_source_state',
                 'D_source_business',
                 'D_date_of_gift',
                 'D_gift_value',
                 'D_reimbursed',
                 'D_gift_description',
                 'E_source_name',
                 'E_source_city',
                 'E_source_state',
                 'E_source_business',
                 'E_date_of_gift',
                 'E_gift_value',
                 'E_gift_or_income',
                 'E_speech_or_panel',
                 'E_gift_description',
                 'image_url']
    gift_df.columns = col_names


# Gets all the leg office personnel out of the db
# Returns: df containing the values found in the table
def fetch_leg_personnel(cnxn):
    # cursor = cnxn.cursor()
    query = """SELECT staff_member, legislator, l.first as leg_first, l.middle as leg_middle,
                l.last as leg_last, start_date, end_date, p.first as staff_first, p.middle as staff_middle,
                p.last as staff_last, district, t.year as term_year, lop.house
               FROM LegOfficePersonnel lop
                   JOIN Person p
                   ON lop.staff_member = p.pid
                   JOIN Term t
                   ON lop.legislator = t.pid and lop.term_year = t.year
                    and lop.house = t.house and lop.state = t.state
                   JOIN Person l
                   ON t.pid = l.pid"""

    return pdsql.read_sql(query, cnxn)


# Gets all the office personnel out of the db
# Returns: df containing the values found in the table
def fetch_office_personnel(cnxn):
    # cursor = cnxn.cursor()
    query = """SELECT staff_member, office, start_date, end_date, p.first as staff_first,
                p.middle as staff_middle, p.last as staff_last, o.name as office_name,
                o.house
               FROM OfficePersonnel op
                   JOIN Person p
                   ON op.staff_member = p.pid
                   JOIN LegislatureOffice o
                   ON op.office = o.lo_id"""

    return pdsql.read_sql(query, cnxn)

# Does it's very best to return a number representation of whatever object it's given
def clean_number(num):
    if type(num) == float:
        return num
    if type(num) == np.float64:
        return num
    if pd.isnull(num):
        return num
    if num.strip() == '':
        return np.nan
    if type(num) == str:
        num = num.replace(',', '').replace('$', '')
        return float(num)
    assert False


# Cleans up all the problematic data found in the spreadsheets
# Returns: Cleaned dataframe
def clean_data(gift_df):
    gift_df['D_cleaned_date'] = np.vectorize(clean_date)(gift_df['D_date_of_gift'], gift_df['year_filed'])
    gift_df['E_cleaned_date'] = np.vectorize(clean_date)(gift_df['E_date_of_gift'], gift_df['year_filed'])
    clean_reimbursed = lambda x: True if x == 'TRUE' else False
    gift_df['D_reimbursed'] = gift_df['D_reimbursed'].apply(clean_reimbursed)
    clean_speech_or_panel = lambda x: True if not pd.isnull(x) else False
    gift_df['E_speech_or_panel'] = gift_df['E_speech_or_panel'].apply(clean_speech_or_panel)
    gift_df['D_gift_value'] = gift_df['D_gift_value'].apply(clean_number)
    gift_df['E_gift_value'] = gift_df['E_gift_value'].apply(clean_number)

    return gift_df


# Builds your basic row for appending onto the dataframe
# Returns: Dictionary with the appropriate values
def build_base_row(gift_row, c_set2):
    out = {k: None for k in set(gift_row.index) | c_set2 | APPENDED_INFO}
    for k in gift_row.index:
        out[k] = gift_row[k]
    for k in APPENDED_INFO:
        out[k] = False
    return out

# Matches the staff member name given to a person in leg office personnel if possible
# Returns: Dictionary of pid, and the names of matched staff
def match_op(op_row, gift_row):
    out = build_base_row(gift_row, set(op_row.index))

    name1 = (gift_row['first_name'], None, gift_row['last_name'])
    name2 = (op_row['staff_first'], op_row['staff_middle'], op_row['staff_last'])
    if cmp_names(*(name1 + name2)):
        # out['name_match'] = True
        out['vanilla_match'] = True
        for k, v in op_row.iteritems():
            out[k] = v
        # want to make sure these comparisons make sense
        # assert type(op_row['start_date']) == datetime.date
        # assert type(op_row['end_date']) == datetime.date or pd.isnull(op_row['end_date'])
        # assert type(gift_row['cleaned_date']) == datetime.date or pd.isnull(gift_row['cleaned_date'])
        #
        # if gift_row['cleaned_date'] and gift_row['cleaned_date'] > op_row['start_date'] and \
        #         (pd.isnull(op_row['end_date']) or gift_row['cleaned_date'] < op_row['end_date']):
        #     out['full_date_match'] = True
        # elif gift_row['cleaned_date'] and gift_row['cleaned_date'] > (op_row['start_date'] - relativedelta(months=2))\
        #         and (pd.isnull(op_row['end_date']) or gift_row['cleaned_date'] < (op_row['end_date']) \
        #                 + relativedelta(months=2)):
        #     out['fuzzy_date_match'] = True

    # assert type(out['full_date_match']) == bool
    # assert type(out['name_match']) == bool

    return pd.Series(out)


# Guesses the house
def guess_house(agency_name):
    if 'assembly' in agency_name.lower():
        house = 'Assembly'
    elif 'senat' in agency_name.lower():
        house = 'Senate'
    else:
        house = None
    return house


# Matches a staff member from the name on the spreadsheet to a staff member that we had
# found previously
# Returns pd.Series of information about the validity of the match
def match_staff_member(gift_row, leg_lop_df):

    # leg_lop_df = leg_lop_df[leg_lop_df.primary_source]
    out = pd.Series(build_base_row(gift_row, set(leg_lop_df.columns)))

    house_guess = guess_house(gift_row['agency_name'])
    name = (gift_row['first_name'], None, gift_row['last_name'])

    if house_guess:
        match_df = leg_lop_df[(gift_row['cleaned_date'] > (leg_lop_df['start_date'] - relativedelta(months=2))) &
                              (gift_row['cleaned_date'] < leg_lop_df['end_date'] + relativedelta(months=2)) &
                              (leg_lop_df.house == house_guess)]
    else:
        match_df = leg_lop_df[(gift_row['cleaned_date'] > (leg_lop_df['start_date'] - relativedelta(months=2))) &
                              (gift_row['cleaned_date'] < leg_lop_df['end_date'] + relativedelta(months=2))]

    match_df = match_df[match_df.apply(lambda row: cmp_names(*(name + (row['staff_first'], row['staff_middle'],
                                                                       row['staff_last']))), axis=1)]
    if len(match_df.index) == 1:
        row = match_df.iloc[0]
        row['vanilla_match'] = True
        for k, v in row.iteritems():
            out[k] = v
    elif len(match_df.index) > 1:
        new_match_df = match_df[(gift_row['cleaned_date'] > match_df['start_date']) &
                            (gift_row['cleaned_date'] < match_df['end_date'])]
        if len(new_match_df.index):
            row = new_match_df.iloc[0]
            if len(new_match_df.index) == 1:
                row['vanilla_match'] = True
            else:
                row['multi_match'] = True
        else:
            row = match_df.iloc[0]
            row['multi_match'] = True

        for k, v in row.iteritems():
            out[k] = v

    print(datetime.datetime.now())

    return out


    # full_match_df = match_df.loc[match_df['full_date_match'], :]
    # fuzzy_match_df = match_df.loc[match_df['fuzzy_date_match'], :]
    # if len(full_match_df.index) == 1:
    #     out = full_match_df.iloc[0]
    # elif len(full_match_df.index) > 1:
    #     print('multiple matches')
    #     out = full_match_df.iloc[0]
    #     out['multi_match'] = True
    #     # assert False
    # elif len(fuzzy_match_df.index) == 1:
    #     out = fuzzy_match_df.iloc[0]
    # elif len(fuzzy_match_df.index) > 1:
    #     print('multiple matches')
    #     out = fuzzy_match_df.iloc[0]
    #     out['multi_match'] = True
    #     # assert False
    # else:
    #     name_match_df = match_df.loc[match_df['name_match'], :]
    #     if len(name_match_df.index):
    #         out = name_match_df.iloc[0]
    #     else:
    #         out = pd.Series(build_base_row(gift_row, set(leg_lop_df.columns)))
    #
    # assert len(out) > 0
    # assert type(out) == pd.core.series.Series
    # assert type(out['multi_match']) == bool or type(out['multi_match']) == np.bool_
    # assert type(out['full_date_match']) == bool or type(out['full_date_match']) == np.bool_
    # assert type(out['name_match']) == bool or type(out['name_match']) == np.bool_

    # print(datetime.datetime.now())
    #
    # return out


# Renames all the D or E values to their generic types. Takes a list of columns
# Returns data frame with the specified columns renamed
def rename_columns(gift_df, letter):
    if letter == 'D':
        drop_letter = 'E'
    elif letter == 'E':
        drop_letter = 'D'
    else:
        assert False
    new_cols = []
    for col in gift_df.columns:
        if letter in col:
            new_cols.append(col[2:])
        elif drop_letter in col:
            gift_df.drop(col, axis=1, inplace=True)
        else:
            new_cols.append(col)
    gift_df.columns = new_cols
    return gift_df


# Gets the leg term information from the database
# Returns: A dataframe containing the legislator information with their corresponding terms
def fetch_leg_terms(cnxn):
    cursor = cnxn.cursor()
    query = """SELECT p.pid, p.first, p.middle, p.last, t.year, t.house
               FROM Legislator l
                  JOIN Term t
                  ON l.pid = t.pid
                  JOIN Person p
                  ON l.pid = p.pid"""

    cursor.execute(query)
    rows = [[pid, first, middle, last, year, house] for pid, first, middle, last, year, house in cursor]
    return pd.DataFrame(rows, columns=['pid', 'first', 'middle', 'last', 'year', 'house'])


# Matches the gift row to the corresponding leg_terms_df row. The parallel to the match_op
# function but for legislators
# Returns: Dictionary containing the pid, and name of the matched leg
def match_leg_helper(leg_term_row, gift_row, new_row_cols, house):
    out = build_base_row(gift_row, new_row_cols)

    name1 = (gift_row['first_name'], None, gift_row['last_name'])
    name2 = (leg_term_row['first'], leg_term_row['middle'], leg_term_row['last'])

    if cmp_names(*(name1 + name2)) and leg_term_row['house'] == house:
        out['full_leg_match'] = True
        out['legislator'] = leg_term_row['pid']
        out['leg_first'] = leg_term_row['first']
        out['leg_middle'] = leg_term_row['middle']
        out['leg_last'] = leg_term_row['last']

    return pd.Series(out)


# Matches a row to the corresponding legislator that the gift was apparently given directly to
# Returns: pd.Series fo information about the validity of the match
def match_leg(gift_row, leg_terms_df, new_row_cols):
    if 'assembly' in gift_row['person_type'].lower():
        house = 'Assembly'
    elif 'senator' in gift_row['person_type'].lower():
        house = 'Senate'
    assert house

    matched_rows_df = leg_terms_df.apply(lambda row: match_leg_helper(row, gift_row, new_row_cols, house),
                                         axis=1)
    full_matched_rows_df = matched_rows_df.loc[matched_rows_df.full_leg_match]

    assert len(full_matched_rows_df.index) == 1
    row = full_matched_rows_df.iloc[0]

    print(datetime.datetime.now())
    return pd.Series(row)


# Adds the districts to the leg lop df
def add_districts(cursor, leg_lop_df):
    query = """SELECT pid, year, district
               FROM Term
               WHERE state = 'CA'"""
    cursor.execute(query)
    districts = {(pid, year): district for pid, year, district in cursor}
    leg_lop_df['district_number'] = np.nan
    leg_lop_df.ix[pd.notnull(leg_lop_df.legislator), 'district_number'] = \
        leg_lop_df[pd.notnull(leg_lop_df.legislator)].apply(lambda row: districts[(row['legislator'],\
                                                                                  row['term_year'])], axis=1)
    return leg_lop_df


COLUMNS_ORDER_D = ['year_filed',
                   'agency_name',
                   'last_name',
                   'first_name',
                   'staff_member',
                   'legislator',
                   'leg_first',
                   'leg_last',
                   'lo_id',
                   'office',
                   'person_type',
                   'position',
                   'district_number',
                   'jurisdiction',
                   'source_name',
                   'source_city',
                   'source_state',
                   'source_business',
                   'date_of_gift',
                   'gift_value',
                   'reimbursed',
                   'gift_description',
                   'image_url']

COLUMNS_ORDER_E = ['year_filed',
                   'agency_name',
                   'last_name',
                   'first_name',
                   'staff_member',
                   'legislator',
                   'leg_first',
                   'leg_last',
                   'lo_id',
                   'office',
                   'person_type',
                   'position',
                   'district_number',
                   'jurisdiction',
                   'source_name',
                   'source_city',
                   'source_state',
                   'source_business',
                   'date_of_gift',
                   'gift_value',
                   'gift_or_income',
                   'speech_or_panel',
                   'gift_description',
                   'image_url']


def main():

    # load_data = True
    load_data = False
    if load_data:

        cnxn = pymysql.connect(**CONN_INFO)
        path = DATA_DIR + '/' + DATA_FILE
        # There's a bunch of empty columns at the end
        gift_df = pd.read_csv(path).iloc[:, :26]
        add_headers(gift_df)
        # leg_lop_df = fetch_leg_personnel(cnxn)
        # op_df = fetch_office_personnel(cnxn)
        leg_terms_df = fetch_leg_terms(cnxn)
        cnxn.close()
        pickle.dump(gift_df, open('gift_df.p', 'wb'))
        pickle.dump(leg_terms_df, open('leg_terms_df.p', 'wb'))
        # pickle.dump(leg_lop_df, open('leg_lop_df.p', 'wb'))
        # pickle.dump(op_df, open('op_df.p', 'wb'))

    else:
        with pymysql.connect(**CONN_INFO) as cursor:
            gift_df = pickle.load(open('gift_df.p', 'rb'))
            leg_terms_df = pickle.load(open('leg_terms_df.p', 'rb'))
            leg_lop_df = pickle.load(open('leg_lop_df.p', 'rb'))
            leg_lop_df = leg_lop_df.rename(columns = {'first': 'staff_first',
                                                      'middle': 'staff_middle',
                                                      'last': 'staff_last',
                                                      'pid': 'staff_member',
                                                      'leg_pid': 'legislator',
                                                      'hire_date': 'start_date'
                                                      })
            # leg_lop_df['end_date'] = leg_lop_df.apply(correct_end_dates, axis=1)
            # op_df = pickle.load(open('op_df.p', 'rb'))
            leg_lop_df = add_districts(cursor, leg_lop_df)

            gift_df = clean_data(gift_df)

            d_gift_df = gift_df[~pd.isnull(gift_df.D_gift_value)].copy()
            e_gift_df = gift_df[~pd.isnull(gift_df.E_gift_value)].copy()
            mix_gift_df = gift_df[~pd.isnull(gift_df.E_gift_value) & ~pd.isnull(gift_df.D_gift_value)]
            assert mix_gift_df.shape[0] == 0

            d_gift_df = rename_columns(d_gift_df, 'D')
            d_gift_df['og_index'] = d_gift_df.index
            e_gift_df = rename_columns(e_gift_df, 'E')
            e_gift_df['og_index'] = e_gift_df.index
            # It's important that you don't overwrite any data
            assert len(d_gift_df.columns & leg_lop_df.columns & APPENDED_INFO) == 0
            assert len(e_gift_df.columns & leg_lop_df.columns & APPENDED_INFO) == 0

            # e_gift_df = e_gift_df.sample(10)
            pickle.dump(e_gift_df, open('e_gift_df.p', 'wb'))
            pickle.dump(d_gift_df, open('d_gift_df.p', 'wb'))
            staff_idx = e_gift_df['person_type'] == 'Staff'
            leg_idx = (e_gift_df['person_type'] == 'Assemblymember') | (e_gift_df['person_type'] == 'Senator')
            # e_gift_df_legs = e_gift_df.loc[leg_idx].apply(lambda row: match_leg(row, leg_terms_df, set(leg_lop_df.columns)),
            #                                     axis=1)
            # pickle.dump(e_gift_df_legs, open('e_gift_df_legs.p', 'wb'))
            e_gift_df = e_gift_df.sample(5)
            # e_gift_df = e_gift_df.ix[[11415, 13756]]
            pickle.dump(e_gift_df, open('e_gift_df_staff_before.p', 'wb'))
            e_gift_df_staff = e_gift_df.loc[staff_idx].apply(lambda row: match_staff_member(row, leg_lop_df), axis=1)
            pickle.dump(e_gift_df_staff, open('e_gift_df_staff.p', 'wb'))
            e_gift_df_staff.to_excel('sched_e_data.xlsx', columns=COLUMNS_ORDER_E)
            # d_gift_df = d_gift_df.apply(lambda row: match_staff_member(row, leg_lop_df), axis=1)
            # match_counts = d_gift_df['og_index'].value_counts()
            # assert sum(match_counts > 1) == 0


if __name__ == '__main__':
    main()





