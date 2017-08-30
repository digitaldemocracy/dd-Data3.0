
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

import csv
import pymysql
import itertools
import datetime
import re
from unidecode import unidecode
import pickle


# apologies to future Andrew for this function
def convert_date(date_str, previous_date):
    date_str = date_str.replace("**", "01")
    date_str = date_str.replace("*", "01")
    date_str = date_str.replace("/00/", "/01/")
    date_str = date_str.replace("/  /", "/01/")
    date_str = date_str.replace("/ /", "/01/")
    date_str = date_str.replace('.', '/')
    date_str = date_str.replace("//", "/")
    if date_str != '':
        if date_str[-1] == '/':
            date_str = date_str[:-1]
        elif date_str[-1] == '`':
            date_str = date_str[:-1]
    try:
        if date_str.strip() == "":
            out = previous_date
        elif date_str == '50/20/11':
            out = datetime.date(2011, 5, 20)
        elif date_str == 'Jan or Feb 2011':
            out = datetime.date(2011, 2, 1)
        elif date_str == '808/17/13':
            out = datetime.date(2013, 8, 17)
        elif date_str == '06/13-14/12':
            out = datetime.date(2012, 6, 13)
        elif date_str == '06\\22\\13':
            out = datetime.date(2013, 6, 22)
        elif date_str == '12/87/14':
            out = datetime.date(2014, 12, 12)
        elif date_str == '08?01/13':
            out = datetime.date(2013, 8, 1)
        elif date_str == '12/00/14':
            out = datetime.date(2014, 12, 1)
        elif date_str == '110612':
            out = datetime.date(2012, 11, 6)
        elif date_str == 'APRIL/MAY 2011':
            out = datetime.date(2011, 5, 1)
        elif date_str == '25/6/13':
            out = datetime.date(2013, 6, 25)
        elif date_str == '01/18 &19/11':
            out = datetime.date(2011, 1, 19)
        elif date_str == '04/20812':
            out = datetime.date(2012, 4, 20)
        elif date_str == '10/19/14`':
            out = datetime.date(2014, 10, 19)
        elif re.match(r'\d+-\w{3}', date_str):
            out = None
        elif date_str == '16/15/14':
            out = None
        elif date_str == '9192013':
            out = datetime.date(2013, 9, 19)
        elif date_str == '09.05.12':
            out = datetime.date(2012, 9, 5)
        elif date_str == '11//8/11':
            out = datetime.date(2011, 11, 8)
        elif date_str == '09/   /13':
            out = datetime.date(2013, 9, 1)
        elif date_str == '/':
            out = None
        elif date_str == '10/11':
            out = datetime.date(2011, 10, 1)
        elif date_str == '02/04/14-02/05/14':
            out = datetime.date(2014, 2, 4)
        elif date_str == '09/17':
            out = None
        elif date_str == '-0.027777778':
            out = None
        elif date_str == '04/07014':
            out = datetime.date(2014, 4, 7)
        elif date_str == '11':
            out = None
        elif date_str == '10/1713':
            out = datetime.date(2013, 10, 17)
        elif date_str == '93/22/13':
            out = datetime.date(2013, 9, 22)
        elif date_str == '19/17/11':
            out = datetime.date(2011, 9, 17)
        elif date_str == '-0/027777778':
            out = None
        elif date_str == '05/3-4/2010':
            out = datetime.date(2010, 5, 3)
        elif date_str == '404/16/13':
            out = datetime.date(2013, 4, 16)
        elif date_str == '276':
            out = None
        elif date_str == '00/01/14':
            out = datetime.date(2014, 1, 1)
        elif date_str == '15/11/14':
            out = datetime.date(2014, 5, 11)
        elif date_str == '11613':
            out = datetime.date(2013, 6, 11)
        elif date_str == '9/149/':
            out = None
        elif date_str == '50':
            out = None
        elif date_str == '09/31/13':
            out = datetime.date(2013, 9, 30)
        elif date_str == '9/149':
            out = None
        elif date_str == '404/13/11':
            out = datetime.date(2011, 4, 13)
        elif date_str == '42310':
            out = datetime.date(2010, 4, 23)
        elif date_str == '41210':
            out = datetime.date(2010, 4, 12)
        elif date_str == '12210':
            out = datetime.date(2010, 12, 21)
        elif date_str == '12810':
            out = datetime.date(2010, 12, 8)
        elif date_str == '20610':
            out = datetime.date(2010, 6, 10)
        elif date_str == 'unknown':
            out = None
        elif date_str == '10/29':
            out = None
        elif date_str == '512013':
            out = datetime.date(2013, 5, 1)
        elif date_str == '12':
            out = None
        elif date_str == 'O7/22/12':
            out = datetime.date(2012, 7, 22)
        elif date_str == '08/12814':
            out = datetime.date(2014, 8, 12)
        elif date_str == '2011':
            out = datetime.date(2011, 1, 1)
        elif date_str == '30/19/17':
            out = None
        elif date_str == '01/18/19/11':
            out = datetime.date(2011, 1, 18)
        elif date_str == '02/04/05/14':
            out = datetime.date(2014, 2, 5)
        else:
            try:
                month, day, year = date_str.split("/")
                day = day if '-' not in day else day.split("-")[0]
            except ValueError:
                year, month, day = date_str.split("-")

            year, month, day = int(year), int(month), int(day)
            if year < 100:
                year += 2000
            if month == 0:
                month = 1
            if day == 29 and month == 2:
                day = 28
            if month > 12:
                month = month - 10
            out = datetime.date(year, month, day)
    except ValueError:
        out = None
    return out


def decode_and(name):
    basic = unidecode(name).strip().replace(',', '').lower()
    return re.sub(r'\(.*?\)', '', basic).strip()


def levenshtein_distance(a, b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return current[n]


def drop_E(str):
    if str[0] == 'E':
        return str[2:]
    return str


def concat_names(first, middle, last):
    return first + (' ' + middle + ' ' if middle else ' ') + last


def fetch_staff(cursor):
    query = """SELECT DISTINCT p.pid, p.first, p.middle, p.last
               FROM Person p
                  JOIN LegOfficePersonnel lop
                  ON lop.staff_member = p.pid"""

    cursor.execute(query)

    out = {}
    for pid, first, middle, last in cursor:
        first = first.strip().lower()
        last = last.strip().lower()
        if middle:
            middle = middle.strip().lower()
        key = last[0]
        if key in out:
            out[key].append({"pid": pid,
                             "first": first,
                             "last": last,
                             "middle": middle})
        else:
            out[key] = [{"pid": pid,
                         "first": first,
                         "last": last,
                         "middle": middle}]

    return out


def fetch_leg_personnel(cursor):
    query = """SELECT staff_member, legislator, start_date, end_date, p.first, p.middle, p.last, district
               FROM LegOfficePersonnel lop
                   JOIN Person p
                   ON lop.legislator = p.pid
                   JOIN Term t
                   ON lop.legislator = t.pid and lop.term_year = t.year
                    and lop.house = t.house and lop.state = t.state"""

    cursor.execute(query)

    out = {}
    for staff_member, legislator, start_date, end_date, first, middle, last, district in cursor:
        if staff_member not in out:
            out[staff_member] = []
        out[staff_member].append({"legislator": legislator,
                                  "start_date": start_date,
                                  "end_date": end_date,
                                  "first": first,
                                  "middle": middle,
                                  "last": last,
                                  "district": district})

    return out


def get_staff_member(all_staff, values, row, missed_staff):

    pid = None
    matched_first = None
    matched_last = None
    k = norm_name(values['last_name'])[0]
    subset_staff = all_staff[k]

    matched = []
    for info in subset_staff:
        db = norm_name(info['last'])
        csv = norm_name(values['last_name'])
        if db == csv or (levenshtein_distance(db, csv) <= 1 and len(db) > 5):
            matched.append(info)

    # waay too generous
    # if len(matched) == 1:
    #     pid = matched[0]['pid']
    #     matched_first = matched[0]['first']
    #     matched_last = matched[0]['last']
    for match in matched:
        first_combo = norm_name(match['first'] + ' ' + match['middle'] if match['middle'] else match['first'])
        csv = norm_name(values['first_name'])
        if first_combo == csv:
            pid = match['pid']
            matched_first = match['first']
            matched_last = match['last']
            break
        elif csv in first_combo or first_combo in csv:
            pid = match['pid']
            matched_first = match['first']
            matched_last = match['last']


    if not pid and int(values['year']) > 2011 and (values['last_name'], values['first_name']) not in missed_staff:
        # print("No staff member found for {}, {} @ {}".format(values['last_name'], values['first_name'], row))
        missed_staff.add((values['last_name'], values['first_name'], pid))

    values['staff_member'] = pid
    # Added for the spreadsheet at the end
    values['matched_staff_first'] = matched_first
    values['matched_staff_last'] = matched_last
    values['match_staff_flag'] = 1


def get_legislator(leg_personnel, values, row):

    leg_pid = None
    matched_first = None
    matched_middle = None
    matched_last = None
    matched_district = None
    values['match_leg_flag'] = 0

    if values['staff_member'] and values['date_given']:
        subset_personnel = leg_personnel[values['staff_member']]

        for info in subset_personnel:
            if info['start_date'] < values['date_given'] and not info['end_date']:
                leg_pid = info['legislator']
                matched_first = info['first']
                matched_middle = info['middle']
                matched_last = info['last']
                matched_district = info['district']

            elif info['start_date'] < values['date_given'] and info['end_date'] > values['date_given']:
                leg_pid = info['legislator']
                matched_first = info['first']
                matched_middle = info['middle']
                matched_last = info['last']
                matched_district = info['district']
        # assert(leg_pid)

    values['legislator'] = leg_pid
    values['matched_leg_first'] = matched_first
    values['matched_leg_middle'] = matched_middle
    values['matched_leg_last'] = matched_last
    values['district_number'] = matched_district
    if leg_pid:
        values['match_leg_flag'] = 1


# attempts to match the staff_member to a legislator based on info in the Agency Name field
def get_legislator_fuzzy(values):
    pass


# Finds the staff member from the list of missed staff,
# inserts a new staff member if they could not be found
def match_missed_staff(cursor, values, missed_staff):
    first = values['first_name'].strip()
    last = values['last_name'].strip()
    pid = None

    try:
        pid = missed_staff[(first.lower(), last.lower())]

    except KeyError:
        insert_stmt = """INSERT INTO Person
                         (first, last)
                         VALUES
                         (%s, %s)"""
        cursor.execute(insert_stmt, (first, last))

        pid = cursor.lastrowid
        missed_staff[(first.lower(), last.lower())] = pid

        insert_stmt = """INSERT INTO LegislativeStaff
                         (pid)
                         VALUES
                         (%s)"""
        cursor.execute(insert_stmt, pid)

    assert(pid)
    values['staff_member'] = pid
    values['matched_staff_first'] = first
    values['matched_staff_last'] = last
    values['match_staff_flag'] = 0


def assign_values(row, previous_date):

    return {'year': row[0],
            'agency_name': row[1],
            'last_name': row[2],
            'first_name': row[3],
            'person_type': row[4],
            'position': row[5],
            'district_number': row[6],
            'jurisdiction': row[7],
            'source_name': row[8],
            'source_city': row[9],
            'source_state': row[10],
            'source_business': row[11],
            'date_given': convert_date(row[12], previous_date),
            'original_date': row[12],
            'gift_value': row[13],
            'reimbursed': row[14],
            'gift_description': row[15],
            'E_source_name': row[16],
            'E_source_city': row[17],
            'E_source_state': row[18],
            'E_source_business': row[19],
            'E_date_given': row[20],
            'E_gift_value': row[21],
            'gift_or_income': row[22],
            'speech_or_panel': row[23],
            'E_gift_description': row[24],
            'image_url': row[25]}


def clean_values(values):

    if values['reimbursed'].strip() == '' or values['reimbursed'] == 'FALSE':
        values['reimbursed'] = 0
    if values['reimbursed'] == 'TRUE':
        values['reimbursed'] = 1

    if values['speech_or_panel'].strip() == '' or values['speech_or_panel'] == 'FALSE':
        values['speech_or_panel'] = 0

    if values['speech_or_panel'] == 'TRUE':
        values['speech_or_panel'] = 1

    if values['gift_value'].strip() == '':
        values['gift_value'] = None

    if values['E_gift_value'].strip() == '':
        values['E_gift_value'] = None

    return values

def insert_gift(cursor, values):

    insert = '''INSERT INTO LegStaffGifts
                (year, staff_member,
                 position, source_name,
                 source_city, source_state,
                 source_business, date_given,
                 gift_value, reimbursed,
                 gift_description, speech_or_panel,
                 image_url)
                 VALUES
                 (%(year)s, %(staff_member)s,
                 %(position)s, %(source_name)s,
                 %(source_city)s, %(source_state)s,
                 %(source_business)s, %(date_given)s,
                 %(gift_value)s, %(reimbursed)s,
                 %(gift_description)s, %(speech_or_panel)s,
                 %(image_url)s)'''

    cursor.execute(insert, values)



def main():
    count = 0
    with open('GiftDataConcatenated.csv') as f:
        with pymysql.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                             port=3306,
                             db='AndrewTest',
                             # db='DDDB2015Dec',
                             user='awsDB',
                             passwd='digitaldemocracy789') as cursor:
            reader = csv.reader(f)
            all_staff = fetch_staff(cursor)
            leg_personnel = fetch_leg_personnel(cursor)
            previous_date = None

            first = True
            # each row contains two entries
            missed_staff = set()
            saved_rows = []
            for row in reader:
                count += 1
                if first:
                    first = False
                else:
                    # first one gets the first entries
                    values = clean_values(assign_values(row, previous_date))
                    previous_date = values['date_given']

                    #TODO make this clever
                    get_staff_member(all_staff, values, count, missed_staff)
                    get_legislator(leg_personnel, values, count)

                    if not values['staff_member']:
                        match_missed_staff(cursor, values, all_staff)

                    # Used for building your new csv
                    saved_rows.append(values)

                    # First option: One entry in this row
                    if values['gift_value'] and not values['E_gift_value']:

                        subset_values = {k: values[k] for k in ('year', 'staff_member', 'legislator',
                                                                'position', 'source_name', 'source_city',
                                                                'source_state', 'source_business', 'date_given',
                                                                'gift_value', 'reimbursed', 'gift_description',
                                                                'speech_or_panel', 'image_url')}
                        subset_values['schedule'] = 'D'
                        insert_gift(cursor, subset_values)

                    # Two entries in this row
                    elif values['gift_value'] and values['E_gift_value']:

                        subset_values = {k: values[k] for k in ('year', 'staff_member', 'legislator',
                                                                'position', 'source_name', 'source_city',
                                                                'source_state', 'source_business', 'date_given',
                                                                'gift_value', 'reimbursed', 'gift_description',
                                                                'speech_or_panel', 'image_url')}
                        subset_values['schedule'] = 'D'
                        insert_gift(cursor, subset_values)

                        subset_values = {drop_E(k): values[k] for k in ('year', 'staff_member', 'legislator',
                                                                'position', 'E_source_name', 'E_source_city',
                                                                'E_source_state', 'E_source_business', 'E_date_given',
                                                                'E_gift_value', 'reimbursed', 'E_gift_description',
                                                                'speech_or_panel', 'image_url')}
                        subset_values['schedule'] = 'E'
                        insert_gift(cursor, subset_values)

        print("Number of tuples seen: ", count)
        # with open('AppendedGiftData.csv', 'w') as out_f:
        #     first = True
        #     for row in saved_rows:
        #         if first:
        #             first = False
        #             out_f.write(','.join(row.keys()))
        #         out_f.write(','.join(row.values()))
        with open('SavedRowsNew.p', 'wb') as out_f:
            pickle.dump(saved_rows, out_f)

        with open('MissedStaff.p', 'wb') as out_f:
            pickle.dump(missed_staff, out_f)

if __name__ == '__main__':
    main()



