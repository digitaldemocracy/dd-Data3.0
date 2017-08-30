'''
File: ImportLegStaffDirectoriesAlternative.py
Author: Andrew Voorhees
Date: 7/7/2016
Description:
- Goes through the file old staff directories and places the data into DDDB2015Dec
- Fills tables LegislativeStaff, LegOfficePersonnel

Sources:
- Old staff directory excel files originally provided by Christine
'''



import openpyxl as xl
import re
import pymysql
from unidecode import unidecode
from datetime import date
import os, os.path

DATA_DIR = 'AltStaffData'
# DATA_DIR = 'TempData'
YEAR_OFFSET = 2000
SHEET = 'Sheet1'


def remap_leg_names(first, last):
    if first == 'Ben' and last.lower() == 'allen':
        first = 'Benjamin'
    if first == 'Patricia':
        first = 'Pat'
    if first == 'Steven':
        first = 'Steve'
    if last == "Mcguire":
        last = "McGuire"
    if last == "de Leon":
        last = "De Leon"
    if first == "William" and last == "Monning":
        first = "Bill"
    if first == "Katcho":
        first = 'K.H. "Katcho"'
    if first == "Ian" and last == "Calderon":
        first = "Ian Charles"
    if first == "Ling-Ling":
        first = "Ling Ling"
    if first.lower() == "matt" and last.lower() == "dababneh":
        first = "Matthew"

    return first, last


# Strips white-space, decodes, removes unwanted characters
def clean_name(name):
    return unidecode(name).strip().replace(',', '')


def match_term_year(year):
    if year % 2 == 0:
        return year - 1
    return year


def concat_names(first, middle, last):
    return first + (' ' + middle + ' ' if middle else ' ') + last


def scrape_file_date(file_name):
    file_info = {}
    match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2})", file_name)
    return date(int(match.group(3)) + YEAR_OFFSET, int(match.group(1)), int(match.group(2)))


def get_db_leg_term_info(cursor):
    query = '''SELECT p.pid, p.first, p.middle, p.last, t.year, t.house, t.party
               FROM Legislator l
                    JOIN Person p
                    ON l.pid = p.pid
                    JOIN Term t
                    ON l.pid = t.pid
                WHERE l.state = "CA" '''
    cursor.execute(query)
    return {(concat_names(first, middle, last),
             year,
             house): pid for pid, first, middle, last, year, house, party in cursor}


def match_house(house_value):
    match = re.search(r"Senate|Assembly", house_value)
    return match.group()


# Inserts a new legislator into both Legislator and Person
# Returns the pid of the newly inserted leg
def insert_new_leg(cursor, first, last):

    middle = None
    split_first = first.split(' ')
    if len(split_first) >= 2:
        first = split_first[0]
        middle = ' '.join(split_first[1:])

    insert_person_stmt = '''
                            INSERT INTO Person
                            (first, middle, last)
                            VALUES
                            (%s, %s, %s)
                            '''
    cursor.execute(insert_person_stmt, (first, middle, last))

    pid = cursor.lastrowid
    insert_leg_stmt = '''
                    INSERT INTO Legislator
                    (pid, state)
                    VALUES
                    (%s, "CA")
                    '''
    cursor.execute(insert_leg_stmt, pid)

    return pid


def insert_term(cursor, pid, house, year, party, district):
    insert_term_stmt = """INSERT INTO Term
                          (pid, year, house, party, state, district)
                          VALUES
                          (%s, %s, %s, %s, "CA", %s)"""
    cursor.execute(insert_term_stmt, (pid, year, house, party, district))


def get_leg_pid(cursor, first, last, house, year, district, db_term_info):

    db_leg_info = {key[0]: pid for key, pid in db_term_info.items()}

    first, last = remap_leg_names(first, last)
    full_name = concat_names(first, None, last)

    year = match_term_year(year)

    # check if the legislator already has a corresponding term, if not insert it
    try:
        pid = db_term_info[(full_name, year, house)]
        # if they did't have the term in the db
    except KeyError:
        # check if the leg even exists in the db at this point, if not inserts it
        try:
            pid = db_leg_info[full_name]
        except KeyError:
            # I don't think this should ever run
            # This is for debugging
            if year == 15:
                print("Couldn't find {} in db".format(full_name))
            assert year < 2011
            pid = insert_new_leg(cursor, first, last)
            db_leg_info[full_name] = pid
            # Inserts the term if it didn't exist
        if (full_name, year, house) not in db_term_info:
            party = None
            insert_term(cursor, pid, house, year, party, district)
            db_term_info[(full_name, year, house)] = pid

    return pid


def get_existing_staff(cursor):
    query = '''
            SELECT p.pid, p.first, p.middle, p.last
            FROM LegislativeStaff ls
                JOIN Person p
                ON ls.pid = p.pid
            WHERE state = 'CA'
            '''
    cursor.execute(query)
    return {normalize_staff_name(first, middle, last): pid for pid, first, middle, last in cursor}


def get_current_LegOfficePersonnnel(cursor):
    query = '''
            SELECT staff_member, legislator, term_year, house
            FROM LegOfficePersonnel
            '''
    cursor.execute(query)
    return [{'staff': staff, 'leg': leg, 'term': term, 'house': house} for staff, leg, term, house in cursor]


# adds person if they're not in this table
def get_staff_pid(cursor, staff, existing_staff):
    first = staff[0]
    middle = staff[1]
    last = staff[2]

    if (first, middle, last) in existing_staff:
        pid = existing_staff[(first, middle, last)]

    else:
        # print("Please don't run")
        # print('Adding staff', staff)
        insert_person_stmt = '''
                            INSERT INTO Person
                            (first, middle, last)
                            VALUES
                            (%s, %s, %s)
                            '''
        cursor.execute(insert_person_stmt, (first, middle, last))

        pid = cursor.lastrowid
        insert_staff_stmt = '''
                    INSERT INTO LegislativeStaff
                    (pid, flag, state)
                    VALUES
                    (%s, 0, "CA")
                    '''
        cursor.execute(insert_staff_stmt, pid)

    return pid


# adds person if they're not in this table
def get_staff_pid(cursor, staff, existing_staff):
    first = staff[0]
    middle = staff[1]
    last = staff[2]

    if normalize_staff_name(first, middle, last) in existing_staff:
        pid = existing_staff[normalize_staff_name(first, middle, last)]

    else:
        # print("Please don't run")
        # print('Adding staff', staff)
        insert_person_stmt = '''
                            INSERT INTO Person
                            (first, middle, last)
                            VALUES
                            (%s, %s, %s)
                            '''
        cursor.execute(insert_person_stmt, (first, middle, last))

        pid = cursor.lastrowid
        insert_staff_stmt = '''
                    INSERT INTO LegislativeStaff
                    (pid, flag, state)
                    VALUES
                    (%s, 0, "CA")
                    '''
        cursor.execute(insert_staff_stmt, pid)

        existing_staff[normalize_staff_name(first, middle, last)] = pid

    return pid


# only adds staff if the term hasn't already been found
def add_to_LegOfficePersonnel(cursor, staff_pid, leg_pid, house, file_date, lop_pks):

    term_year = match_term_year(file_date.year)

    # If this staff, leg combo isn't in the db, add a new tuple
    if (staff_pid, leg_pid, term_year, house) not in lop_pks:

        # updates exists end_dates
        update_stmt = """UPDATE LegOfficePersonnel
                         SET end_date = %s
                         WHERE staff_member = %s
                            AND end_date IS NULL"""
        cursor.execute(update_stmt, ("{}-{}-{}".format(file_date.year, file_date.month, file_date.day),
                                     staff_pid))

        # actually insert the new tuple
        insert_stmt = '''INSERT INTO LegOfficePersonnel
                         (staff_member, legislator, term_year, house, start_date, state)
                         VALUES
                         (%s, %s, %s, %s, %s, "CA")'''
        # Note that 15 gets auto-converted to 2015 by mysql
        cursor.execute(insert_stmt, (staff_pid, leg_pid, match_term_year(file_date.year), house, "{}-{}-{}".format(
            file_date.year, file_date.month, file_date.day)))
        lop_pks.add((staff_pid, leg_pid, term_year, house))


# lowercase-a-fies every each name and returns the tuple
def normalize_staff_names(first, middle, last):
    return first.lower(), middle.lower() if middle else None, last.lower()


# reads in all staff and associates them with a legislator. Inserts unseen staff into the db
def read_staff(cursor, file_name, file_date, db_term_info):
    wb = xl.load_workbook(file_name, read_only=True)
    ws = wb[SHEET]

    existing_staff = get_existing_staff(cursor)
    lop_info = get_current_LegOfficePersonnnel(cursor)
    lop_pks = set((row['staff'], row['leg'], row['term'], row['house']) for row in lop_info)

    leg_staff_dict = {}

    first = True
    for row in ws.rows:
        if first:
            first = False
            header = [cell.value for cell in row]
        else:
            values = [cell.value for cell in row]
            # this lets you ignore that weird blank row
            if len(set(values)) > 2:
                col_dict = {key: value for key, value in zip(header, values)}
                staff_last = clean_name(col_dict['Last Name'])
                staff_first = clean_name(col_dict['First Name'])
                staff = (staff_first, None, staff_last)
                leg_last = clean_name(col_dict['Leg Last name'])
                leg_first = clean_name(col_dict['Leg First name'])
                house = match_house(col_dict['Leg Type']).strip()
                district = int(col_dict['District No'])
                # party = map_party(col_dict['Party'])
                year = file_date.year

                # Check to ensure that the row actually is a legislator
                if 'district' not in leg_first.lower() and 'district' not in leg_last.lower():
                    leg_pid = get_leg_pid(cursor, leg_first, leg_last, house, year, district, db_term_info)
                    staff_pid = get_staff_pid(cursor, staff, existing_staff)
                    add_to_LegOfficePersonnel(cursor, staff_pid, leg_pid, house, file_date,
                                              lop_pks)


# Returns the files ordered by their date in ascending order
def order_files_by_date(files):
    new_list = []
    for f in files:
        date = scrape_file_date(f)
        new_list.append((f, date))

    new_list = sorted(new_list, key=lambda x: x[1])
    return (tup[0] for tup in new_list)


def main():
    with pymysql.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='AndrewTest',
                         # db='AndrewTest',
                         # db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789') as cursor:

        for root, _, files in os.walk(DATA_DIR):

            ordered_files = order_files_by_date(files)
            for f in ordered_files:
                full_path = os.path.join(root, f)

                file_date = scrape_file_date(f)

                # Gets all the legislators that currently exist in the db as full name -> pid
                db_term_info = get_db_leg_term_info(cursor)
                # Reads the names of all the staff for each leg in the excel file as full name -> staff list
                read_staff(cursor, full_path, file_date, db_term_info)

if __name__ == '__main__':
    main()