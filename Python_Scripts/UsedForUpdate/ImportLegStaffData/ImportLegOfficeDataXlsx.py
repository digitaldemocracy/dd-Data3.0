'''
File: ImportLegStaffInfo.py
Author: Andrew Voorhees
Date: 3/27/2016
Description:
- Goes through the file old staff directories and places the data into DDDB2015Dec
- Fills table LegislativeStaff

Sources:
- Old staff directory excel files originally provided by Christine
'''


import openpyxl as xl
import re
import pymysql
from unidecode import unidecode
# from unicodedata import normalize
from datetime import date
import os, os.path

DATA_DIR = 'StaffData'
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


def clean_name(name):
    return unidecode(name).strip().replace(',', '')


def match_term_year(year):
    if year % 2 == 0:
        return year - 1
    return year


def concat_names(first, middle, last):
    return first + (' ' + middle + ' ' if middle else ' ') + last


def map_party(party):
    if party.lower() == 'd':
        return 'Democrat'
    if party.lower() == 'r':
        return 'Republican'
    if party.lower() == 'i':
        return 'Other'
    print("Party: {} from spreadsheet didn't match expected input".format(party))
    return None


# Gets the house, year, and actual date associated with the info in the file
def scrape_file_info(file_name):
    file_info = {}
    match = re.match(r"CA (\w+) (\d{1,2})-(\d{1,2})-(\d{2})", file_name)
    file_info['house'] = match.group(1)
    file_info['date'] = date(int(match.group(4)) + YEAR_OFFSET, int(match.group(2)), int(match.group(3)))

    return file_info


# Gets the legislator and term info for every leg currently in the db
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
             house,
             party): pid for pid, first, middle, last, year, house, party in cursor}

# Inserts a new legislator into both Legislator and Person
# Returns the pid of the newly inserted leg
def insert_new_leg(cursor, first, middle, last):

    middle = None
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


# updates the legs in the db to reflect any new ones found in the file
def get_leg_pid(cursor, first, last, house, year, party, district, db_term_info, db_leg_info):
    first, last = remap_leg_names(first, last)
    full_name = concat_names(first, None, last)

    year = match_term_year(year)

    # check if the legislator already has a corresponding term, if not insert it
    try:
       pid = db_term_info[(full_name, year, house, party)]
    # if they did't have the term in the db
    except KeyError:
       # check if the leg even exists in the db at this point, if not inserts it
       try:
           pid = db_leg_info[full_name]
       except KeyError:
           # This is for debugging
           if year == 15:
               print("Couldn't find {} in db".format(full_name))
           pid = insert_new_leg(cursor, first, last)
     # Inserts the term if it didn't exist
        # This check is hacky and disgusting, but I don't want to ping the db again because it
        # takes forever
       if (full_name, year, house, 'Republican') not in db_term_info and \
            (full_name, year, house, 'Democrat') not in db_term_info:
          insert_term(cursor, pid, house, year, party, district)

    return pid


# parses out the staff names from the cell in the excel file
def clean_staff_name(name):
    try:
        match = re.match(r".+?:(.+?),\s*([\w-]+)\s*\(?", name)
        # output as first, middle, last
        return unidecode(match.group(2)), None, unidecode(match.group(1))
    except AttributeError:
        try:
            # second match object is used to catch the middle name issue
            match = re.match(r".+?:(.+?),\s*(.+?),\s*([\w-]+)\s*\(?", name)
            # output as first, middle, last
            return unidecode(match.group(2)), unidecode(match.group(3)), unidecode(match.group(1))
        except AttributeError:
            print('Issue parsing the following staff member')
            print('name', name)


def get_staff_names(cursor, file_name, file_date, house, db_term_info):
    wb = xl.load_workbook(file_name, read_only=True)
    ws = wb[SHEET]

    leg_staff_dict = {}

    first = True
    for row in ws.rows:
        if first:
            first = False
            header = [cell.value for cell in row]
        else:
            values = [cell.value for cell in row]
            col_dict = {key:value for key, value in zip(header, values)}
            leg_last = clean_name(col_dict['Lname'])
            leg_first = clean_name(col_dict['Fname'])
            party = map_party(col_dict['Party'])
            try:
                if 'Dist_Number' in col_dict:
                    district = int(col_dict['Dist_Number'])
                else:
                    district = int(col_dict['enat'])
                year = file_date.year
            except TypeError:
                district = None

            # Heuristic for picking out the office rows
            if len(leg_first.split(' ')) <= 2 and 'District' not in leg_first:

                staff = col_dict['Staff'].split(';') if col_dict['Staff'] else []
                dist_staff = col_dict['Dist_Staff'].split(';') if col_dict['Dist_Staff'] else []
                dist_staff2 = col_dict['Dist_Staff2'].split(';') if col_dict['Dist_Staff2'] else []
                dist_staff3 = col_dict['Dist_Staff3'].split(';') if col_dict['Dist_Staff3'] else []
                dist_staff4 = col_dict['Dist_Staff4'].split(';') if col_dict['Dist_Staff4'] else []

                all_staff = staff + dist_staff + dist_staff2 + dist_staff3 + dist_staff4
                # names follow first, middle, last ordering
                all_staff = set(clean_staff_name(name) for name in all_staff if name.strip() != '')
                db_leg_info = {key[0]: pid for key, pid in db_term_info.items()}
                leg_pid = get_leg_pid(cursor, leg_first, leg_last, house, year, party, district, db_term_info,
                                      db_leg_info)
                leg_staff_dict[leg_pid] = all_staff

            else:
                print("Ignoring {} {} in file {}".format(leg_first, leg_last, file_name))

    return leg_staff_dict


# Matches to a legislator in the db based on their last name and returns their pid
def fuzzy_match_leg(db_legs, leg_name):
    last_name = leg_name.split(' ')[-1]
    matched_pid = None
    for leg, pid in db_legs.items():
        if last_name == leg.split(' ')[-1]:
            # Makes sure you haven't already matched another legislator
            assert not matched_pid, 'Unmatched legislator name: %s from file matched multiple legislators in db' % leg_name
            matched_pid = pid

    assert matched_pid, 'Legislator name: %s from file, failed to match any in db' % leg_name
    return matched_pid


# lowercase-a-fies every each name and returns the tuple
def normalize_staff_name(first, middle, last):
    return first.lower(), middle.lower() if middle else None, last.lower()


# adds person if they're not in this table
def get_staff_pid(cursor, staff, existing_staff):
    first = staff[0]
    middle = staff[1]
    last = staff[2]
    normed_name = normalize_staff_name(first, middle, last)

    if normed_name in existing_staff:
        pid = existing_staff[normed_name]

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

        # adds them to the existing staff dictionary so you
        # only need to query the db once
        existing_staff[normed_name] = pid

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
    return {normalize_staff_name(first, middle, last) : pid for pid, first, middle, last in cursor}


def get_current_LegOfficePersonnnel(cursor):
    query = '''
            SELECT staff_member, legislator, term_year, house
            FROM LegOfficePersonnel
            '''
    cursor.execute(query)
    return [{'staff' : staff, 'leg' : leg, 'term' : term, 'house' : house} for staff, leg, term, house in cursor]


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

        # actually inserst the new tuple
        insert_stmt = '''INSERT INTO LegOfficePersonnel
                         (staff_member, legislator, term_year, house, start_date, state)
                         VALUES
                         (%s, %s, %s, %s, %s, "CA")'''
        # Note that 15 gets auto-converted to 2015 by mysql
        cursor.execute(insert_stmt, (staff_pid, leg_pid, match_term_year(file_date.year), house, "{}-{}-{}".format(
            file_date.year, file_date.month, file_date.day)))


# Returns the files ordered by their date in ascending order
def order_files_by_date(files):
    new_list = []
    for f in files:
        date = scrape_file_info(f)['date']
        new_list.append((f, date))

    new_list = sorted(new_list, key=lambda x: x[1])
    return (tup[0] for tup in new_list)


def main():
    with pymysql.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='AndrewTest2',
                         # db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789') as dd_cursor:

        for root, _, files in os.walk(DATA_DIR):
            ordered_files = order_files_by_date(files)
            existing_staff = get_existing_staff(dd_cursor)

            for f in ordered_files:
                full_path = os.path.join(root, f)

                file_info = scrape_file_info(f)
                # Gets all the legislators that currently exist in the db as full name -> pid
                db_term_info = get_db_leg_term_info(dd_cursor)
                # Reads the names of all the staff for each leg in the excel file as full name -> staff list
                staff_dict = get_staff_names(dd_cursor, full_path, file_info['date'], file_info['house'], db_term_info)

                # ensures that every legislator in the file got matched
                # assert len(staff_dict) == len(leg_staff_dict), 'Not every row was matched to a legislator'
                # print('staff_dict', len(staff_dict), 'leg_staff_dict', len(leg_staff_dict))
                print('staff dict', len(staff_dict))

                # The hope here is that the same staff doesn't appear twice in a single sheet
                lop_info = get_current_LegOfficePersonnnel(dd_cursor)
                lop_pks = set((row['staff'], row['leg'], row['term'], row['house']) for row in lop_info)
                # lop_staff_pids = set(staff for staff, leg, term, house in lop_pks)

                for leg_pid, staff_mems in staff_dict.items():
                    for staff in staff_mems:
                        staff_pid = get_staff_pid(dd_cursor, staff, existing_staff)
                        add_to_LegOfficePersonnel(dd_cursor, staff_pid, leg_pid, file_info['house'], file_info['date'],
                                                  lop_pks)

if __name__ == '__main__':
    main()
