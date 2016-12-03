import os, os.path
import pickle
import pymysql
import csv
import datetime
import pandas as pd
from dateutil.relativedelta import relativedelta
import re
from unidecode import unidecode

from ScrapeLegFunctions import LEG_DATA_DIR
from ScrapeLegFunctions import order_files_by_date
from ScrapeLegFunctions import fetch_db_leg_term_info
from ScrapeLegFunctions import scrape_file_info
from ScrapeLegFunctions import scrape_legislators
from ScrapeLegFunctions import match_term_year
from ScrapeLegFunctions import cmp_dicts
from MatchingFunctions import norm_names
from MatchingFunctions import cmp_names

STAFF_DATA_DIR = 'BestStaffData'

CONN_INFO = {'host': 'digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             # 'db': 'AndrewTest',
             'db': 'DDDB2015Dec',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

LOP_COLUMNS = ['first',
               'middle',
               'last',
               'hire_date',
               'file_date',
               'term_year',
               'house',
               'pid',
               'row',
               'file',
               'assoc_type',
               'assoc',
               'office',
               'lo_id',
               'leg_pid',
               'leg_first',
               'leg_middle',
               'leg_last',
               'end_date',
               'last_seen',
               'file_appearances',
               'set_at_end']

STAFF_STINT_ASSUMPTION = relativedelta(months=6)

SEN_PRES_PRO_TEMP = {2011: 'darrell steinberg',
                     2013: 'kevin de leon',
                     2015: 'kevin de leon'}

SEN_DEM_FLOOR_LEAD = {2011: 'ellen corbett',
                      2013: 'ellen corbett',
                      2015: 'bill monning'}

SEN_DEM_CAUC_CHAIR = {2011: 'kevin de leon',
                      2013: 'jerry hill',
                      2015: 'connie leyva'}

SEN_REP_FLOOR_LEAD = {2011: 'bob huff',
                      2013: 'bob huff',
                      2015: 'jean fuller'}


SEN_REP_CAUC_CHAIR = {2011: 'tom harman',
                      2013: 'ted gaines',
                      2015: 'tom berryhill'}

SEN_DEM_WHIP = {2015: 'lois wolk'}

SEN_REP_WHIP = {2015: 'ted gaines'}

g_missed_staff = []


# Remaps the leg last name for better matching, eg quirk -> quirk-silva
def remap_leg_names_pdfs(first, middle, last):
    if first == 'ed' and middle == 'hernandez' and last == 'o.d.':
        first, middle, last = 'ed', None, 'hernandez'
    if first == 'curren' and middle == 'd.' and last == 'price,':
        first, middle, last = 'curren', 'd.', 'price'
    if first == 'kevin' and middle == 'de' and last == 'lea3n':
        first, middle, last = 'kevin', 'de', 'leon'
    return first, middle, last


# Scrapes the legs and inserts them into the db
# Returns: leg_term_infos
def scrape_legs_wrapper(cursor):

    db_leg_term_infos = []
    for root, _, files in os.walk(LEG_DATA_DIR):
        ordered_files = order_files_by_date(files)

        db_leg_term_infos = fetch_db_leg_term_info(cursor)

        for f in ordered_files:
            full_path = os.path.join(root, f)
            file_info = scrape_file_info(f)
            # Gets all the legislators and their info and fills db_leg_term_infos
            scrape_legislators(cursor, full_path, file_info['date'], file_info['house'],
                               db_leg_term_infos)
    return db_leg_term_infos


# Gets all current staff from the db
# Returns: Dictionary of all the staff (first, middle, last): pid
def fetch_existing_staff(cursor):
    query = '''
            SELECT p.pid, p.first, p.middle, p.last
            FROM LegislativeStaff ls
                JOIN Person p
                ON ls.pid = p.pid
            WHERE state = 'CA'
            '''
    cursor.execute(query)
    out = []
    for pid, first, middle, last in cursor:
        first, middle, last = norm_names(first, middle, last)
        out.append({'first': first, 'middle': middle, 'last': last, 'pid': pid})

    return pd.DataFrame(out)


# Just attaches the header information to each row and outputs it as a dictionary
# Returns: dictionary of row attributes, None if header row
def add_headers_assembly(row):
    # makes sure this isn't a header row
    header = ['Employee', 'assoc', 'Hire Date', 'Classification', 'Salary']
    # This is for 'AssemblyStaff 11-30-11.csv' because seriously, fuck that file
    if len(row) == 4:
        row.append('')
    if len(header) != len(row):
        return None
    return {k: v for k, v in zip(header, row)}


# Wrapper function for determining which kind of file this is and how to handle those rows
def add_headers_generic(row, house):

    row = [e for e in row if e != '']
    if len(row) == 0:
        row_dict = None
    elif row[0].lower() == 'employee':
        row_dict = None
    elif house == 'Senate':
        row_dict = add_headers_senate(row)
    elif house == 'Assembly':
        row_dict = add_headers_assembly(row)
    else:
        assert False

    return row_dict


# Returns: dictionary of row attributes, None if header row
def add_headers_senate(row):
    # makes sure this isn't a header row
    header = ['Employee', 'assoc', 'Classification', 'Hire Date', 'Salary', 'Grant', 'Salary_2']
    if len(header) != len(row):
        return None
    return {k: v for k, v in zip(header, row)}


# Gets the associated legislator or office for this row
# Returns: ('office'/'leg', name), first value is type
def parse_association(org, db_leg_term_df, house, term_year):
    type = None
    assoc = None
    if house == 'Senate':
        if 'senator ' in org.lower():
            type = 'leg'
            assoc = org.lower().replace('senator ', '').strip()
        elif 'pro temp' in org.lower():
            type = 'leg'
            assoc = SEN_PRES_PRO_TEMP[term_year]
        elif 'democratic floor leader' in org.lower():
            type = 'leg'
            assoc = SEN_DEM_FLOOR_LEAD[term_year]
        elif 'republican floor leader' in org.lower():
            type = 'leg'
            assoc = SEN_REP_FLOOR_LEAD[term_year]
        elif 'democratic caucus' in org.lower():
            type = 'leg'
            assoc = SEN_DEM_CAUC_CHAIR[term_year]
        elif 'republican caucus' in org.lower():
            type = 'leg'
            assoc = SEN_REP_CAUC_CHAIR[term_year]
        elif 'majority whip' in org.lower():
            type = 'leg'
            assoc = SEN_DEM_WHIP[term_year]
        elif 'minority whip' in org.lower():
            type = 'leg'
            assoc = SEN_REP_WHIP[term_year]

    if house == 'Assembly':
        potential_leg = org.split(',')[0]
        potential_leg = norm_names(None, None, potential_leg)[2]
        # potential_leg = remap_leg_last_name(potential_leg)
        if potential_leg in db_leg_term_df['last'].str.lower().values:
            type = 'leg'
            assoc = potential_leg
    if not type:
        type = 'office'
        assoc = org.lower().strip()
    return type, assoc


# parses the date given m/d/year format
# Returns python date. None if date cannot be parsed
def parse_date(date_str):
    try:
        month, day, year = date_str.split("/")
        year, month, day = int(year), int(month), int(day)
        if year < 25:
            year += 2000
        elif year < 100:
            year += 1900

        assert year > 1940, 'Who the hell wrote this year?'

        return datetime.date(year, month, day)
    except ValueError:
        return None


# Gets the pid of existing staff member or inserts a new one if that staff member could not be found
# Returns: pid of the staff member
def get_staff_pid(cursor, first, middle, last, existing_staff_df):

    assert (first.lower() == first and last.lower() == last), 'Staff names not lowercase-afied'

    matched_staff_df = existing_staff_df.loc[(existing_staff_df['first'] == first) &
                                             ((existing_staff_df.middle == middle) |
                                                (pd.isnull(existing_staff_df.middle) & (not middle))
                                              ) &
                                             (existing_staff_df['last'] == last)]

    assert matched_staff_df.shape[0] < 2, 'Matched too many staff names somehow'
    if matched_staff_df.shape[0] == 1:
        pid = matched_staff_df.iloc[0]['pid']

    else:
        insert_person_stmt = '''
                            INSERT INTO Person
                            (first, middle, last)
                            VALUES
                            (%s, %s, %s)
                            '''
        title_middle = middle.title() if middle else None
        cursor.execute(insert_person_stmt, (first.title(), title_middle, last.title()))

        pid = cursor.lastrowid
        insert_staff_stmt = '''INSERT INTO LegislativeStaff
                               (pid, state)
                               VALUES
                               (%s, "CA")'''
        cursor.execute(insert_staff_stmt, pid)

        new_row = pd.Series([pid, first, middle, last], ['pid', 'first', 'middle', 'last'])
        existing_staff_df.loc[len(existing_staff_df.index)] = new_row

    return pid


# Matches the term of a legislator based on the given inputs
# Returns: True is the criteria for a match are met
def match_leg_term_senate(leg_term_row, first=None, middle=None, last=None):
    print(type(leg_term_row))
    print(leg_term_row)
    # cmp_keys = ('term_year', 'house')
    name_match = cmp_names(first, middle, last, leg_term_row['first'], leg_term_row['middle'],
                           leg_term_row['last'])
    # return name_match and cmp_dicts(leg_term_row, staff_info, cmp_keys)
    return name_match




# Handles the case where the file is an senate file. Called in get_leg_pid
def get_leg_pid_senate(staff_info, db_leg_term_df):
    file_leg_name = staff_info['assoc'].lower().replace('senator', '').strip()
    names = file_leg_name.split(' ', 3)
    # removes the empty names that might appear
    names = [name for name in names if name.strip() != '']
    if len(names) == 4:
        if len(names[1]) < len(names[3]) and len(names[2]) < len(names[3]):
            first, middle, last = norm_names(names[0], names[1] + ' ' + names[2], names[3])
        else:
            first, middle, last = norm_names(names[0], names[1], names[2])
    elif len(names) == 3:
        first, middle, last = norm_names(names[0], names[1], names[2])
    elif len(names) == 2:
        first, middle, last = norm_names(names[0], None, names[1])
    else:
        assert False, 'Parsed too many names for leg'

    first, middle, last = remap_leg_names_pdfs(first, middle, last)

    matched_leg_terms_df = db_leg_term_df.loc[(db_leg_term_df.apply(lambda row: cmp_names(row['first'],
                                                    row['middle'], row['last'], first, middle, last), axis=1))&
                                              (db_leg_term_df.term_year == staff_info['term_year']) &
                                              (db_leg_term_df.house == staff_info['house']), ]
    assert len(matched_leg_terms_df.index) == 1, 'Matched improper number of legislators'
    leg_term_row = matched_leg_terms_df.iloc[0]

    staff_info['leg_pid'] = leg_term_row['pid']
    staff_info['leg_first'] = leg_term_row['first']
    staff_info['leg_middle'] = leg_term_row['middle']
    staff_info['leg_last'] = leg_term_row['last']

    # gross
    return True


# Matches the term of a legislator based on the given inputs
# Returns: True is the criteria for a match are met
def match_leg_term_assembly(leg_term_row, staff_info):
    cmp_keys = ('term_year', 'house')
    # return (leg_term_row['last'].lower() == potential_leg_last) and cmp_dicts(staff_info, leg_term_row, cmp_keys)
    return cmp_dicts(staff_info, leg_term_row, cmp_keys)


# Attempts to match the provided staff name to a list of staff names from the db
# Returns: The matched name in the other directory as tuple (first, middle, last)
def match_other_dir_staff(first, middle, last, dir_staff_names):
    found = None
    first, middle, last = norm_names(first, middle, last)
    for cmp_staff_name in dir_staff_names:

        if cmp_names(first, middle, last, cmp_staff_name[0], cmp_staff_name[1], cmp_staff_name[2]):
            assert not found, 'Staff found twice in list'
            found = cmp_staff_name[0], cmp_staff_name[1], cmp_staff_name[2]

    return found


# Handles the case where the file is an assembly file. Called in get_leg_pid
def get_leg_pid_assembly(staff_info, db_leg_term_df):
    potential_leg_last = staff_info['assoc']

    success = True

    # matched_leg_terms_df = db_leg_term_df.loc[db_leg_term_df.apply(partial(match_leg_term_assembly,
    #                                                                        potential_leg_last, staff_info))]
    matched_leg_terms_df = db_leg_term_df.loc[(db_leg_term_df.term_year == staff_info['term_year']) &
                                              (db_leg_term_df.house == staff_info['house']) &
                                              (db_leg_term_df['last'].str.lower() == potential_leg_last)]

    # structured this way because you're assuming that assertion will eventually break
    if len(matched_leg_terms_df.index) > 1:
        new_matched_leg_terms_df = pd.DataFrame(columns=matched_leg_terms_df.columns)
        for idx, leg_term_row in matched_leg_terms_df.iterrows():
            found = None
            dir_staff = leg_term_row['staff_set']
            found = match_other_dir_staff(staff_info['first'], staff_info['middle'], staff_info['last'], dir_staff)
            if found:
                new_matched_leg_terms_df.loc[len(new_matched_leg_terms_df.index)] = leg_term_row
                # print(staff_info['first'], staff_info['middle'], staff_info['last'], ' --> ', found)

                # Okay okay so there's a Quirk and a Quirk-Silva in 2015 senate, this is not ideal


        # This is for debugging purposes, you can look through these later
        if len(new_matched_leg_terms_df.index) == 0:
            g_missed_staff.append(staff_info)
            success = False
        assert len(new_matched_leg_terms_df.index) < 2, 'Multiple legs matched in staff dir'
        matched_leg_terms_df = new_matched_leg_terms_df

    # Makes sure we only found one leg
    if success:
        assert len(matched_leg_terms_df) == 1, 'Multiple legs matched'
        leg_term_info = matched_leg_terms_df.iloc[0]

        staff_info['leg_pid'] = leg_term_info['pid']
        staff_info['leg_first'] = leg_term_info['first']
        staff_info['leg_middle'] = leg_term_info['middle']
        staff_info['leg_last'] = leg_term_info['last']

    return success


# Loops through leg_term information to assign the staff member to the appropriate legislator
# Returns: True if found, false otherwise, modifies staff_info dictionary passed in
def get_leg_pid(staff_info, db_leg_term_infos):

    if staff_info['house'] == 'Senate':
        return get_leg_pid_senate(staff_info, db_leg_term_infos)
    elif staff_info['house'] == 'Assembly':
        return get_leg_pid_assembly(staff_info, db_leg_term_infos)
    assert False

# # Updates the hired date for your current staff member. Basically if they match a leg from a
# # previous term and overlap that, you set the date hired to the beginning of the term
# # Also updates the hired date to the beginning of the term year if they have an entry in the previous term
# # and the file date is February
# # Note: This doesn't need to touch the database because you're fixing the date hired prior
# # to ever inserting the staff
# # Returns: Nothing, just updates staff_info
# def update_date_hired(staff_info, leg_lop_df):
#     if staff_info['hire_date'].year < staff_info['term_year']:
#         matched_rows_df = leg_lop_df[(leg_lop_df.pid == staff_info['pid']) &
#                                      (leg_lop_df.leg_pid == staff_info['leg_pid']) &
#                                      (leg_lop_df.term_year < staff_info['term_year']) &
#                                      (leg_lop_df.house == staff_info['house']) &
#                                      (pd.isnull(leg_lop_df.end_date))]
#         assert len(matched_rows_df.index) < 2
#         if len(matched_rows_df.index) == 1:
#             staff_info['hire_date'] = datetime.date(staff_info['term_year'], 1, 1)


# Checks to see if this staff member is just starting with this house, or if this staff member is
# just switching houses.
# Returns: Nothing, sets the hire_date for staff_info
def set_hire_date(staff_info, leg_lop_df, hire_date):
    set_hire_date = None
    matched_rows_df = leg_lop_df[(leg_lop_df.pid == staff_info['pid']) &
                                 (leg_lop_df.house == staff_info['house'])]
    # Means this staff member is just starting with this house
    if len(matched_rows_df.index) == 0:
        set_hire_date = parse_date(hire_date)
        # Means the OCR got confused
        if not set_hire_date:
            set_hire_date = staff_info['file_date']
        if set_hire_date > staff_info['file_date']:
            set_hire_date = staff_info['file_date']
    # staff member has an existing entry with this house
    else:
        matched_rows_df = leg_lop_df[leg_lop_df.pid == staff_info['pid']].sort_values(['file_date', 'house'], \
                                                                                      ascending=[False, False])
        assert len(matched_rows_df.index)

        most_recent_row = matched_rows_df.iloc[0]
        if most_recent_row['house'] == staff_info['house'] and \
                        most_recent_row['leg_pid'] == staff_info['leg_pid']:
            set_hire_date = datetime.date(staff_info['term_year'], 1, 1)
        # We're just gonna bring all Februaries back to Jan 1st
        elif staff_info['file_date'].month == 1 and staff_info['file_date'].year == staff_info['term_year']:
            set_hire_date = datetime.date(staff_info['term_year'], 1, 1)
        # Every other scenario just uses the file_date
        else:
            set_hire_date = staff_info['file_date']

    assert set_hire_date
    assert not staff_info['hire_date']
    staff_info['hire_date'] = set_hire_date


# Inserts a new leg office personnel into the database and updates your version
# Returns: Nothing, updates leg_lop_df
def insert_new_lop(cursor, staff_info, leg_lop_df):

    insert_stmt = '''INSERT INTO LegOfficePersonnel
                     (staff_member, legislator, term_year, house, start_date, state)
                     VALUES
                     (%s, %s, %s, %s, %s, "CA")'''
    # Note that 15 gets auto-converted to 2015 by mysql
    cursor.execute(insert_stmt, (int(staff_info['pid']), int(staff_info['leg_pid']), staff_info['term_year'],
                                 staff_info['house'], str(staff_info['hire_date'])))

    rows = leg_lop_df.shape[0]
    leg_lop_df.loc[len(leg_lop_df.index)] = staff_info
    assert leg_lop_df.shape[0] == rows + 1, 'Failed to add a row'


# Inserts a new leg office personnel into the database and updates your version
# Returns: Nothing, updates leg_lop_df
def insert_new_op(cursor, staff_info, leg_lop_df):

    insert_stmt = '''INSERT INTO OfficePersonnel
                     (staff_member, office, start_date, state)
                     VALUES
                     (%s, %s, %s, "CA")'''
    # Note that 15 gets auto-converted to 2015 by mysql
    cursor.execute(insert_stmt, (int(staff_info['pid']), int(staff_info['lo_id']), str(staff_info['hire_date'])))
    # cursor.execute(insert_stmt, (staff_info['pid'], int(staff_info['lo_id'])))

    rows = leg_lop_df.shape[0]
    leg_lop_df.loc[len(leg_lop_df.index)] = staff_info
    assert leg_lop_df.shape[0] == rows + 1, 'Failed to add a row'


# Updates the state of LegOfficePersonnel in both the database and your in memory concept
# of it
# Returns: Nothing, updates the leg_lop_df by appending new row. *this step is done in a lower function
def update_leg_personnel(cursor, staff_info, leg_lop_df):

    row_idxr = ((leg_lop_df.pid == staff_info['pid']) &
               (leg_lop_df.leg_pid == staff_info['leg_pid']) &
               (leg_lop_df.term_year == staff_info['term_year']) &
               (leg_lop_df.house == staff_info['house']))
    matched_rows_df = leg_lop_df.loc[row_idxr]

    assert len(matched_rows_df.index) < 2, 'Matched too many rows'
    if len(matched_rows_df.index) == 0:
        insert_new_lop(cursor, staff_info, leg_lop_df)
        row_idxr = ((leg_lop_df.pid == staff_info['pid']) &
                    (leg_lop_df.leg_pid == staff_info['leg_pid']) &
                    (leg_lop_df.term_year == staff_info['term_year']) &
                    (leg_lop_df.house == staff_info['house']))
        matched_rows_df = leg_lop_df.loc[row_idxr]
    assert len(matched_rows_df.index) == 1, 'Why are you matching so many terms?'

    leg_lop_df.loc[row_idxr, 'last_seen'] = staff_info['file_date']
    # leg_lop_df.loc[row_idxr, 'file_appearances'] = leg_lop_df[row_idxr, 'file_appearances'].add(staff_info['file'])
    leg_lop_df.loc[row_idxr, 'file_appearances'] += ' ' + staff_info['file']



# Updates the state of OfficePersonnel in both the database and your in memory concept
# of it
# Returns: Nothing, updates the leg_lop_df by appending new row. *this step is done in a lower function
def update_office_personnel(cursor, staff_info, leg_lop_df):

    row_idxr = ((leg_lop_df.pid == staff_info['pid']) &
                (leg_lop_df.lo_id == staff_info['lo_id']) &
                (leg_lop_df.house == staff_info['house']) &
                (leg_lop_df.hire_date <= staff_info['file_date']) &
                pd.isnull(leg_lop_df.end_date))
    matched_rows_df = leg_lop_df.loc[row_idxr]
    assert len(matched_rows_df.index) < 2, 'Matched too many rows'

    if len(matched_rows_df.index) == 0:
        insert_new_op(cursor, staff_info, leg_lop_df)
        row_idxr = ((leg_lop_df.pid == staff_info['pid']) &
                    (leg_lop_df.lo_id == staff_info['lo_id']) &
                    (leg_lop_df.house == staff_info['house']) &
                    (leg_lop_df.hire_date <= staff_info['file_date']) &
                    pd.isnull(leg_lop_df.end_date))
        matched_rows_df = leg_lop_df.loc[row_idxr]

    assert len(matched_rows_df.index) == 1, 'Improper number of office associations matched'

    leg_lop_df.loc[row_idxr, 'last_seen'] = staff_info['file_date']
    # leg_lop_df.loc[row_idxr, 'file_appearances'] = leg_lop_df[row_idxr, 'file_appearances'].add(staff_info['file'])
    leg_lop_df.loc[row_idxr, 'file_appearances'] += ' ' + staff_info['file']


# Inserts a new record into the office table
# Returns: Nothing, updates office_info
def insert_new_office(cursor, staff_info, offices_df):
    insert_stmt = """INSERT INTO LegislatureOffice
                     (name, house, state)
                     VALUES
                     (%s, %s, 'CA')"""
    cursor.execute(insert_stmt, (staff_info['office'].title(), staff_info['house']))
    lo_id = cursor.lastrowid
    size = len(offices_df.index)
    offices_df.loc[len(offices_df)] = {'lo_id': lo_id, 'office': staff_info['office'], 'house': staff_info['house']}
    assert len(offices_df.index) == size + 1


# Updates and inserts new office associations for legislative staff
# Returns: Nothing, just updates leg_lop_df
def handle_office_row(cursor, staff_info, leg_lop_df, offices_df):

    staff_info['office'] = staff_info['assoc'].lower().strip()

    matched_rows_df = offices_df.loc[((offices_df.office == staff_info['office']) &
                                      (offices_df.house == staff_info['house'])), ]
    assert len(matched_rows_df.index) < 2, 'Matched too many rows'
    if len(matched_rows_df.index) == 0:
        insert_new_office(cursor, staff_info, offices_df)
        matched_rows_df = offices_df.loc[((offices_df.office == staff_info['office']) &
                                          (offices_df.house == staff_info['house'])), ]

    assert len(matched_rows_df.index) == 1, 'Too many offices matched, brah'

    office = matched_rows_df.iloc[0]
    staff_info['lo_id'] = office['lo_id']
    assert staff_info['lo_id']

    update_office_personnel(cursor, staff_info, leg_lop_df)


# The OCR has some issues, helps to clean any obvious mistakes that might arise
# Returns: Nothing, just updates the row dict
def help_OCR(row):

    if ',' not in row and len(row['Employee'].split('. ')) == 2:
        row['Employee'] = row['Employee'].replace('. ', ', ')

    if ' . ' in row['assoc']:
        row['assoc'] = row['assoc'].replace(' . ', ' ')

    if ',' not in row['Employee']:
        names = row['Employee'].split(' ')
        if len(names) < 2:
            names = row['Employee'].split('.')
        names[-2] = names[-2] + ','
        row['Employee'] = ' '.join(names)

    if 'Senalor' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Senalor', 'Senator')

    if 'ed hernandez' in row['assoc'].lower():
        row['assoc'] = 'Senator Ed Hernandez'

    elif 'bob dunon' in row['assoc'].lower():
        row['assoc'] = 'Senator Bob Dutton'

    elif 'Pavlcy' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Pavlcy', 'Pavley')

    elif 'Paviey' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Paviey', 'Pavley')

    elif 'Tom Hannan' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Hannan', 'Harman')

    elif row['assoc'] == 'Senator Elaine Kontominas AJquist':
        row['assoc'] = 'Senator Elaine Kontominas Alquist'

    elif row['assoc'] == 'Senator Elaine Kontominas Alquisl':
        row['assoc'] = 'Senator Elaine Kontominas Alquist'

    elif row['assoc'] == 'Senator Mark DeSaulnicr':
        row['assoc'] = 'Senator Mark DeSaulnier'

    elif 'Mlchael' in row['Employee']:
        row['Employee'] = row['Employee'].replace('Mlchael', 'Michael')

    elif 'DcSaulnier' in row['assoc']:
        row['assoc'] = row['assoc'].replace('DcSaulnier', 'DeSaulnier')

    elif "!'rice" in row['assoc']:
        row['assoc'] = row['assoc'].replace("!'rice", 'Price')

    elif 'Le6n' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Le6n', 'Leon')

    elif 'Hi II' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Hi II', 'Hill')

    elif 'Mooning' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Mooning', 'Monning')

    elif 'Price, Jr.' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Price, Jr.', 'Price')

    elif 'lerry Hill' in row['assoc']:
        row['assoc'] = row['assoc'].replace('lerry Hill', 'Jerry Hill')

    elif 'Jerry llill' in row['assoc']:
        row['assoc'] = row['assoc'].replace('llill', 'Hill')

    elif 'Torn Berryhill' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Torn', 'Tom')

    elif row['assoc'] == 'Senator Tom Benyhill':
        row['assoc'] = 'Tom Berryhill'

    elif 'Pro Tempore • Los Angeles' in row['assoc']:
        row['assoc'] = 'Pro Tempore Los Angeles'

    elif '•' in row['assoc']:
        row['assoc'] = row['assoc'].replace('•', '')

    elif 'Kevin de Lecin' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Lecin', 'Leon')

    elif 'Ed  Hernandez  O D' in row['assoc']:
        row['assoc'] = row['assoc'].replace('O D', '')

    elif 'Jerry  HilJ' in row['assoc']:
        row['assoc'] = row['assoc'].replace('HilJ', 'Hill')

    elif 'Fran Pavky' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Pavky', 'Pavley')

    elif 'William  W. Moaning' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Moaning', 'Monning')

    elif 'William  W. Manning' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Manning', 'Monning')

    elif row['assoc'] == 'Senator Tom  Hannan':
        row['assoc'] = 'Senator Tom  Harman'

    elif row['assoc'] == 'Senator Kevin de Lc6n':
        row['assoc'] = 'Senator Kevin de Leon'

    elif row['assoc'] == 'Senator Ed  Hernandez O D.':
        row['assoc'] = 'Senator Ed Hernandez'

    elif row['assoc'] == 'Senator Ed  Hernandez O.D.':
        row['assoc'] = 'Senator Ed Hernandez'

    elif row['assoc'] == 'Senator Jim N iclsen':
        row['assoc'] = 'Senator Jim Nielsen'

    elif row['assoc'] == 'Senator Isadore Hall, Ill':
        row['assoc'] = 'Senator Isadore Hall'

    elif row['assoc'] == 'Senator Isadore Hall. III':
        row['assoc'] = 'Senator Isadore Hall'

    elif row['assoc'] == 'Senator Ed Hemandez O.D.':
        row['assoc'] = 'Senator Ed Hernandez'

    elif row['assoc'] == 'Senator Isadore Hall, III':
        row['assoc'] = 'Senator Isadore Hall'

    elif row['assoc'] == 'Senator Many Block':
        row['assoc'] = 'Senator Marty Block'

    elif row['assoc'] == 'Senator Mike MorreJI':
        row['assoc'] = 'Senator Mike Morrell'

    elif row['assoc'] == 'Senator Isadore Hall, lII':
        row['assoc'] = 'Senator Isadore Hall'

    elif row['assoc'] == 'Senator lsadore Hall, 111':
        row['assoc'] = 'Senator Isadore Hall'

    elif row['assoc'] == 'Senator William W. Manning':
        row['assoc'] = 'Senator William W. Monning'

    elif row['assoc'] == 'Senator Isadore Hall. llI':
        row['assoc'] = 'Senator Isadore Hall'

    elif 'Senator Isadore Hall' in row['assoc']:
        row['assoc'] = 'Senator Isadore Hall'

    elif 'Senator Isadore Ball' in row['assoc']:
        row['assoc'] = 'Senator Isadore Hall'

    elif row['assoc'] == 'Senator Jerry I!ill':
        row['assoc'] = 'Senator Jerry Hill'

    elif 'im Nielsen' in row['assoc']:
        row['assoc'] = 'Senator Jim Nielsen'

    elif 'm Berryhill' in row['assoc']:
        row['assoc'] = 'Senator Tom Berryhill'

    elif 'Bob Hufr' in row['assoc']:
        row['assoc'] = 'Senator Bob Huff'

    elif 'Henzberg' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Henzberg', 'Hertzberg')

    elif 'Ga!giani' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Ga!giani', 'Galgiani')

    elif 'Mitcheil' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Mitcheil', 'Mitchell')

    elif 'Hettberg' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Hettberg', 'Hertzberg')

    elif 'Herttberg' in row['assoc']:
        row['assoc'] = row['assoc'].replace('Herttberg', 'Hertzberg')

    elif 'kff Stone' in row['assoc']:
        row['assoc'] = row['assoc'].replace('kff', 'Jeff')

    elif 'Senator Ricardo  L-ara' in row['assoc']:
        row['assoc'] = 'Senator Ricardo Lara'

    elif 'Isadore 1:-Iall , Ill' in row['assoc']:
        row['assoc'] = 'Senator Isadore Hall'

    elif 'William W. Menning' in row['assoc']:
        row['assoc'] = 'Senator William W. Monning'

    elif 'Hannah-Beth Jackso!\\' in row['assoc']:
        row['assoc'] = 'Senator Hannah-Beth Jackson'

    elif 'Isadore Halt' in row['assoc']:
        row['assoc'] = 'Senator Isadore Hall'

    elif '.Ierry' in row['assoc'] and 'Hill' in row['assoc']:
        row['assoc'] = 'Senator Jerry Hill'

    elif 'Mike McGui re' in row['assoc']:
        row['assoc'] = 'Senator Mike McGuire'

    elif 'Marty 13lock' in row['assoc']:
        row['assoc'] = 'Senator Marty Block'

    elif 'Senator Mark DeSau!nier' in row['assoc']:
        row['assoc'] = 'Senator Mark DeSaulnier'

    elif 'Senator Jean f uller' in row['assoc']:
        row['assoc'] = 'Senator Jean Fuller'

    elif 'William' in row['assoc'] and 'Menning' in row['assoc']:
        row['assoc'] = 'Senator William Monning'

    elif 'William' in row['assoc'] and 'Manning' in row['assoc']:
        row['assoc'] = 'Senator William Monning'

    elif 'Steve: Knight' in row['assoc']:
        row['assoc'] = 'Senator Steve Knight'

    elif 'Nonna Torres' in row['assoc']:
        row['assoc'] = 'Senator Norma Torres'

    elif 'Jerry' in row['assoc'] and 'HilJ' in row['assoc']:
        row['assoc'] = 'Senator Jerry Hill'

    elif 'Bob' in row['assoc'] and 'uff' in row['assoc']:
        row['assoc'] = 'Senator Bob Huff'

    elif 'Jcrry' in row['assoc'] and 'Hill' in row['assoc']:
        row['assoc'] = 'Senator Jerry Hill'

    elif 'Ed' in row['assoc'] and 'Ilernandez' in row['assoc']:
        row['assoc'] = 'Senator Ed Hernandez'

    elif 'Mark' in row['assoc'] and 'DeSaul' in row['assoc']:
        row['assoc'] = 'Senator Mark DeSaulnier'

    elif 'Carol' in row['assoc'] and 'Li' in row['assoc']:
        row['assoc'] = 'Senator Carol Liu'

    elif 'Ma' in row['assoc'] and 'Leno' in row['assoc']:
        row['assoc'] = 'Senator Mark Leno'

    elif 'L-0is' in row['assoc'] and 'Wolk' in row['assoc']:
        row['assoc'] = 'Senator Lois Wolk'

    elif 'Jean' in row['assoc'] and 'Full' in row['assoc']:
        row['assoc'] = 'Senator Jean Fuller'

    elif 'Fran' in row['assoc'] and 'Pav' in row['assoc']:
        row['assoc'] = 'Senator Fran Pavley'

    elif 'William' in row['assoc'] and 'Mann' in row['assoc']:
        row['assoc'] = 'Senator William Monning'

    elif 'Ji' in row['assoc'] and 'Beall' in row['assoc']:
        row['assoc'] = 'Senator Jim Beall'

    elif 'Ed' in row['assoc'] and 'andez' in row['assoc']:
        row['assoc'] = 'Senator Ed Hernandez'

    search_obj = re.search(r'\w\.\w', row['assoc'])
    if search_obj:
        obj = search_obj.group()
        repl = obj.replace('.', ' ')
        row['assoc'] = row['assoc'].replace(obj, repl, 1)


# There are rows that cause problems. You're going to manually ignore these
def special_row_exceptions(staff_info):

    # if staff_info['first'] == 'russell' and staff_info['last'] == 'stiger' and staff_info['assoc'].lower().strip() == \
    #         'capitol security':
    if staff_info['assoc'].lower().strip() == 'capitol security':
        staff_info['assoc_type'] = 'Ignore'


# If this person was just found in the same file, you want to use the one that was just the legislative staff
# or you ignore them.
# Returns: Nothing, modifies staff info so that we skip it, or deletes a row from leg_lop_df
def handle_same_names(staff_info, leg_lop_df):
    if staff_info['assoc_type'] != 'Ignore':
        matched_rows_df = leg_lop_df.loc[(leg_lop_df.pid == staff_info['pid']) &
                                         (leg_lop_df.file == staff_info['file'])]
        assert len(matched_rows_df.index) < 2
        if len(matched_rows_df.index) == 1:
            matched_row = matched_rows_df.iloc[0]
            if matched_row['assoc_type'] == 'leg' and staff_info['assoc_type'] == 'office':
                staff_info['assoc_type'] == 'Ignore'
            elif matched_row['assoc_type'] == 'office' and staff_info['assoc_type'] == 'leg':
                leg_lop_df.drop(matched_row.name, inplace=True)
            elif matched_row['assoc_type'] == 'leg' and staff_info['assoc_type'] == 'leg':
                assert False, "I don't know how to handle this"
            else:
                assert False, 'God willing you never get here'


# Wrapper for scraping the staff file
def scrape_staff_file_wrapper(cursor, full_path, db_leg_term_df, leg_lop_df, existing_staff_df, file_info, f,
                              offices_df):

    skipped_rows = []
    with open(full_path, 'r') as f_obj:
        reader = csv.reader(f_obj)

        for num, row in enumerate(reader):
            num = num + 1
            row = add_headers_generic(row, file_info['house'])

            if row:
                # cleans up any issues in the row
                help_OCR(row)
                empl = row['Employee']
                org = row['assoc']
                hire_date = row['Hire Date']
                last, first = tuple(empl.split(',', 1))
                middle = None
                split_first = tuple(first.strip().split(' ', 1))
                if len(split_first) > 1:
                    first, middle = split_first
                else:
                    first = split_first[0]
                first, middle, last = norm_names(first, middle, last)

                assert first != ''
                assert last != ''

                staff_pid = get_staff_pid(cursor, first, middle, last, existing_staff_df)
                term_year = match_term_year(file_info['date'].year)
                assoc_type, assoc = parse_association(org, db_leg_term_df, file_info['house'], term_year)

                assoc = unidecode(assoc)

                staff_info = {'first': first,
                              'middle': middle,
                              'last': last,
                              # 'hire_date': parse_date(hire_date),
                              'file_date': file_info['date'],
                              'term_year': term_year,
                              'house': file_info['house'],
                              'pid': staff_pid,
                              'row': num,
                              'file': f,
                              'assoc_type': assoc_type,
                              'assoc': assoc}
                # makes appending to your dataframe possible
                for key in LOP_COLUMNS:
                    if key == 'file_appearances':
                        staff_info[key] = ''
                    elif key == 'set_at_end':
                        staff_info[key] = False
                    elif key not in staff_info:
                        staff_info[key] = None


                # Changes problem rows so that they are ignored
                special_row_exceptions(staff_info)

                assert staff_info['term_year'] <= staff_info['file_date'].year
                if staff_info['assoc_type'] == 'leg':

                    # This function returns False if the staff member could not be matched
                    if get_leg_pid(staff_info, db_leg_term_df):

                        set_hire_date(staff_info, leg_lop_df, hire_date)
                        # Like 50% sure this function is now deprecated
                        # update_date_hired(staff_info, leg_lop_df)

                        update_leg_personnel(cursor, staff_info, leg_lop_df)

                        # Literally just defined here for the assertion
                        matched_rows_df = leg_lop_df.loc[((leg_lop_df.pid == staff_info['pid']) &
                                                          (leg_lop_df.leg_pid == staff_info['leg_pid']) &
                                                          (leg_lop_df.term_year == staff_info['term_year']) &
                                                          (leg_lop_df.house == staff_info['house'])), ]
                        assert matched_rows_df.iloc[0]['last_seen']
                        assert len(matched_rows_df.index) == 1

                    # else:
                    #     # should always match your staff member?
                    #     assert False, 'Staff member never matched'

                if staff_info['assoc_type'] == 'office':

                    set_hire_date(staff_info, leg_lop_df, hire_date)
                    handle_office_row(cursor, staff_info, leg_lop_df, offices_df)
            else:
                skipped_rows.append(num)

        cpy_leg_lop_df = leg_lop_df.copy()
        leg_lop_df = leg_lop_df.apply(lambda row: update_end_dates(row, cursor, file_info['date'], cpy_leg_lop_df,
                                                                   file_info['house'], f), axis=1)
        print('Skipped Rows', f, skipped_rows)

        return leg_lop_df


# Runs the update for LegOfficePersonnel
def lop_update_stmt(cursor, lop_info):
    update_stmt = """UPDATE LegOfficePersonnel
                     SET end_date = %s
                     WHERE staff_member = %s
                        and legislator = %s
                        and house = %s"""

    cursor.execute(update_stmt, (lop_info['end_date'], int(lop_info['pid']), int(lop_info['leg_pid']),
                                 lop_info['house']))


# Runs the update for LegOfficePersonnel
def op_update_stmt(cursor, op_info):
    update_stmt = """UPDATE OfficePersonnel
                     SET end_date = %s
                     WHERE staff_member = %s
                        and office = %s"""

    cursor.execute(update_stmt, (op_info['end_date'], int(op_info['pid']), int(op_info['lo_id'])))


# Helper function for update_end_dates. Sets the end date in leg_lop_row if it overlaps with
# another row
def correct_overlap_helper(leg_lop_row, cpy_leg_lop_df):

    out = False

    # No fricken clue why you need the int cast here
    matched_rows_df = cpy_leg_lop_df[(int(leg_lop_row['pid']) == cpy_leg_lop_df.pid) &
                                     (leg_lop_row['hire_date'] < cpy_leg_lop_df.hire_date) &
                                     (leg_lop_row['end_date'] > cpy_leg_lop_df.hire_date)]

    assert len(matched_rows_df.index) < 2, 'Too many overlaps'

    if len(matched_rows_df.index) == 1:
        leg_lop_row['end_date'] = matched_rows_df.iloc[0]['hire_date'] - relativedelta(days=1)
        matched_rows_df = cpy_leg_lop_df[(int(leg_lop_row['pid']) == cpy_leg_lop_df.pid) &
                                         (leg_lop_row['hire_date'] < cpy_leg_lop_df.hire_date) &
                                         (leg_lop_row['end_date'] > cpy_leg_lop_df.hire_date)]
        out = True

    assert len(matched_rows_df.index) == 0

    return out


# Sets the end date to be the date associated with the file if the staff member
# was not found
# Returns: Series with updated end_date value
def update_end_dates(leg_lop_row, cursor, file_date, cpy_leg_lop_df, house, file_name):

    update = False
    # Only updates end dates for staff members that weren't in the most recent file of the same house
    if leg_lop_row['last_seen'] < file_date and pd.isnull(leg_lop_row['end_date']) and \
            leg_lop_row['house'] == house:

        update = True
        # For leg rows with a term year lower than the term associated with the file, set the end date to the
        # end of the year
        if pd.notnull(leg_lop_row['leg_pid']) and match_term_year(file_date.year) > leg_lop_row['term_year']:
            leg_lop_row['end_date'] = datetime.date(int(leg_lop_row['term_year']) + 1, 12, 31)

        else:
            leg_lop_row['end_date'] = leg_lop_row['last_seen'] + STAFF_STINT_ASSUMPTION

    # corrects overlap if you made one
    update = correct_overlap_helper(leg_lop_row, cpy_leg_lop_df) or update

    if update:
        if leg_lop_row['assoc_type'] == 'leg':
                lop_update_stmt(cursor, leg_lop_row)
        elif leg_lop_row['assoc_type'] == 'office':
            op_update_stmt(cursor, leg_lop_row)
        else:
            assert False

    return leg_lop_row


def main():
    with pymysql.connect(**CONN_INFO) as cursor:

        load_legs = False
        # load_legs = True
        if load_legs:
            db_leg_term_infos = scrape_legs_wrapper(cursor)
            pickle.dump(db_leg_term_infos, open('SavedLegTermInfo.p', 'wb'))
        else:

            db_leg_term_infos = pickle.load(open('SavedLegTermInfo.p', 'rb'))
            db_leg_term_df = pd.DataFrame(db_leg_term_infos)

            leg_lop_df = pd.DataFrame(columns=LOP_COLUMNS)

            existing_staff_df = fetch_existing_staff(cursor)
            for root, _, files in os.walk(STAFF_DATA_DIR):
                ordered_files = order_files_by_date(files)

                offices_df = pd.DataFrame(columns=['office', 'lo_id', 'house'])
                for f in ordered_files:
                    full_path = os.path.join(root, f)
                    file_info = scrape_file_info(f)

                    leg_lop_df = scrape_staff_file_wrapper(cursor, full_path, db_leg_term_df, leg_lop_df,
                                              existing_staff_df, file_info, f, offices_df)

            # Catches and sets an end date for the very last group
            leg_lop_df.loc[pd.isnull(leg_lop_df.end_date), 'set_at_end'] = True
            leg_lop_df.loc[pd.isnull(leg_lop_df.end_date), 'end_date'] = \
                leg_lop_df.loc[pd.isnull(leg_lop_df.end_date), 'last_seen'] + STAFF_STINT_ASSUMPTION
            leg_lop_df['primary_source'] = True
            pickle.dump(leg_lop_df, open('leg_lop_df.p', 'wb'))
            pickle.dump(existing_staff_df, open('existing_staff_df.p', 'wb'))
            pickle.dump(g_missed_staff, open('g_missed_staff.p', 'wb'))
            print('blah')


if __name__ == '__main__':
    main()
