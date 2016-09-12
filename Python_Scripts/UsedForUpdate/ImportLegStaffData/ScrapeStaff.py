import os, os.path
import sys
import pickle
import pymysql
import csv
import warnings
import datetime
warnings.filterwarnings('error', category=pymysql.Warning)

from MatchingFunctions import norm_names
from MatchingFunctions import cmp_names
from ScrapeLegFunctions import scrape_file_info
from ScrapeLegFunctions import scrape_legislators
from ScrapeLegFunctions import order_files_by_date
from ScrapeLegFunctions import fetch_db_leg_term_info
from ScrapeLegFunctions import LEG_DATA_DIR
from ScrapeLegFunctions import match_term_year
from ScrapeLegFunctions import cmp_dicts


STAFF_DATA_DIR = 'BestStaffData'
g_missed_staff = []

# Remaps the leg last name for better matching, eg quirk -> quirk-silva
# Deprecated?
def remap_leg_last_name(last):
    if last == 'quirk':
        last = 'quirk-silva'
    return last

# Remaps the leg last name for better matching, eg quirk -> quirk-silva
def remap_leg_names_pdfs(first, middle, last):
    if first == 'ed' and middle == 'hernandez' and last == 'o.d.':
        first, middle, last = 'ed', None, 'hernandez'
    if first == 'curren' and middle == 'd.' and last == 'price,':
        first, middle, last = 'curren', 'd.', 'price'
    if first == 'kevin' and middle == 'de' and last == 'lea3n':
        first, middle, last = 'kevin', 'de', 'leon'
    return first, middle, last


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
    return {norm_names(first, middle, last): pid for pid, first, middle, last in cursor}


# Gets the pid of existing staff member or inserts a new one if that staff member could not be found
# Returns: pid of the staff member
def get_staff_pid(cursor, first, middle, last, existing_staff):

    assert (first.lower() == first and last.lower() == last), 'Staff names not lowercase-afied'

    if (first, middle, last) in existing_staff:
        pid = existing_staff[(first, middle, last)]

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
                               (pid, flag, state)
                               VALUES
                               (%s, 0, "CA")'''
        cursor.execute(insert_staff_stmt, pid)

        existing_staff[(first, middle, last)] = pid

    return pid


# Gets the associated legislator or office for this row
# Returns: ('office'/'leg', name), first value is type
def parse_association(org, leg_last_names, house):
    type = None
    assoc = None
    if house == 'Senate' and 'senator ' in org.lower():
        type = 'leg'
        assoc = org.replace('senator ', '').strip()
    if house == 'Assembly':
        potential_leg = org.split(',')[0]
        potential_leg = norm_names(None, None, potential_leg)[2]
        # potential_leg = remap_leg_last_name(potential_leg)
        if potential_leg in leg_last_names:
            type = 'leg'
            assoc = potential_leg
    if not type:
        type = 'office'
        assoc = org.lower().strip()
    return type, assoc

# Builds a set of all legislator last names
# Returns: Set of last names, all lowercase
def collect_leg_last_names(db_leg_term_infos):
    out = set()
    for leg_term_info in db_leg_term_infos:
        out.add(leg_term_info['last'].lower())
    return out


# Just attaches the header information to each row and outputs it as a dictionary
# Returns: dictionary of row attributes, None if header row
def add_headers_assembly(row):
    # makes sure this isn't a header row
    header = ['Employee', 'Organization Name', 'Hire Date', 'Classification', 'Salary']
    # This is for 'AssemblyStaff 11-30-11.csv' because seriously, fuck that file
    if len(row) == 4:
        row.append('')
    if len(header) != len(row):
        return None
    return {k: v for k, v in zip(header, row)}



# Returns: dictionary of row attributes, None if header row
def add_headers_senate(row):
    # makes sure this isn't a header row
    header = ['Employee', 'Organization Name', 'Classification', 'Hire Date', 'Salary', 'Grant', 'Salary_2']
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


# Handles the case where the file is an assembly file. Called in get_leg_pid
def get_leg_pid_assembly(staff_info, db_leg_term_infos):
    potential_leg_last = staff_info['assoc']

    success = True
    matched_leg_terms = []
    for leg_term_info in db_leg_term_infos:
        cmp_keys = ('term_year', 'house')
        if leg_term_info['last'].lower() == potential_leg_last and cmp_dicts(staff_info, leg_term_info, cmp_keys):
            matched_leg_terms.append(leg_term_info)

            # structured this way because you're assuming that assertion will eventually break
    if len(matched_leg_terms) > 1:
        new_matched_leg_terms = []
        for leg_term_info in matched_leg_terms:
            found = None
            dir_staff = leg_term_info['staff_set']
            found = match_other_dir_staff(staff_info['first'], staff_info['middle'], staff_info['last'], dir_staff)
            if found:
                new_matched_leg_terms.append(leg_term_info)
                # print(staff_info['first'], staff_info['middle'], staff_info['last'], ' --> ', found)

    # Okay okay so there's a Quirk and a Quirk-Silva in 2015 senate, this is not ideal


        # This is for debugging purposes, you can look through these later
        if len(new_matched_leg_terms) == 0:
            g_missed_staff.append(staff_info)
            success = False
        assert len(new_matched_leg_terms) < 2, 'Multiple legs matched in staff dir'
        matched_leg_terms = new_matched_leg_terms

    # Makes sure we only found one leg
    if success:
        assert len(matched_leg_terms) == 1, 'Multiple legs matched'
        leg_term_info = matched_leg_terms[0]

        staff_info['leg_pid'] = leg_term_info['pid']
        staff_info['leg_first'] = leg_term_info['first']
        staff_info['leg_middle'] = leg_term_info['middle']
        staff_info['leg_last'] = leg_term_info['last']

    return success


# Handles the case where the file is an senate file. Called in get_leg_pid
def get_leg_pid_senate(staff_info, db_leg_term_infos):
    file_leg_name = staff_info['assoc'].lower().replace('senator', '').strip()
    names = file_leg_name.split(' ', 3)
    if len(names) == 4:
        first, middle, last = norm_names(names[0], names[1], names[2])
    elif len(names) == 3:
        first, middle, last = norm_names(names[0], names[1], names[2])
    elif len(names) == 2:
        first, middle, last = norm_names(names[0], None, names[1])
    else:
        assert False, 'Parsed too many names for leg'

    first, middle, last = remap_leg_names_pdfs(first, middle, last)

    matched_leg_terms = []
    for leg_term_info in db_leg_term_infos:
        cmp_keys = ('term_year', 'house')
        name_match = cmp_names(first, middle, last, leg_term_info['first'], leg_term_info['middle'],
                               leg_term_info['last'])
        if name_match and cmp_dicts(staff_info, leg_term_info, cmp_keys):
            matched_leg_terms.append(leg_term_info)

    assert len(matched_leg_terms) == 1, 'Matched improper number of legislators'
    leg_term_info = matched_leg_terms[0]

    staff_info['leg_pid'] = leg_term_info['pid']
    staff_info['leg_first'] = leg_term_info['first']
    staff_info['leg_middle'] = leg_term_info['middle']
    staff_info['leg_last'] = leg_term_info['last']

    # gross
    return True


# Loops through leg_term information to assign the staff member to the appropriate legislator
# Returns: True if found, false otherwise, modifies staff_info dictionary passed in
def get_leg_pid(staff_info, db_leg_term_infos):

    if staff_info['house'] == 'Senate':
        return get_leg_pid_senate(staff_info, db_leg_term_infos)
    elif staff_info['house'] == 'Assembly':
        return get_leg_pid_assembly(staff_info, db_leg_term_infos)
    assert False


# Builds the starting dictionary for leg_lop_info
# Returns dict of leg_pid: <empty dict>
def build_leg_lop_info(db_leg_term_infos):
    out = {}
    for leg_term_info in db_leg_term_infos:
        if leg_term_info['pid'] not in out:
            out[leg_term_info['pid']] = {}
    return out


# parses the date given m/d/year format
# Returns python date
def parse_date(date_str):
    month, day, year = date_str.split("/")
    year, month, day = int(year), int(month), int(day)
    if year < 25:
        year += 2000
    elif year < 100:
        year += 1900

    assert year > 1940, 'Who the hell wrote this year?'

    return datetime.date(year, month, day)


# Inserts a new leg office personnel into the database and updates your version
# Returns: Nothing, updates staff_info
def insert_new_lop(cursor, staff_info):

    insert_stmt = '''INSERT INTO LegOfficePersonnel
                     (staff_member, legislator, term_year, house, start_date, state)
                     VALUES
                     (%s, %s, %s, %s, %s, "CA")'''
    # Note that 15 gets auto-converted to 2015 by mysql
    cursor.execute(insert_stmt, (staff_info['pid'], staff_info['leg_pid'], staff_info['term_year'], staff_info['house'],
                                 str(staff_info['hire_date'])))

    staff_info['last_seen'] = staff_info['hire_date']


# Inserts a new leg office personnel into the database and updates your version
# Returns: Nothing, updates staff_info
def insert_new_op(cursor, staff_info):

    insert_stmt = '''INSERT INTO OfficePersonnel
                     (staff_member, office, start_date, state)
                     VALUES
                     (%s, %s, %s, "CA")'''
    # Note that 15 gets auto-converted to 2015 by mysql
    cursor.execute(insert_stmt, (staff_info['pid'], staff_info['lo_id'], str(staff_info['hire_date'])))

    staff_info['last_seen'] = staff_info['hire_date']


# Determines which flavor of personnel to insert
def insert_new_personnel_generic(cursor, staff_info):

    if staff_info['assoc_type'] == 'leg':
        insert_new_lop(cursor, staff_info)
    elif staff_info['assoc_type'] == 'office':
        insert_new_op(cursor, staff_info)
    else:
        assert False


# Updates the state of LegOfficePersonnel in both the database and your in memory concept
# of it
# Returns: Nothing, just updates db and changes lop_pks
def update_office_personnel(cursor, staff_info, lop_pks, pk_fields):
    pk = tuple([staff_info[k] for k in pk_fields])
    # pk = (staff_info['pid'], staff_info['leg_pid'], staff_info['term_year'])
    if pk not in lop_pks:
        insert_new_personnel_generic(cursor, staff_info)
        lop_pks[pk] = staff_info
    lop_pks[pk]['last_seen'] = staff_info['file_date']


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


# Runs the update for LegOfficePersonnel
def lop_update_stmt(cursor, lop_info):
    update_stmt = """UPDATE LegOfficePersonnel
                                 SET end_date = %s
                                 WHERE staff_member = %s
                                    and legislator = %s
                                    and house = %s"""

    cursor.execute(update_stmt, (lop_info['end_date'], lop_info['pid'], lop_info['leg_pid'],
                                 lop_info['house']))


# Runs the update for LegOfficePersonnel
def op_update_stmt(cursor, op_info):
    update_stmt = """UPDATE OfficePersonnel
                     SET end_date = %s
                     WHERE staff_member = %s
                        and office = %s"""

    cursor.execute(update_stmt, (op_info['end_date'], op_info['pid'], op_info['office']))


# Sets the end date to be the date associated with the file if the staff member
# was not found
# Returns: Nothing, just updates info (either office_info or leg_lop_info)
def update_end_dates(cursor, file_date, info):

    assert file_date.year >= info['term_year']

    for key in info:
        pks = info[key]
        for pk in pks:
            personnel = pks[pk]

            if personnel['last_seen'] < file_date and 'end_date' not in personnel:
                # This method will leave brief holes regarding association
                personnel['end_date'] = personnel['last_seen']

                if personnel['assoc_type'] == 'leg':
                    lop_update_stmt(cursor, personnel)
                elif personnel['assoc_type'] == 'office':
                    op_update_stmt(cursor, personnel)
                else:
                    assert False

            else:
                assert personnel['last_seen'] == file_date
                assert 'end_date' not in personnel



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


# Inserts a new record into the office table
# Returns: Nothing, updates office_info
def insert_new_office(cursor, staff_info, office_info, office_ids):
    insert_stmt = """INSERT INTO LegislatureOffice
                     (name, house, state)
                     VALUES
                     (%s, %s, 'CA')"""
    cursor.execute(insert_stmt, (staff_info['office'].title(), staff_info['house']))
    lo_id = cursor.lastrowid
    office_info[lo_id] = {}
    office_ids[(staff_info['office'], staff_info['house'])] = lo_id


# Updates and inserts new office associations for legislative staff
# Returns: Nothing, just updates office_info
def handle_office_row(cursor, staff_info, office_info, office_ids):

    staff_info['office'] = staff_info['assoc'].lower().strip()

    if (staff_info['office'], staff_info['house']) not in office_ids:
        insert_new_office(cursor, staff_info, office_info, office_ids)

    staff_info['lo_id'] = office_ids[(staff_info['office'], staff_info['house'])]
    assert staff_info['lo_id']

    pk_fields = ['pid', 'lo_id', 'hire_date']
    update_office_personnel(cursor, staff_info, office_info[staff_info['lo_id']], pk_fields)


# Wrapper for scraping the staff file
def scrape_staff_file_wrapper(cursor, full_path, db_leg_term_infos, leg_last_names, leg_lop_info, existing_staff,
                              file_info, f, office_info, office_ids):

    skipped_rows = []
    with open(full_path, 'r') as f_obj:
        reader = csv.reader(f_obj)

        for num, row in enumerate(reader):
            num = num + 1
            row = add_headers_generic(row, file_info['house'])

            if row:
                empl = row['Employee']
                org = row['Organization Name']
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

                staff_pid = get_staff_pid(cursor, first, middle, last, existing_staff)
                assoc_type, assoc = parse_association(org, leg_last_names, file_info['house'])

                staff_info = {'first': first,
                              'middle': middle,
                              'last': last,
                              'hire_date': parse_date(hire_date),
                              'file_date': file_info['date'],
                              'term_year': match_term_year(file_info['date'].year),
                              'house': file_info['house'],
                              'pid': staff_pid,
                              'row': num,
                              'file': f,
                              'assoc_type': assoc_type,
                              'assoc': assoc}

                assert staff_info['term_year'] <= staff_info['file_date'].year
                if staff_info['assoc_type'] == 'leg':

                    # This function returns None if the staff member could not be matched
                    if get_leg_pid(staff_info, db_leg_term_infos):
                        pk_fields = ['pid', 'leg_pid', 'term_year']
                        update_office_personnel(cursor, staff_info, leg_lop_info[staff_info['leg_pid']], pk_fields)

                        # Literally just defined here for the assertion
                        leg_pid = staff_info['leg_pid']
                        pk = (staff_info['pid'], staff_info['leg_pid'], staff_info['term_year'])
                        assert leg_lop_info[leg_pid][pk]['last_seen'], '"last_seen" not set'

                if staff_info['assoc_type'] == 'office':
                    handle_office_row(cursor, staff_info, office_info, office_ids)
            else:
                skipped_rows.append(num)

        print('Skipped Rows', f, skipped_rows)


def main():

    with pymysql.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='AndrewTest',
                         # db='AndrewTest2',
                         # db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789') as cursor:

        # load_legs = False
        load_legs = True
        if load_legs:
            db_leg_term_infos = scrape_legs_wrapper(cursor)
            pickle.dump(db_leg_term_infos, open('SavedLegTermInfo.p', 'wb'))

        db_leg_term_infos = pickle.load(open('SavedLegTermInfo.p', 'rb'))

        leg_last_names = collect_leg_last_names(db_leg_term_infos)
        leg_lop_info = build_leg_lop_info(db_leg_term_infos)
        # format <office name> -> <pid, associations>
        office_info = {}
        office_ids = {}

        existing_staff = fetch_existing_staff(cursor)
        for root, _, files in os.walk(STAFF_DATA_DIR):
            ordered_files = order_files_by_date(files)

            for f in ordered_files:
                full_path = os.path.join(root, f)
                file_info = scrape_file_info(f)

                scrape_staff_file_wrapper(cursor, full_path, db_leg_term_infos, leg_last_names, leg_lop_info,
                                          existing_staff, file_info, f, office_info, office_ids)

            update_end_dates(cursor, file_info['date'], leg_lop_info)
            update_end_dates(cursor, file_info['date'], office_info)

        print('Number missed staff', len(g_missed_staff))
        pickle.dump(g_missed_staff, open('MissedStaff.p', 'wb'))
        # exit()
        print('hello world')

if __name__ == '__main__':
    main()
