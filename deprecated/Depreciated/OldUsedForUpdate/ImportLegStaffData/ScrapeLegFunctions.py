import re
import openpyxl as xl
from datetime import date
from unidecode import unidecode

from MatchingFunctions import norm_names
from MatchingFunctions import cmp_names

LEG_DATA_DIR = 'LegData'
YEAR_OFFSET = 2000
SHEET = 'Sheet1'

# Inserts a new legislator into both Legislator and Person
# Returns: Nothing, pid is added to the dictionary
def insert_new_leg(cursor, leg_term_info):

    insert_person_stmt = '''INSERT INTO Person
                            (first, middle, last)
                            VALUES
                            (%(first)s, %(middle)s, %(last)s)'''
    cursor.execute(insert_person_stmt, leg_term_info)

    pid = cursor.lastrowid
    insert_leg_stmt = '''INSERT INTO Legislator
                         (pid, state)
                         VALUES
                         (%s, "CA")'''
    cursor.execute(insert_leg_stmt, pid)

    leg_term_info['pid'] = pid


# Gets the house, year, and actual date associated with the info in the file
def scrape_file_info(file_name):
    file_info = {}
    match = re.match(r"(\w+) (\d{1,2})-(\d{1,2})-(\d{2})", file_name)
    house = match.group(1)
    if 'assembly' in house.lower():
        file_info['house'] = 'Assembly'
    elif 'senate' in house.lower():
        file_info['house'] = 'Senate'
    assert file_info['house']
    file_info['date'] = date(int(match.group(4)) + YEAR_OFFSET, int(match.group(2)), int(match.group(3)))

    return file_info


# Returns the files ordered by their date in ascending order
def order_files_by_date(files):
    new_list = []
    for f in files:
        date = scrape_file_info(f)['date']
        new_list.append((f, date))

    new_list = sorted(new_list, key=lambda x: x[1])
    return (tup[0] for tup in new_list)


# Gets the legislator and term info for every leg currently in the db
# Returns: dictionary of leg_term info
def fetch_db_leg_term_info(cursor):
    query = '''SELECT p.pid, p.first, p.middle, p.last, t.year, t.house, t.party, t.district
               FROM Legislator l
                    JOIN Person p
                    ON l.pid = p.pid
                    JOIN Term t
                    ON l.pid = t.pid
                WHERE l.state = "CA" '''
    cursor.execute(query)
    return [{'first': first,
             'middle': middle,
             'last': last,
             'term_year': year,
             'house': house,
             'party': party,
             'district': district,
             'pid': pid,
             'staff_set': {}} for pid, first, middle, last, year, house, party, district in cursor]


def clean_name(name):
    return unidecode(name).strip().replace(',', '')


def map_party(party):
    if party.lower() == 'd':
        return 'Democrat'
    if party.lower() == 'r':
        return 'Republican'
    if party.lower() == 'i':
        return 'Other'
    print("Party: {} from spreadsheet didn't match expected input".format(party))
    assert False


# parses out the staff names from the cell in the excel file
def parse_staff_name(name):
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

# Handles any special case of a term you might have missed
def special_term_exceptions(cursor, leg_term_info, db_leg_term_infos):

    if leg_term_info['last'].lower() == 'torres' and leg_term_info['term_year'] == 2013 and \
                    leg_term_info['house'] == 'Senate':
        copy_leg_term_info = {k: v for k, v in leg_term_info.items()}
        copy_leg_term_info['house'] = 'Assembly'
        insert_new_term(cursor, copy_leg_term_info, db_leg_term_infos)

    if leg_term_info['last'].lower() == 'mitchell' and leg_term_info['term_year'] == 2013 and \
                    leg_term_info['house'] == 'Senate':
        copy_leg_term_info = {k: v for k, v in leg_term_info.items()}
        copy_leg_term_info['house'] = 'Assembly'
        insert_new_term(cursor, copy_leg_term_info, db_leg_term_infos)

    if leg_term_info['last'].lower() == 'hueso' and leg_term_info['term_year'] == 2013 and \
                    leg_term_info['house'] == 'Senate':
        copy_leg_term_info = {k: v for k, v in leg_term_info.items()}
        copy_leg_term_info['house'] = 'Assembly'
        insert_new_term(cursor, copy_leg_term_info, db_leg_term_infos)

    if leg_term_info['last'].lower() == 'blumenfield' and leg_term_info['term_year'] == 2011 and \
                    leg_term_info['house'] == 'Assembly':
        copy_leg_term_info = {k: v for k, v in leg_term_info.items()}
        copy_leg_term_info['term_year'] = 2013
        insert_new_term(cursor, copy_leg_term_info, db_leg_term_infos)

    if leg_term_info['last'].lower() == 'price' and leg_term_info['term_year'] == 2011 and \
                    leg_term_info['house'] == 'Senate':
        copy_leg_term_info = {k: v for k, v in leg_term_info.items()}
        copy_leg_term_info['term_year'] = 2013
        insert_new_term(cursor, copy_leg_term_info, db_leg_term_infos)


def insert_new_term(cursor, leg_term_info, db_leg_term_infos):
    insert_term_stmt = """INSERT INTO Term
                          (pid, year, house, party, state, district)
                          VALUES
                          (%(pid)s, %(term_year)s, %(house)s, %(party)s, "CA", %(district)s)"""
    cursor.execute(insert_term_stmt, leg_term_info)
    db_leg_term_infos.append(leg_term_info)

    special_term_exceptions(cursor, leg_term_info, db_leg_term_infos)


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


def match_term_year(year):
    if year % 2 == 0:
        return year - 1
    return year


def concat_names(first, middle, last):
    first, middle, last = norm_names(first, middle, last)
    return first + (' ' + middle + ' ' if middle else ' ') + last


def cmp_dicts(d1, d2, key_subset):
    compared_values = [d1[k] == d2[k] for k in key_subset]
    return compared_values.count(False) == 0


# Unions the two sets of legislative staff based on name comparisons
# Returns: the union of the two sets
def smart_union_names(og_set, new_set, file_date):
    if not new_set:
        return og_set
    if not og_set:
        return new_set

    for f2, m2, l2 in new_set:
        for f1, m1, l1 in og_set:
            matched = False
            if cmp_names(f1, m1, l1, f2, m2, l2):
                matched = True
                og_set[(f1, m1, l1)]['last_date'] = file_date
                break

        if not matched:
            elems = len(og_set)
            og_set[(f2, m2, l2)] = {'first_date': file_date, 'last_date': file_date}
            assert len(og_set) > elems, 'Failed to add new element to the set'

    return og_set


# Parses the corresponding legislator info from the file. If that legislator doesn't exist, inserts
# a new leg and term info. Updates your local version of this info as well
# Returns: doesn't return shit, just updates info in db_leg_term_infos
def parse_corresponding_leg_info(cursor, leg_term_info, db_leg_term_infos, file_date):
    leg_term_info['first'], leg_term_info['last'] = remap_leg_names(leg_term_info['first'], leg_term_info['last'])
    # also norms it
    full_name = concat_names(leg_term_info['first'], None, leg_term_info['last'])

    leg_term_info['pid'] = None
    for db_leg_term_info in db_leg_term_infos:
        cmp_full_name = concat_names(db_leg_term_info['first'], db_leg_term_info['middle'],
                                     db_leg_term_info['last'])
        keys_to_cmp = ('term_year', 'house')
        if full_name == cmp_full_name and cmp_dicts(leg_term_info, db_leg_term_info, keys_to_cmp):
            # this line's just retained for you stupid assertion, idiot
            leg_term_info['pid'] = db_leg_term_info['pid']
            assert db_leg_term_info['staff_set'] != None, 'staff_set never initialized'
            db_leg_term_info['staff_set'] = smart_union_names(db_leg_term_info['staff_set'], leg_term_info['staff_set'],
                                                              file_date)
            break

    if not leg_term_info['pid']:
        for db_leg_term_info in db_leg_term_infos:
            cmp_full_name = concat_names(db_leg_term_info['first'], db_leg_term_info['middle'],
                                         db_leg_term_info['last'])
            if full_name == cmp_full_name:
                leg_term_info['pid'] = db_leg_term_info['pid']
                insert_new_term(cursor, leg_term_info, db_leg_term_infos)
                break

    if not leg_term_info['pid']:
        assert leg_term_info['term_year'] != 2015
        insert_new_leg(cursor, leg_term_info)
        insert_new_term(cursor, leg_term_info, db_leg_term_infos)

    assert(leg_term_info['pid'])


# Scrapes the leg names from the files
# Returns: Dictionary containing all legislator info and associated leg staff
def scrape_legislators(cursor, file_name, file_date, house, db_leg_term_infos):

    wb = xl.load_workbook(file_name, read_only=True)
    ws = wb[SHEET]

    first = True
    for row in ws.rows:
        if first:
            first = False
            header = [cell.value for cell in row]
        else:
            leg_term_info = {'house': house}
            values = [cell.value for cell in row]
            col_dict = {key:value for key, value in zip(header, values)}
            leg_term_info['last'] = clean_name(col_dict['Lname'])
            leg_term_info['first'] = clean_name(col_dict['Fname'])
            leg_term_info['middle'] = None
            leg_term_info['term_year'] = match_term_year(file_date.year)
            try:
                if 'Dist_Number' in col_dict:
                    leg_term_info['district'] = int(col_dict['Dist_Number'])
                else:
                    leg_term_info['district'] = int(col_dict['enat'])
            except TypeError:
                leg_term_info['district'] = None

            # Heuristic for picking out the office rows
            if len(leg_term_info['first'].split(' ')) <= 2 and 'District' not in leg_term_info['first']:
                leg_term_info['party'] = map_party(col_dict['Party'])

                staff = col_dict['Staff'].split(';') if col_dict['Staff'] else []
                dist_staff = col_dict['Dist_Staff'].split(';') if col_dict['Dist_Staff'] else []
                dist_staff2 = col_dict['Dist_Staff2'].split(';') if col_dict['Dist_Staff2'] else []
                dist_staff3 = col_dict['Dist_Staff3'].split(';') if col_dict['Dist_Staff3'] else []
                dist_staff4 = col_dict['Dist_Staff4'].split(';') if col_dict['Dist_Staff4'] else []

                all_staff = staff + dist_staff + dist_staff2 + dist_staff3 + dist_staff4
                # names follow first, middle, last ordering
                all_staff = {parse_staff_name(name): {'start_date': file_date, 'last_date': file_date} for name in\
                    all_staff if name.strip() != ''}

                leg_term_info['staff_set'] = all_staff
                parse_corresponding_leg_info(cursor, leg_term_info, db_leg_term_infos, file_date)

            else:
                print("Ignoring {} {} in file {}".format(leg_term_info['first'], leg_term_info['last'], file_name))

