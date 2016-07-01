#!/usr/bin/env python
'''
File: populate_sa.py
Author: Gregory Davis
Last Modified: June 20, 2016
Description:
- Performs initial population of the Digital Democracy State Agency Database.
Sources:
  - State_Agency_People.csv
Populates:
  - State (abbrev, country, name)
  - StateAgency (name, state)
  - Person (first, middle, last)
  - servesOn (pid, year, agency, position)
'''

import MySQLdb
import csv
import xlrd

# U.S. State
state = 'CA'

# Querys used to Insert into the Database
QI_STATE = '''INSERT INTO State (abbrev, country, name)
                          VALUES (%s, %s, %s)'''
QI_STATE_AGENCY = '''INSERT INTO StateAgency (name, state)
                            VALUES (%s, %s)'''
QI_PERSON = '''INSERT INTO Person (first, middle, last)
                 VALUES (%s, %s, %s)'''
QI_SERVES_ON = '''INSERT INTO servesOn (pid, year, agency, position)
               VALUES (%s, %s, %s, %s)'''
QS_STATE = '''SELECT abbrev
                  FROM State
                  WHERE abbrev = %s'''
QS_STATE_AGENCY = '''SELECT sa_id
                  FROM StateAgency
                  WHERE name = %s
                  AND state = %s'''
QS_PERSON = '''SELECT pid
                   FROM Person
                   WHERE first = %s
                   AND middle = %s
                   AND last = %s'''
QS_SERVES_ON = '''SELECT pid, year, agency, position
                      FROM servesOn
                      WHERE pid = %s
                       AND year = %s
                       AND agency = %s
                       AND position = %s'''

'''
Finds the corresponding pid for the given Person name.
|first|: First name of Person
|middle|: Middle name of Person
|last|: Last name of Person
Returns pid if Person is found. Otherwise, return None.
'''
def get_person(dd_cursor, first, middle, last):
  dd_cursor.execute(QS_PERSON, (first, middle, last))
  if dd_cursor.rowcount == 1:
    return dd_cursor.fetchone()[0]
  elif dd_cursor.rowcount == 0: return None
  else: raise ValueError

'''
Finds the corresponding state agency for the given name.
|name|: state agency name
Returns sa_id if StageAgency is found. Otherwise, return None.
'''
def get_state_agency(dd_cursor, name, state):
  dd_cursor.execute(QS_STATE_AGENCY, (name, state))
  if dd_cursor.rowcount == 1:
    return dd_cursor.fetchone()[0]
  elif dd_cursor.rowcount == 0: return None
  else: raise ValueError

'''
Given a state's abbreviation, country, and name, check if it's in DDDB. If not, add.
|dd_cursor|: DDDB database cursor
|abbrev|: state abbreviation
|country|: country state is in
|name|: state name
'''
def insert_state(dd_cursor, abbrev, country, name):
  dd_cursor.execute(QS_STATE, (abbrev, ))
  if dd_cursor.rowcount == 0:
    dd_cursor.execute(QI_STATE, (abbrev, country, name))

'''
Given a state agency's information, check if it's in DDDB. If not, add.
|dd_cursor|: DDDB database cursor
|name|: agency name
|state|: state abbreviation
'''
def insert_state_agency(dd_cursor, name, state):
  sa_id = get_state_agency(dd_cursor, name, state)
  if sa_id is None:
    dd_cursor.execute(QI_STATE_AGENCY, (name, state))
    sa_id = get_state_agency(dd_cursor, name, state)
  return sa_id

'''
Given a person's name, check if it's in DDDB. If not, add.
|dd_cursor|: DDDB database cursor
|first|: First name of Person
|middle|: Middle name of Person
|last|: Last name of Person
'''
def insert_person(dd_cursor, first, middle, last):
  pid = get_person(dd_cursor, first, middle, last)
  if pid is None:
    dd_cursor.execute(QI_PERSON, (first, middle, last))
    pid = get_person(dd_cursor, first, middle, last)
  return pid

'''
Given a "serves on" relationship, check if it's in DDDB. If not, add.
|dd_cursor|: DDDB database cursor
|pid|: person pid
|year|: year of relationship
|agency|: agency sa_id
|position|: person's position in agency
'''
def insert_serves_on(dd_cursor, pid, year, agency, position):
  dd_cursor.execute(QS_SERVES_ON, (pid, year, agency, position))
  if dd_cursor.rowcount == 0:
    dd_cursor.execute(QI_SERVES_ON, (pid, year, agency, position))

to_remove = [
    "m.d",
    "dr",
    "mr",
    "ms",
    "mrs",
    "ph.d",
    "assemblymember",
    "senator"
]
def split_name(full_name):
    names = full_name.split()

    # Strip commas and periods
    names = [name.strip(",.") for name in names]

    # Strip unnecessary words
    names = [x for x in names if x.lower() not in to_remove]

    # Remove parenthesized names
    for i in xrange(len(names)):
        inverse_i = len(names) - i - 1
        if names[inverse_i].startswith("("):
            del names[inverse_i]

    # Combine "de la"
    try:
        lower_names = [name.lower() for name in names]
        de_index = lower_names.index("de")
        la_index = lower_names.index("la")

        if de_index+1 == la_index:
            del names[la_index]
            del names[de_index]
            names[de_index] = "De La " + names[de_index]
    except ValueError: pass

    # Combine "de"
    try:
        lower_names = [name.lower() for name in names]
        de_index = lower_names.index("de")
        del names[de_index]
        names[de_index] = "De " + names[de_index]
    except ValueError: pass

    # Combine "del"
    try:
        lower_names = [name.lower() for name in names]
        del_index = lower_names.index("del")
        del names[del_index]
        names[del_index] = "Del " + names[del_index]
    except ValueError: pass

    # Combine single letter middle names
    single_letters = tuple(x for x in names if len(x) == 1)
    if len(single_letters) > 1:
        names[names.index(single_letters[0])] = " ".join(single_letters)
        for letter in single_letters:
            try: names.remove(letter)
            except ValueError: pass

    # Handle Jr and Sr
    if "Jr" in names:
        names[-2] = names[-2] + " Jr"
        names.remove("Jr")
    if "Sr" in names:
        names[-2] = names[-2] + " Sr"
        names.remove("Sr")

    # Insert blank middle name if necessary
    if len(names) < 3:
        names.insert(1, "")

    out = "%25s" % full_name
    for name in names: out += "%20s" % name
    #print(out)
    return tuple(names)

def extract_members(xlsx_name, verbose=False):
    if verbose: print("Processing file: %s" % (xlsx_name))
    wb = xlrd.open_workbook(xlsx_name)
    sheet_name = "State Agency People"
    sh = wb.sheet_by_name(sheet_name)
    agency_dict = {}

    for rownum in xrange(sh.nrows):
        row_types = sh.row_types(rownum)
        row = sh.row_values(rownum)
        if row[0] == "AGENCY":
            index = 2
            while "Board" in row[index]: index += 1
            end_board_members = index
        elif len(row[0]) > 0:
            agency = row[0]
            board_members = [x.encode("utf-8").strip()
                for x in row[2:end_board_members]
                    if len(x.strip()) > 0]
            staff = [x.encode("utf-8").strip()
                for x in row[end_board_members:]
                    if len(x.strip()) > 0]
            agency_dict[agency] = (board_members, staff)
    return agency_dict

def main():
  xlsx_file = "./sa.xlsx"
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='StateAgencyDB',
                         user='awsDB',
                         passwd='digitaldemocracy789') as dd_cursor:
    # Turn off foreign key checks
    dd_cursor.execute('SET foreign_key_checks = 0')
    agency_dict = extract_members(xlsx_file)

    insert_state(dd_cursor, "CA", "United States", "California")

    for agency,(board,staff) in agency_dict.iteritems():
        sa_id = insert_state_agency(dd_cursor, agency, "CA")
        for member in board:
            pid = insert_person(dd_cursor, *split_name(member))
            insert_serves_on(dd_cursor, pid, 2016, sa_id, "Boardmember")
        for member in staff:
            pid = insert_person(dd_cursor, *split_name(member))
            insert_serves_on(dd_cursor, pid, 2016, sa_id, "Executive Staff")
        
    # Set foreign key checks on afterwards
    dd_cursor.execute('SET foreign_key_checks = 1')

      
if __name__ == '__main__':
  main()
