#!/usr/bin/env python
'''
File: legislator_migrate.py
Author: ???
Modified By: Mandy Chan, Steven Thon
Created: 6/11/2015
Last Changed: 11/25/2015

Description:
- Gathers Legislator Data from capublic.legislator_tbl and inserts the data into
  DDDB.Person, DDDB.Legislator, and DDDB.Term
- Used in the daily update of DDDB

Sources:
  - Leginfo (capublic)
    - Pubinfo_2015.zip
    - Pubinfo_Mon.zip
    - Pubinfo_Tue.zip
    - Pubinfo_Wed.zip
    - Pubinfo_Thu.zip
    - Pubinfo_Fri.zip
    - Pubinfo_Sat.zip

  - capublic
    - legislator_tbl

Populates:
  - Person (last, first)
  - Legislator (pid, state)
  - Term (pid, year, district, house, party, state)
'''

import loggingdb
import MySQLdb
from Name_Fixes_Legislator_Migrate import clean_name_legislator_migrate

# U.S. State
STATE = 'CA'

# Queries
QS_CPUB_LEGISLATOR = '''SELECT last_name, first_name,
                         SUBSTRING(session_year, 1, 4),
                         CONVERT(SUBSTRING(district, -2), UNSIGNED), house_type,
                         party, active_legislator 
                        FROM legislator_tbl'''
QS_PERSON = '''SELECT pid
               FROM Person
               WHERE last = %s
                AND first = %s'''
QS_TERM = '''SELECT *
             FROM Term
             WHERE pid = %s
              AND year = %s AND state = %s'''
QS_LEGISLATOR = '''SELECT pid
                   FROM Legislator
                   WHERE pid = %s AND state = %s'''
QI_PERSON = '''INSERT INTO Person (last, first) 
               VALUES (%s, %s)'''
QI_LEGISLATOR = '''INSERT INTO Legislator (pid, state) 
                   VALUES (%s, %s)'''
QI_TERM = '''INSERT INTO Term (pid, year, district, house, party, state) 
             VALUES (%s, %s, %s, %s, %s, %s)'''

# Dictionaries
_HOUSE = {
  'A':'Assembly',
  'S':'Senate'
}

_PARTY = {
  'REP':'Republican',
  'DEM':'Democrat'
}

'''
Checks if there's a legislator with this pid
'''
def check_legislator_pid(cursor, pid):
   print('Checking legislator pid = {0} from state = {1}...'.format(pid, STATE))
   result = cursor.execute(QS_LEGISLATOR, (pid, STATE))
   return cursor.fetchone()

'''
Checks if there's a legislator with this term year
'''
def check_term(cursor, pid, year, district, house):
   print('pid={0},year={1},district={2},house={3},state={4}'.format(
     pid,year,district,house,STATE))
   cursor.execute(QS_TERM, (pid, year, STATE))
   return cursor.fetchone()

'''
Checks if there's a person with this first and last name
'''
def check_name(cursor, last, first):
   name = clean_name_legislator_migrate(last, first).split('<SPLIT>')
   first = name[0]
   last = name[1]
   cursor.execute(QS_PERSON, (last, first))
   return cursor.fetchone()

'''
Gets the legislators from Leginfo (capublic DB) and migrate them to DDDB
'''
def migrate_legislators(ca_cursor, dd_cursor):
  ca_cursor.execute(QS_CPUB_LEGISLATOR)

  # Check each legislator in capublic
  for (last, first, year, district, house, party, active) in ca_cursor:
    house = _HOUSE.get(house)
    party = _PARTY.get(party)
    exist = check_name(dd_cursor, last, first)

    # If this legislator isn't in DDDB, add them to Person table
    if exist is None:
      print 'New Member: {0} {1}'.format(first, last)
      dd_cursor.execute(QI_PERSON, (last, first))

      # If this is an active legislator, add them to Legislator and Term too
      if active == 'Y':
        pid = dd_cursor.lastrowid
        print('Inserting Legislator: %s %s %s %s %s %s' %
              (pid, year, district, house, party, STATE))
        dd_cursor.execute(QI_LEGISLATOR, (pid, STATE))
        dd_cursor.execute(QI_TERM, (year, district, house, party, pid, STATE))

    # If this legislator is in DDDB, check if they're also in the Legislator 
    # and Term tables if they're active.
    else:
      pid = exist[0]
      result = check_legislator_pid(dd_cursor, pid)
      if result is None and active == 'Y':
        dd_cursor.execute(QI_LEGISLATOR, (pid, STATE))
      result = check_term(dd_cursor, pid, year, district, house)
      if result is None and active == 'Y':
        result = dd.execute(QI_TERM, (pid, year, district, house, party, STATE))

def main():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python') as ca_cursor:
    with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                           port=3306,
                           db='DDDB2015Dec',
                           user='awsDB',
                           passwd='digitaldemocracy789') as dd_cursor:
      migrate_legislators(ca_cursor, dd_cursor)

if __name__ == "__main__":
  main()
