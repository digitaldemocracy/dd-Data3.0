#!/usr/bin/env python27

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

from unidecode import unidecode
from slugify import slugify, Slugify, slugify_unicode
from Database_Connection import mysql_connection
import traceback
import sys
import MySQLdb
from Name_Fixes_Legislator_Migrate import clean_name_legislator_migrate
from graylogger.graylogger import GrayLogger
from datetime import datetime

logger = None
P_INSERT = 0
L_INSERT = 0
T_INSERT = 0
T_UPDATE = 0

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
QI_TERM = '''INSERT INTO Term (pid, year, district, house, party, state, start) 
             VALUES (%s, %s, %s, %s, %s, %s, %s)'''
QI_PERSON_STATE = '''INSERT INTO PersonStateAffiliation (pid, state)
                     VALUES (%s, "CA")'''
QU_TERM_END_DATE = '''UPDATE Term
                      SET end = %s, current_term = 0
                      WHERE pid = %s
                       AND year = %s
                       AND end IS NULL'''

# Dictionaries
_HOUSE = {
  'A':'Assembly',
  'S':'Senate'
}

_PARTY = {
  'REP':'Republican',
  'DEM':'Democrat'
}

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA',
      '_log_type':'Database'
  }

'''
Checks if there's a legislator with this pid
'''
def check_legislator_pid(cursor, pid):
   #print('Checking legislator pid = {0} from state = {1}...'.format(pid, STATE))
   result = cursor.execute(QS_LEGISLATOR, (pid, STATE))
   return cursor.fetchone()

'''
Checks if there's a legislator with this term year
'''
def check_term(cursor, pid, year, district, house):
   #print('pid={0},year={1},district={2},house={3},state={4}'.format(
   #  pid,year,district,house,STATE))
   cursor.execute(QS_TERM, (pid, year, STATE))
   return cursor.fetchone()

'''
Checks if there's a person with this first and last name
'''
def check_name(cursor, last, first):
   name = clean_name_legislator_migrate(last, first).split('<SPLIT>')
   first = name[0]
   last = name[1]

   if 'Reginald Byron' in first:
      first = 'Reginald'

   cursor.execute(QS_PERSON, (last, first))
   return cursor.fetchone()

'''
Gets the legislators from Leginfo (capublic DB) and migrate them to DDDB
'''
def migrate_legislators(ca_cursor, dd_cursor):
  global P_INSERT, L_INSERT, T_INSERT, T_UPDATE
  ca_cursor.execute(QS_CPUB_LEGISLATOR)
  date = datetime.now().strftime('%Y-%m-%d')

  # Check each legislator in capublic
  for (last, first, year, district, house, party, active) in ca_cursor:
    house = _HOUSE.get(house)
    party = _PARTY.get(party)
    last = unidecode(last)
    first = unidecode(first)
    print(last, first)
    exist = check_name(dd_cursor, last, first)
    if last == 'Reyes':
      print(last, first, exist)

    # If this legislator isn't in DDDB, add them to Person table
    if exist is None:
      print('   ', type(first), type(last))
      print('       ', unidecode(first), unidecode(last))
      #logger.info('New Member: first: {0} last: {1}'.format(first, last))
      try:
        dd_cursor.execute(QI_PERSON, (last, first))
        P_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Person', (QI_PERSON % (last, first))))

      # If this is an active legislator, add them to Legislator and Term too
      if active == 'Y':
        pid = dd_cursor.lastrowid
        logger.info(('Inserting Legislator: %s %s %s %s %s %s' %
              (pid, year, district, house, party, STATE)))
        try:
          dd_cursor.execute(QI_LEGISLATOR, (pid, STATE))
          L_INSERT += dd_cursor.rowcount
        except MySQLdb.Error:
           logger.warning('Insert Failed', full_msg=traceback.format_exc(),
               additional_fields=create_payload('Legislator' % (QI_LEGISLATOR, (pid, STATE))))
        try:
          dd_cursor.execute(QI_TERM, (pid, year, district, house, party, STATE, date))
          T_INSERT += dd_cursor.rowcount
        except MySQLdb.Error:
          logger.warning('Insert Failed', full_msg=traceback.format_exc(),
              additional_fields=create_payload('Term', 
                (QI_TERM % (pid, year, district, house, party, STATE, date))))

    # If this legislator is in DDDB, check if they're also in the Legislator 
    # and Term tables if they're active.
    else:
      pid = exist[0]
      result = check_legislator_pid(dd_cursor, pid)
      if result is None and active == 'Y':
        try:
          dd_cursor.execute(QI_LEGISLATOR, (pid, STATE))
          L_INSERT += dd_cursor.rowcount
        except MySQLdb.Error:
          logger.warning('Insert Failed', full_msg=traceback.format_exc(),
              additional_fields=create_payload('Legislator', (QI_LEGISLATOR % (pid, STATE))))
      result = check_term(dd_cursor, pid, year, district, house)
      if result is None and active == 'Y':
        try:
          result = dd_cursor.execute(QI_TERM, (pid, year, district, house, party, STATE, date))
          T_INSERT += dd_cursor.rowcount
        except MySQLdb.Error:
          logger.warning('Insert Failed', full_msg=traceback.format_exc(),
              additional_fields=create_payload('Term',
                (QI_TERM % (pid, year, district, house, party, STATE, date))))
      if result is not None and active == 'N':
        try:
          dd_cursor.execute(QU_TERM_END_DATE, (date, pid, year))
          T_UPDATE += dd_cursor.rowcount
        except MySQLdb.Error:
          logger.warning('Update Failed', full_msg=taceback.format_exc(),
              additional_fields=create_payload('Term',
                (QU_TERM_END_DATE % (date, pid, year))))

def main():
  dbinfo = mysql_connection(sys.argv)
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python',
                       charset='utf8') as ca_cursor:
    with MySQLdb.connect(host=dbinfo['host'],
                           port=dbinfo['port'],
                           db=dbinfo['db'],
                           user=dbinfo['user'],
                           passwd=dbinfo['passwd'],
                           charset='utf8') as dd_cursor:
      migrate_legislators(ca_cursor, dd_cursor)
      #raise TypeError
      logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(P_INSERT) + ' rows in Person, inserted ' +
                   str(L_INSERT) + ' rows in Legislator and inserted '
                    + str(T_INSERT) + ' rows and updated ' + str(T_UPDATE) + ' rows in Term',
          additional_fields={'_affected_rows':'Person:'+str(P_INSERT)+
                                         ', Legislator:'+str(L_INSERT)+
                                         ', Term:'+str(T_INSERT + T_UPDATE),
                             '_inserted':'Person:'+str(P_INSERT)+
                                         ', Legislator:'+str(L_INSERT)+
                                         ', Term:'+str(T_INSERT),
                             '_updated':'Term:'+str(T_UPDATE),
                             '_state':'CA',
                             '_log_type':'Database'})

if __name__ == "__main__":
  with GrayLogger('http://dw.digitaldemocracy.org:12202/gelf') as _logger:
    logger = _logger
    main()
