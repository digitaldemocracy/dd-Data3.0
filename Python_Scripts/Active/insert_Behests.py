#!/usr/bin/env python2.7
'''
File: insert_Behests.py
Author: Mandy Chan
Date: 7/18/2015

Description:
  - Gathers Behest Data and puts it into DDDB

NOTE: If you want to use this and you grab the data from Windows and vim shows 
      ^M instead of line breaks, use the following command:
      
      :%s/^V^M/\r/g (where ^V^M means CTRL+V, CTRL+M)

      in order to replace all instances of ^M with newline characters. 
      Otherwise, this script will think there is only one line in the file.

Usage:
  python insert_Behests.py [file_name.csv]
  - file_name.csv : a .csv file containing the Behests

Source:
  - California Fair Political Practices Commission (fppc.ca.gov/index.php?id=499)
    - YYYY-Senate.csv
    - YYYY-Assembly.csv

Populates:
  - Behests (official, datePaid, payor, amount, payee, description, purpose, noticeReceived)
  - Organizations (name, city, state)
  - Payors (name, city, state)

'''

import re
import sys
import csv
import MySQLdb
from pprint import pprint
from datetime import datetime

# Global Data
# Number of cmdline arguments needed
NUM_ARGS = 2

# Column names dictionary
COL = {
  'official':0, 'pay_date':1, 'payor':2,       'payor_city':3, 'payor_state':4,
  'amount':5,   'payee':6,    'payee_city':7,  'payee_state':8,
  'descr':9,    'purpose':10, 'notice_rec':11, 'ytd':12
}
'''
# Column names in csv. Change accordingly, but be sure each column name is unique.
COL_NAMES = ['OFFICIAL', 'DATE OF PAYMENT', 'PAYOR', 'PAYOR_CITY', 'PAYOR_STATE',
    'AMOUNT', 'PAYEE', 'PAYEE_CITY', 'PAYEE_STATE', 'DESCRIPTION', 
    'LGC PURPOSE', 'NOTICE RECEIVED', 'YTD AMOUNT BY MEMBER']
'''
# Dictionary of problematic legislator names. Add as necessary.
NAME_EXCEPTIONS = {
    "Allen, Ben":"Allen, Benjamin",
    "Jackson, Hanna- Beth":"Jackson, Hannah-Beth",
}

'''
Finds the pid of the official. If found, returns tuple (name, pid). Otherwise,
(name, -1).
'''
def find_official(dd_cursor, name):
  # Check and refactor legislator name if necessary
  name = NAME_EXCEPTIONS.get(name, name)

  # Find legislator pid from name
  select_stmt = '''SELECT l.pid
                   FROM Person p
                   JOIN Legislator l ON p.pid = l.pid
                   WHERE CONCAT_WS(', ',last,first) = %(name)s;
                '''
  dd_cursor.execute(select_stmt, {'name':name})

  # Returns the pid if found, otherwise -1
  query = dd_cursor.fetchone()
  if query is None:
    return (name, -1)
  return (name, query[0])

'''
Creates a row in Organizations table.
'''
def create_organization(dd_cursor, org, city, state):
  insert_stmt = '''INSERT INTO Organizations
                   (name, city, state)
                   VALUES
                   (%(name)s, %(city)s, %(state)s);
                '''
  dd_cursor.execute(insert_stmt, {'name':org, 'city':city, 'state':state})
  return dd_cursor.lastrowid

'''
Finds organization from the organization name, city, and state. If found, 
returns the organization's 'oid'. Otherwise, creates an organization row and 
returns the newly created 'oid'.
'''
def find_organization(dd_cursor, org, city, state):
  select_stmt = '''SELECT oid
                   FROM Organizations
                   WHERE name = %(name)s
                    AND city = %(city)s
                    AND state = %(state)s
                '''
  dd_cursor.execute(select_stmt, {'name':org, 'city':city, 'state':state})
  query = dd_cursor.fetchone()
  if query is None:
    return create_organization(dd_cursor, org, city, state)
  return query[0]

'''
Creates a row in Payors table.
'''
def create_payor(dd_cursor, payor, city, state):
  insert_stmt = '''INSERT INTO Payors (name, city, state)
                   VALUES (%(name)s, %(city)s, %(state)s);
                '''
  dd_cursor.execute(insert_stmt, {'name':payor, 'city':city, 'state':state})
  return dd_cursor.lastrowid

'''
Finds payor from the payor name, city, and state. If found, returns the payor's
'paid'. Otherwise, create a payor row and return the newly created 'paid'.
'''
def find_payor(dd_cursor, payor, city, state):
  select_stmt = '''SELECT paid
                   FROM Payors
                   WHERE name = %(name)s
                    AND city = %(city)s
                    AND state = %(state)s
                '''
  dd_cursor.execute(select_stmt, {'name':payor, 'city':city, 'state':state})
  query = dd_cursor.fetchone()
  if query is None:
    return create_payor(dd_cursor, payor, city, state)
  return query[0]

'''
Check if there is already a behest with the same exact information
'''
def duplicate_behest(dd_cursor,
    off_pid, date_paid, payor_id, amt, payee_id, descr, purpose, notice_rec):
  select_stmt = '''SELECT *
                   FROM Behests
                   WHERE official = %(off_pid)s
                    AND datePaid = %(date)s AND payor = %(payor)s
                    AND amount = %(amt)s AND payee = %(payee)s
                    AND description = %(descr)s AND purpose = %(purpose)s
                    AND noticeReceived = %(notice_rec)s
                '''
  dd_cursor.execute(select_stmt, {'off_pid':off_pid, 'date':date_paid, 
    'payor':payor_id, 'amt':amt, 'payee':payee_id, 'descr':descr,
    'purpose':purpose, 'notice_rec':notice_rec})

  query = dd_cursor.fetchone()
  if query is None:
    return False
  return True

'''
Insert row into Behests
'''
def create_behest(dd_cursor,
    off_pid, date_paid, payor_id, amt, payee_id, descr, purpose, notice_rec):
  if duplicate_behest(dd_cursor, off_pid, date_paid, payor_id, amt, payee_id,
      descr, purpose, notice_rec):
    return -1

  insert_stmt = '''INSERT INTO Behests
                   VALUES
                   (%(off_pid)s, %(datePaid)s, %(payor_id)s, %(amount)s, 
                   %(payee_id)s, %(descr)s, %(purpose)s, %(notice_rec)s)
                '''  
  dd_cursor.execute(insert_stmt, {'off_pid':off_pid, 'datePaid':date_paid,
    'payor_id':payor_id, 'amount':amt, 'payee_id':payee_id, 'descr':descr,
    'purpose':purpose, 'notice_rec':notice_rec})

'''
Parse the row and insert the information to DDDB. It then returns the current 
official because the Behest files don't have the officials mentioned every 
line. (see Behest data)
'''
def parse_row(dd_cursor, attribs, official):
  # Assume if Official and Payor are blank, the line is unnecessary
  if attribs[COL['official']] == '' and attribs[COL['payor']] == '':
    return official
  # If legislator/official name is present in this row, find their pid
  elif attribs[COL['official']] != '':
    official = find_official(dd_cursor, attribs[COL['official']].strip())
  
  name = official[0]
  pid = official[1]

  # If official isn't found, return without inserting anything
  if pid < 0:
    print('Could not find Legislator: %(name)s, skipping' % {'name':name})
    return (name, pid)

  # Find Payor id from Payors table
  payor_id = find_payor(dd_cursor,
      attribs[COL['payor']].strip(), attribs[COL['payor_city']].strip(),
      attribs[COL['payor_state']].strip())

  # Find Payee id from Organizations table 
  payee_id = find_organization(dd_cursor,
      attribs[COL['payee']].strip(), attribs[COL['payee_city']].strip(),
      attribs[COL['payee_state']].strip())

  # Rest of variables
  date_paid = (datetime.strptime(attribs[COL['pay_date']], '%m/%d/%y').
      strftime('%Y-%m-%d'))
  amount = attribs[COL['amount']].replace(',', '')
  descr = attribs[COL['descr']]
  purpose = attribs[COL['purpose']]
  notice_rec = (datetime.strptime(attribs[COL['notice_rec']], '%m/%d/%y').
      strftime('%Y-%m-%d'))

  create_behest(dd_cursor, pid, date_paid, payor_id, amount, payee_id, descr, 
      purpose, notice_rec)
  return official

'''
Check the arguments passed in for any initial errors and raise an error if so
'''
def check_args(args):
  # Check number of arguments
  if len(args) != NUM_ARGS:
    print('usage: python insert_Behests2015.py [file_name.csv] [header_line_num]')
    print('Ex: python insert_Behests2015.py 2015-Senate.csv 6')
    raise Exception('Bad arguments')
  # Check if file is .csv
  elif (args[1]).split('.')[1] != 'csv':
    print('Please use .csv file')
    raise Exception('Bad arguments')

  '''
  # Check if second argument is a positive integer
  try:
    num = int(args[2])
    if (num < 0):
      print('Please put a positive integer as the second argument')
      raise Exception('Bad arguments')
  except ValueError:
    print('Please put a valid integer as the second argument')
    raise Exception('Bad arguments')
  '''

def main():
  # Current Official (as (name, pid) tuple)
  cur_official = ('', -1)

  # Check arguments
  check_args(sys.argv)
  file_name = sys.argv[1]
  #start_line = int(sys.argv[2])

  # Opening db connection
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='DDDB2015July',
                       passwd='python') as dd_cursor:

    # Opening file and start at the header_line number
    with open(file_name, 'r') as f:
      '''
      while start_line - 1 > 0:
        f.readline()
        header_line -= 1
      '''

      reader = csv.reader(f, skipinitialspace=True)
      for row in reader:
        '''
        # Check if the row below headers has an official's name
        if reader.line_num == 1 and row[COL['official']] == '':
          print('First line does not have an official name')
          raise 'Bad starting line'
        '''

        # TESTING PURPOSES
        #print('%(line)s: %(attributes)s' % {'line':reader.line_num, 'attributes':row})
        #print('%(official)s' % {'official': cur_official})

        cur_official = parse_row(dd_cursor, row, cur_official)

if __name__ == "__main__":
  main()
