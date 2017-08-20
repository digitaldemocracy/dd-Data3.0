'''
File: Fix_Extraordinary_Sessions.py
Author: Mandy Chan
Date: 7/20/2015

Problem:
  - Bill type needs a way to indicate special sessions (such as ABX1)

Solution:
  - Fixed 'Bill_Extract.py' to modify the type to indicate special sessions 
    by concatenating the type with session if it is a special session
  - Using this script as a (hopefully) one-time run through of DDDB2015Apr's 
    database to update Bill table

Affects:
  - DDDB
      - Bill(type)

'''

import sys
import MySQLdb
from pprint import pprint

# Queries
# Finds all bills that are extraordinary sessions
select_stmt = '''SELECT bid, type, session
                 FROM Bill
                 WHERE session != 0;
              '''

# Updates bill types as appropriate
update_stmt = '''UPDATE Bill
                 SET type = %(type)s
                 WHERE bid = %(bid)s;
              '''

def fix(dd_cursor):
  dd_cursor.execute(select_stmt)
  rows = dd_cursor.fetchall()

  for row in rows:
    bid, type, session = row
    type = type + 'X' + str(session)

    dd_cursor.execute(update_stmt, {'type':type, 'bid':bid})

def main():
  print('Fixing Bill type: Extraordinary Sessions')
  with MySQLdb.connect(user='root',
                       db='DDDB2015Apr',
                       passwd='') as dd_cursor:
    fix(dd_cursor)
  print('Fixed')

if __name__ == "__main__":
  main()
