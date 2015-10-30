#!/usr/bin/env python2.6
'''
File: Action_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan
Date: 6/11/2015
Last Changed: 10/29/2015

Description:
- Inserts Actions from the bill_history_tbl from capublic into DDDB2015Apr.Action
- This script runs under the update script

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
    - bill_history_tbl

Populates:
  - Action (bid, date, text)

'''

import re
import sys
import time
import loggingdb
import MySQLdb
from pprint import pprint
from urllib import urlopen

# Queries
query_insert_Action = '''INSERT INTO Action (bid, date, text)
                         VALUES (%s, %s, %s);'''

'''
Checks if the Action is in DDDB. If it isn't, insert it. Otherwise, skip.
'''
def insert_Action(cursor, bid, date, text):
  # Check
  select_stmt = "SELECT bid from Action where bid = %(bid)s AND date = %(date)s"
  cursor.execute(select_stmt, {'bid':bid, 'date':date})

  # If Action not in DDDB, add
  if(cursor.rowcount == 0):
    cursor.execute(query_insert_Action, (bid, date, text))	

'''
Loops through all Actions from capublic and adds them as necessary
'''
def main():
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='DDDB2015July',
                       user='awsDB',
                       passwd='digitaldemocracy789') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='capublic',
                       passwd='python') as ca_cursor:

      # Get all of the Actions from capublic
      select_stmt = '''SELECT bill_id, action_date, action
                       FROM bill_history_tbl'''
      ca_cursor.execute(select_stmt)
      for i in range(0, ca_cursor.rowcount):
          tuple = ca_cursor.fetchone()
          if tuple:
            bid = tuple[0];
            date = tuple[1];
            text = tuple[2];
            if(bid):
              insert_Action(dd_cursor, bid, date, text)

if __name__ == "__main__":
	main()
