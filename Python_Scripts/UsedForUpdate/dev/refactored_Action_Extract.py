#!/usr/bin/env python2.6
'''
File: Action_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan
Date: 6/11/2015
Last Changed: 11/20/2015

Description:
- Inserts Actions from the bill_history_tbl from capublic into DDDB.Action
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

import MySQLdb

import loggingdb

# Queries
# INSERTS
qi_Action = '''INSERT INTO Action (bid, date, text)
               VALUES (%s, %s, %s)'''

# SELECTS
qs_bill_history_tbl = '''SELECT bill_id, action_date, action
                         FROM bill_history_tbl'''
qs_Action_check = '''SELECT bid
                     FROM Action
                     WHERE bid = %(bid)s
                      AND date = %(date)s'''

'''
Checks if the Action is in DDDB. If it isn't, insert it. Otherwise, skip.

|dd_cursor|: DDDB database cursor
|bid|: Bill id
|date|: Date of action
|text|: Text of action
'''
def insert_Action(dd_cursor, bid, date, text):
  # Check if DDDB already has this action
  dd_cursor.execute(qs_Action_check, {'bid':bid, 'date':date})

  # If Action not in DDDB, add
  if(dd_cursor.rowcount == 0):
    dd_cursor.execute(qi_Action, (bid, date, text))	

'''
Loops through all Actions from capublic and adds them as necessary
'''
def main():
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='MultiStateTest',
                       user='awsDB',
                       passwd='digitaldemocracy789') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='capublic',
                       passwd='python') as ca_cursor:

      # Get all of the Actions from capublic
      ca_cursor.execute(qs_bill_history_tbl)

      # For each tuple, attempt to add the action to DDDB
      for bid, date, text in ca_cursor.fetchall():
        insert_Action(dd_cursor, bid, date, text)

  if __name__ == "__main__":
    main()
