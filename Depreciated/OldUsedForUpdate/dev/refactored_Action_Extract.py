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

STATE = 'CA'

# INSERTS
QI_ACTION = '''INSERT INTO Action (bid, date, text)
               VALUES (%s, %s, %s)'''

# SELECTS
QS_BILL_HISTORY_TBL = '''SELECT bill_id, action_date, action
                         FROM bill_history_tbl'''
QS_ACTION_CHECK = '''SELECT bid
                     FROM Action
                     WHERE bid = %s
                      AND date = %s'''

'''
Checks if the Action is in DDDB. If it isn't, insert it. Otherwise, skip.

|dd_cursor|: DDDB database cursor
|values|: Tuple that includes the following (in order):
  |bid|: Bill id
  |date|: Date of action
  |text|: Text of action
'''
def insert_Action(dd_cursor, values):
  values[0] = '%s_%s' % (STATE, values[0])

  # Check if DDDB already has this action
  dd_cursor.execute(QS_ACTION_CHECK, (values[0], values[1]))

  # If Action not in DDDB, add
  if(dd_cursor.rowcount == 0):
    dd_cursor.execute(QI_ACTION, values)	

'''
Loops through all Actions from capublic and adds them as necessary
'''
def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='MultiStateTest',
                       user='awsDB',
                       passwd='digitaldemocracy789') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='capublic',
                       passwd='python') as ca_cursor:

      # Get all of the Actions from capublic
      ca_cursor.execute(QS_BILL_HISTORY_TBL)

      for record in ca_cursor.fetchall():
        insert_Action(dd_cursor, list(record))

  if __name__ == "__main__":
    main()
