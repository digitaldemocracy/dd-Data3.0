#!/usr/bin/env python2.6
'''
File: Bill_Extract.py
Author: Daniel Mangin
Modified By: Mandy Chan
Date: 6/11/2015
Last Changed: 11/20/2015

Description:
- Inserts the authors from capublic.bill_tbl into DDDB.Bill and 
  capublic.bill_version_tbl into DDDB.BillVersion
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
    - bill_tbl
    - bill_version_tbl

Populates:
  - Bill (bid, type, number, state, status, house, session)
  - BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes)
'''

import MySQLdb

import loggingdb

# U.S. state
us_state = 'CA'

# Queries
# INSERTS
qi_Bill = '''INSERT INTO Bill
             (bid, type, number, billState, status, house, session, state)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
qi_BillVersion = '''INSERT INTO BillVersion (vid, bid, date, 
                    billState, subject, appropriation, substantive_changes, state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

# SELECTS
qs_Bill_check = '''SELECT bid
                   FROM Bill
                   WHERE bid = %s
                    AND number = %s'''
qs_BillVersion_check = '''SELECT bid
                          FROM BillVersion
                          WHERE vid = %s'''
qs_bill_tbl = '''SELECT bill_id, measure_type, measure_num, measure_state,
                        current_status, current_house, session_num
                 FROM bill_tbl'''
qs_bill_version_tbl = '''SELECT bill_version_id, bill_id, 
                                bill_version_action_date, bill_version_action, 
                                subject, appropriation, substantive_changes 
                         FROM bill_version_tbl'''

'''
Checks if bill exists. If not, adds the bill.

|dd_cursor|: DDDB database cursor
|bid|: Bill id
|type_|: Bill type
|number|: Bill number
|state|: Bill state (billState)
|status|: Bill status
|house|: House (Assembly/Senate/etc.)
|session|: Legislative session
'''
def add_bill(dd_cursor, values):
  dd_cursor.execute(qs_Bill_check, (values[0], values[2]))

  if dd_cursor.rowcount == 0:
    values.append(us_state)
    dd_cursor.execute(qi_Bill, values)

'''
Checks if billVersion exists. If not, adds the BillVersion.

|dd_cursor|: DDDB database cursor
|vid|: Version id
|bid|: Bill id
|date|: Bill version date
|state|: Bill state (billState)
|subject|: Bill subject
|appropriation|: Bill appropriation
|substantive_changes|: Bill changes
'''
def add_bill_version(dd_cursor, values):
  dd_cursor.execute(qs_BillVersion_check, (values[0],))

  if dd_cursor.rowcount == 0:
    values.append(us_state)
    dd_cursor.execute(qi_BillVersion, values)

'''
Gets all of the Bills, then adds them as necessary

|ca_cursor|: capublic database cursor
|dd_cursor|: DDDB database cursor
'''
def get_bills(ca_cursor, dd_cursor):
  ca_cursor.execute(qs_bill_tbl)

  for bid, type_, number, state, status, house, session in ca_cursor.fetchall():
    bid = us_state + '_' + bid

    # Special sessions are marked with an X
    if session != '0':
      type_ = type_ + 'X' + session

    add_bill(dd_cursor, (bid, type_, number, state, status, house, session))

'''
Gets all of the BillVersions, then adds them as necessary

|ca_cursor|: capublic database cursor
|dd_cursor|: DDDB database cursor
'''
def get_bill_versions(ca_cursor, dd_cursor):
  ca_cursor.execute(qs_bill_version_tbl)

  for record in ca_cursor.fetchall():
    # Bill Id keeps track of U.S. state
    record[1] = us_state + '_' + temp[1]

    # Bill status check (Check is necessary; don't know why)
    if record[3] != 0:
      add_bill_version(dd_cursor, record)

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

      get_bills(ca_cursor, dd_cursor)
      get_bill_versions(ca_cursor, dd_cursor)

if __name__ == "__main__":
  main()
