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

US_STATE = 'CA'

# INSERTS
QI_BILL = '''INSERT INTO Bill
             (bid, type, number, billState, status, house, session, state,
              sessionYear)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
QI_BILLVERSION = '''INSERT INTO BillVersion (vid, bid, date, billState, 
                    subject, appropriation, substantive_changes, state)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

# SELECTS
QS_BILL_CHECK = '''SELECT bid
                   FROM Bill
                   WHERE bid = %s
                    AND number = %s'''
QS_BILLVERSION_CHECK = '''SELECT bid
                          FROM BillVersion
                          WHERE vid = %s'''
QS_BILL_TBL = '''SELECT bill_id, measure_type, measure_num, measure_state,
                        current_status, current_house, session_num
                 FROM bill_tbl'''
QS_BILL_VERSION_TBL = '''SELECT bill_version_id, bill_id, 
                                bill_version_action_date, bill_version_action, 
                                subject, appropriation, substantive_changes 
                         FROM bill_version_tbl'''

'''
Checks if bill exists. If not, adds the bill.

|dd_cursor|: DDDB database cursor
|values|: Tuple that includes the following (in order):
  |bid|: Bill id
  |type_|: Bill type
  |number|: Bill number
  |billState|: Bill state
  |status|: Bill status
  |house|: House (Assembly/Senate/etc.)
  |session|: Legislative session
  |state|: U.S. state the bill resides in
  |sessionYear|: The year the bill was created
'''
def add_bill(dd_cursor, values):
  dd_cursor.execute(QS_BILL_CHECK, (values[0], values[2]))

  if dd_cursor.rowcount == 0:
    dd_cursor.execute(QI_BILL, values)

'''
Checks if BillVersion exists. If not, adds the BillVersion.

|dd_cursor|: DDDB database cursor
|values|: Tuple that includes the following (in order):
  |vid|: Version id
  |bid|: Bill id
  |date|: Bill version date
  |billState|: Bill state
  |subject|: Bill subject
  |appropriation|: Bill appropriation
  |substantive_changes|: Bill changes
  |state|: U.S. state the bill resides in
'''
def add_bill_version(dd_cursor, values):
  dd_cursor.execute(QS_BILLVERSION_CHECK, (values[0],))

  if dd_cursor.rowcount == 0:
    dd_cursor.execute(QI_BILLVERSION, values)

'''
Gets all of the Bills, then adds them as necessary

|ca_cursor|: capublic database cursor
|dd_cursor|: DDDB database cursor
'''
def get_bills(ca_cursor, dd_cursor):
  ca_cursor.execute(QS_BILL_TBL)

  for bid, type_, number, state, status, house, session in ca_cursor.fetchall():
    # Session year is taken from bid: Ex: [2015]20160AB1 
    session_yr = bid[:4]
    # Bill Id keeps track of U.S. state
    bid = '%s_%s' % (US_STATE, bid)

    # Special sessions are marked with an X
    if session != '0':
      type_ = '%sX%s' % (type_, session)

    add_bill(dd_cursor, 
       (bid, type_, number, state, status, house, session, US_STATE,
       session_yr))

'''
Gets all of the BillVersions, then adds them as necessary.

|ca_cursor|: capublic database cursor
|dd_cursor|: DDDB database cursor
'''
def get_bill_versions(ca_cursor, dd_cursor):
  ca_cursor.execute(QS_BILL_VERSION_TBL)

  for record in ca_cursor.fetchall():
    # Change to list for mutability
    record = list(record)
    # Bill and Version Id keeps track of U.S. state
    record[0] = '%s_%s' % (US_STATE, record[0])
    record[1] = '%s_%s' % (US_STATE, record[1])
    # Appropriation is 'Yes' or 'No' in capublic, but an int in DDDB.
    if record[5] is not None: 
      record[5] = 0 if record[5] == 'No' else 1
    record.append(US_STATE)

    # Bill status check (Check is necessary; don't know why)
    if record[3] != 0:
      add_bill_version(dd_cursor, record)

def main():
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2015Dec',
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
