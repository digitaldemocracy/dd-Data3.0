#!/usr/bin/env python2.6
'''
File: Bill_Extract.py
Author: Daniel Mangin
Modified By: Mandy Chan
Date: 6/11/2015

Description:
- Inserts the authors from capublic.bill_tbl into DDDB2015Apr.Bill and capublic.bill_version_tbl into DDDB2015Apr.BillVersion
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

-capublic
  - bill_tbl
  - bill_version_tbl

Populates:
  - Bill (bid, type, number, state, status, house, session)
  - BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes)
'''

import re
import sys
import MySQLdb
from pprint import pprint
from urllib import urlopen

# Queries
insert_bill_stmt = '''INSERT INTO Bill
                      (bid, type, number, state, status, house, session)
                      VALUES (%s, %s, %s, %s, %s, %s, %s);'''
insert_billversion_stmt = '''INSERT INTO BillVersion (vid, bid, date, 
                             state, subject, appropriation, substantive_changes)
                             VALUES (%s, %s, %s, %s, %s, %s, %s);'''

'''
#connections to database
db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015AprTest', password = '')
conn2 = db2.cursor(buffered = True)
'''

# Checks if bill exists, if not, adds the bill
def addBill(cursor, bid, type, number, state, status, house, session):
  select_stmt = "SELECT bid from Bill where bid = %(bid)s AND number = %(number)s"
  cursor.execute(select_stmt, {'bid':bid,'number':number})
  if(cursor.rowcount == 0):
    print "adding Bill {0}".format(bid)
    cursor.execute(insert_bill_stmt, (bid, type, number, state, status, house, session))

# Checks if billVersion exists, if not, adds the billVersion
def addBillVersion(cursor, vid, bid, date, state, subject, appropriation, substantive_changes):
  select_stmt = "SELECT bid from BillVersion where vid = %(vid)s"
  cursor.execute(select_stmt, {'vid':vid})
  if(cursor.rowcount == 0):
    print "adding BillVersion {0}".format(vid)
    cursor.execute(insert_billversion_stmt, (vid, bid, date, state, subject, appropriation, substantive_changes))

# Finds the state of the bill
# Used as a helper for finding BillVersions
def findState(cursor, bid):
  select_stmt = "SELECT state from Bill where bid = %(bid)s"
  cursor.execute(select_stmt, {'bid':bid})
  temp = [0]
  if cursor.rowcount > 0:
    temp = cursor.fetchone()
  return temp[0]

# Gets all of the Bills, then adds them as necessary
def getBills(ca_cursor, dd_cursor):
  select_stmt = "SELECT * FROM bill_tbl"
  ca_cursor.execute(select_stmt)
  for i in range(0, ca_cursor.rowcount):
    temp = ca_cursor.fetchone()
    bid = temp[0]
    number = temp[4]
    status = temp[17]
    session = temp[2]
    type = temp[3]

    if (session != 0):
      type = type = 'X' + str(session)

    house = temp[16]
    state = temp[5]
    addBill(dd_cursor, bid, type, number, state, status, house, session)
  '''
  try:
    select_stmt = "SELECT * FROM bill_tbl"
    conn.execute(select_stmt)
    for i in range(0, conn.rowcount):
      temp = conn.fetchone()
      bid = temp[0]
      number = temp[4]
      status = temp[17]
      session = temp[2]
      type = temp[3]
      house = temp[16]
      state = temp[5]
      addBill(conn2, bid, type, number, state, status, house, session)
    db2.commit()
    
  except:
    db2.rollback()
    print 'error!', sys.exc_info()[0], sys.exc_info()[1]
    exit()
  '''

# Gets all of the BillVersions then adds them as necessary
def getBillVersions(ca_cursor, dd_cursor):
  select_stmt = "SELECT * FROM bill_version_tbl"
  ca_cursor.execute(select_stmt)
  print 'versions', ca_cursor.rowcount
  for i in range(0, ca_cursor.rowcount):
    temp = ca_cursor.fetchone()
    if temp:
      vid = temp[0]
      bid = temp[1]
      date = temp[3]
      state = temp[4]
      subject = temp[6]
      appropriation = temp[8]
      substantive_changes = temp[11]
      if state != 0:
        addBillVersion(dd_cursor, vid, bid, date, state, subject, appropriation, substantive_changes)
  '''
  try:
    select_stmt = "SELECT * FROM bill_version_tbl"
    conn.execute(select_stmt)
    print 'versions', conn.rowcount
    for i in range(0, conn.rowcount):
      temp = conn.fetchone()
      if temp:
        vid = temp[0]
        bid = temp[1]
        date = temp[3]
        state = temp[4]
        subject = temp[6]
        appropriation = temp[8]
        substantive_changes = temp[11]
        if state != 0:
          addBillVersion(conn2, vid, bid, date, state, subject, appropriation, substantive_changes)
    db2.commit()
  except:
    print "Something happened!"
    db2.rollback()
    print 'error!', sys.exc_info()[0], sys.exc_info()[1]
    exit()
  '''

def main():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='DDDB2015July',
                       passwd='python') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='capublic',
                         passwd='python') as ca_cursor:
      print "getting Bills"
      getBills(ca_cursor, dd_cursor)
      print "getting Bill Versions"
      getBillVersions(ca_cursor, dd_cursor)
      print "Closing Database Connections"
  #db.close()
  #db2.close()

if __name__ == "__main__":
  main()

