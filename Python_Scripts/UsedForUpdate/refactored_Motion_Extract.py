#!/usr/bin/env python
'''
File: Motion_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers the Motions from capublic.bill_motion_tbl and inserts the Motions into DDDB2015Apr.Motion
- Used in the daily update of DDDB2015Apr
- Fills table:
	Motion (mid, date, text)

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
	- bill_motion_tbl
'''

import re
import sys
import time
import loggingdb
import MySQLdb
from pprint import pprint
from urllib import urlopen

query_insert_Motion = "INSERT INTO Motion (mid, date, text) VALUES (%s, %s, %s)";

def insert_Motion(cursor, mid, date, text):
	select_stmt = "SELECT mid from Motion where mid = %(mid)s AND date = %(date)s"
	cursor.execute(select_stmt, {'mid':mid, 'date':date})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_Motion, (mid, date, text))	
	else:
		#print "Motion mid = {0}, date = {1}, text = {2} Already Exists!".format(mid, date, text)
		pass

def getMotions():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python') as ca_cursor:
    with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2015July',
                         user='awsDB',
                         passwd='digitaldemocracy789') as dddb_cursor:
      ca_cursor.execute('''SELECT motion_id, motion_text, trans_update
                           FROM bill_motion_tbl''')
      rows = ca_cursor.fetchall()
      for (mid, text, update) in rows:
        date = update.strftime('%Y-%m-%d %H:%M:%S')
        if(date):
          insert_Motion(dddb_cursor, mid, date, text)

if __name__ == "__main__":
	getMotions()	
