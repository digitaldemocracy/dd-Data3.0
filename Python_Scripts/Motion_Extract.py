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
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_Motion = "INSERT INTO Motion (mid, date, text) VALUES (%s, %s, %s)";

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn2 = db2.cursor(buffered = True)

def insert_Motion(cursor, mid, date, text):
	select_stmt = "SELECT mid from Motion where mid = %(mid)s AND date = %(date)s"
	cursor.execute(select_stmt, {'mid':mid, 'date':date})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_Motion, (mid, date, text))	
	else:
		#print "Motion mid = {0}, date = {1}, text = {2} Already Exists!".format(mid, date, text)
		pass

try:
	select_count = "SELECT COUNT(*) FROM bill_motion_tbl"
	conn.execute(select_count)
	temp = conn.fetchone()
	a = temp[0]
	print a
	select_stmt = "SELECT * FROM bill_motion_tbl"
	conn.execute(select_stmt)
	for i in range(0, a):
		temp = conn.fetchone()
		if temp:
			mid = temp[0];
			date = temp[3];
			date = date.strftime('%Y-%m-%d %H:%M:%S')
			text = temp[1];
			if(date):
				insert_Motion(conn2, mid, date, text)
	db2.commit()

except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

db.close();
db2.close();
