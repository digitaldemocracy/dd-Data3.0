'''
File: Bill_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Inserts the authors from capublic.bill_tbl into DDDB2015Apr.Bill and capublic.bill_version_tbl into DDDB2015Apr.BillVersion
- This script runs under the update script
- Fills table:
	Bill (bid, type, number, state, status, house, session)
	BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes)

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
'''

import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

#queries used in the script
query_insert_Bill = "INSERT INTO Bill (bid, type, number, state, status, house, session) VALUES (%s, %s, %s, %s, %s, %s, %s);"
query_insert_Bill_Version = "INSERT INTO BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes) VALUES (%s, %s, %s, %s, %s, %s, %s);"

#connections to database
db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015AprTest', password = '')
conn2 = db2.cursor(buffered = True)

#Checks if bill exists, if not, adds the bill
def addBill(cursor, bid, type, number, state, status, house, session):
	select_stmt = "SELECT bid from Bill where bid = %(bid)s AND number = %(number)s"
	cursor.execute(select_stmt, {'bid':bid,'number':number})
	if(cursor.rowcount == 0):
		print "adding Bill {0}".format(bid)
		cursor.execute(query_insert_Bill, (bid, type, number, state, status, house, session))

#Checks if billVersion exists, if not, adds the billVersion
def addBillVersion(cursor, vid, bid, date, state, subject, appropriation, substantive_changes):
	select_stmt = "SELECT bid from BillVersion where vid = %(vid)s"
	cursor.execute(select_stmt, {'vid':vid})
	if(cursor.rowcount == 0):
		print "adding BillVersion {0}".format(vid)
		cursor.execute(query_insert_Bill_Version, (vid, bid, date, state, subject, appropriation, substantive_changes))

#Finds the state of the bill
#Used as a helper for finding BillVersions
def findState(cursor, bid):
	select_stmt = "SELECT state from Bill where bid = %(bid)s"
	cursor.execute(select_stmt, {'bid':bid})
	temp = [0]
	if cursor.rowcount > 0:
		temp = cursor.fetchone()
	return temp[0]

#Gets all of the Bills, then adds them as necessary
def getBills():
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

#Gets all of the BillVersions then adds them as necessary
def getBillVersions():
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

def main():
	print "getting Bills"
	getBills()
	print "getting Bill Versions"
	getBillVersions()
	print "Closing Database COnnections"
	db.close()
	db2.close()

if __name__ == "__main__":
	main()
	
