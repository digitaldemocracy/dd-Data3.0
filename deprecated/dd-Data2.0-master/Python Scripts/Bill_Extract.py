import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_Bill = "INSERT INTO Bill (bid, type, number, state, status, house, session) VALUES (%s, %s, %s, %s, %s, %s, %s);"
query_insert_Bill_Version = "INSERT INTO BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes) VALUES (%s, %s, %s, %s, %s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Test', password = '')
conn2 = db2.cursor(buffered = True)

def addBill(cursor, bid, type, number, state, status, house, session):
	print 'adding Bill'
	select_stmt = "SELECT bid from Bill where bid = %(bid)s"
	cursor.execute(select_stmt, {'bid':bid})
	print 'bill'
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_Bill, (bid, type, number, state, status, house, session))
	print 'done'

def addBillVersion(cursor, vid, bid, date, state, subject, appropriation, substantive_changes):
	print 'adding Versions'
	select_stmt = "SELECT bid from BillVersion where vid = %(vid)s"
	cursor.execute(select_stmt, {'vid':vid})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_Bill_Version, (vid, bid, date, state, subject, appropriation, substantive_changes))

def findState(cursor, bid):
	select_stmt = "SELECT state from Bill where bid = %(bid)s"
	cursor.execute(select_stmt, {'bid':bid})
	temp = cursor.fetchone()
	return temp[0]

try:
	print 'here'
	select_count = "SELECT COUNT(*) FROM bill_tbl"
	conn.execute(select_count)
	print 'what'
	temp = conn.fetchone()
	a = temp[0]
	print a
	select_stmt = "SELECT * FROM bill_tbl"
	conn.execute(select_stmt)
	for i in range(0, a):
		temp = conn.fetchone()
		print temp[0]
		print i
		if i:
			bid = temp[0]
			number = temp[4]
			status = temp[17]
			session = temp[2]
			type = temp[3]
			house = temp[16]
			state = temp[5]
			addBill(conn2, bid, type, number, state, status, house, session)
			print 'got Bill'
	db2.commit()
	
except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

try:
	select_count = "SELECT COUNT(*) FROM bill_version_tbl"
	conn.execute(select_count)
	temp = conn.fetchone()
	a = temp[0]
	print 'getting Versions'
	select_stmt = "SELECT * FROM bill_version_tbl"
	conn.execute(select_stmt)
	for i in range(0, a):
		temp = conn.fetchone()
		if temp:
			vid = temp[0]
			bid = temp[1]
			date = temp[3]
			#state = findState(conn2, bid)
			subject = temp[6]
			appropriation = temp[8]
			substantive_changes = temp[11]
			print 'add Version'
			#addBillVersion(conn2, vid, bid, date, state, subject, appropriation, substantive_changes)
	db2.commit()
except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

	
db.close()
db2.close()	