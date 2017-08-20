import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_author = "INSERT INTO authors (pid, bid, vid, contribution) VALUES (%s, %s, %s, %s);" 

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB', password = '')
conn2 = db2.cursor(buffered = True)

def getPerson(cursor, filer_naml):
	pid = -1
	select_pid = "SELECT pid FROM Person WHERE last = %(filer_naml)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	return pid

def findBill(cursor, vid):
	bid2 = "none"
	select_pid = "SELECT bid FROM BillVersion WHERE vid = %(vid)s;"
	cursor.execute(select_pid, {'vid':vid})
	if cursor.rowcount > 0:
		bid2 = cursor.fetchone()[0]
	return bid2

def addAuthor(cursor, pid, bid, vid, contribution):
	select_stmt = "SELECT bid, pid, vid from authors where bid = %(bid)s AND pid = %(pid)s AND vid = %(vid)s"
	cursor.execute(select_stmt, {'bid':bid, 'vid':vid, 'pid':pid})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_author, (pid, bid, vid, contribution))

try:
	select_count = "SELECT COUNT(*) FROM bill_version_authors_tbl"
	conn.execute(select_count)
	temp = conn.fetchone()
	a = temp[0]
	select_stmt = "Select * from bill_version_authors_tbl"
	conn.execute(select_stmt);
	for i in range(0, a):
		print i
		temp = conn.fetchone()
		if temp:
			pid = getPerson(conn2, temp[3])
			print 'pid '
			print pid
			vid = temp[0]
			print 'vid'
			print vid
			bid = findBill(conn2, vid)
			print 'bid'
			print bid
			print 'contribution'
			print temp[4]
			contribution = "none"
			if temp[4] == "LEAD_AUTHOR":
				contribution = "Lead Author"
			print contribution
			if pid != -1 and vid is not 'none' and contribution is not 'none':
				print "adding author"
				addAuthor(conn2, pid, bid, vid, contribution)
			else:
				print 'ignored'
	db2.commit()
	
except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()
db2.close()	