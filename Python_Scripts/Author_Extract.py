import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_author = "INSERT INTO authors (pid, bid, vid, contribution) VALUES (%s, %s, %s, %s);" 

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn2 = db2.cursor(buffered = True)

def getPerson(cursor, filer_naml):
	pid = -1
	temp = filer_naml.split(' ')
	if(len(temp) > 1):
		filer_naml = temp[1]
	filer_naml = "%" + filer_naml + "%";
	select_pid = "SELECT pid FROM Person WHERE last LIKE %(filer_naml)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	else:
		print "could not find {0}".format(filer_naml)
	return pid

def checkLegislator(cursor, pid):
	select_pid = "SELECT pid FROM Legislator WHERE pid = %(pid)s ORDER BY Legislator.pid;"
	cursor.execute(select_pid, {'pid':pid})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	else:
		pid = -1
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
	cursor.execute(select_stmt, {'bid':bid, 'pid':pid, 'vid':vid})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_author, (pid, bid, vid, contribution))
		return 1
	else:
		print "already have {0}".format(pid);
		return 0

try:
	select_count = "SELECT COUNT(*) FROM bill_version_authors_tbl"
	conn.execute(select_count)
	temp = conn.fetchone()
	a = temp[0]
	select_stmt = "Select * from bill_version_authors_tbl"
	conn.execute(select_stmt);
	j = 0;
	for i in range(0, a):
		print i
		temp = conn.fetchone()
		if temp:
			pid = getPerson(conn2, temp[3])
			pid = checkLegislator(conn2, pid)
			vid = temp[0]
			bid = findBill(conn2, vid)
			contribution = "none"
			if temp[4] == "LEAD_AUTHOR":
				contribution = "Lead Author"
			print contribution
			if pid != -1 and vid is not 'none' and contribution is not 'none' and bid is not 'none':
				print "adding author"
				j = j + addAuthor(conn2, pid, bid, vid, contribution)
				
	print j
	db2.commit()
	
except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()
db2.close()	