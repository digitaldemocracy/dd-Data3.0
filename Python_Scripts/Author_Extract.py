import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_author = "INSERT INTO authors (pid, bid, vid, contribution) VALUES (%s, %s, %s, %s);" 
query_insert_committee_author = "INSERT INTO CommitteeAuthors (cid, bid, vid) VALUES (%s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn2 = db2.cursor(buffered = True)

def addCommitteeAuthor(cursor, cid, bid, vid):
	select_stmt = "SELECT * FROM CommitteeAuthors where cid = %(cid)s AND bid = %(bid)s AND vid = %(vid)s;"
	cursor.execute(select_stmt, {'cid':cid, 'bid':bid, 'vid':vid})
	if cursor.rowcount == 0:
		cursor.execute(query_insert_committee_author, (cid, bid, vid))

def cleanNameCommittee(name):
	if "Committee on" in name:
		temp = ""
		name = name.split(' ')
		temp = name[2]
		for a in range(3, len(name)):
			temp = temp + ' ' + name[a]
		name = temp
	return name

def findCommittee(cursor, name, house):
	cid = -1
	print name
	name = cleanNameCommittee(name)
	select_stmt = "SELECT * FROM Committee where name = %(name)s;"
	cursor.execute(select_stmt, {'name':name})
	if cursor.rowcount == 1:
		print 'Found Committee'
		cid = cursor.fetchone()[0]
	elif cursor.rowcount > 1:
		if(house == 'ASSEMBLY'):
			house = "Assembly"
		else:
			house = "Senate"
		select_stmt = "SELECT * FROM Committee where name = %(name)s AND house = %(house)s;"
		cursor.execute(select_stmt, {'name':name, 'house':house})
		if cursor.rowcount == 1:
			print 'Found Committee'
			cid = cursor.fetchone()[0]
		else:
			print "Cannot determine committee!"
	else:
		print "Cannot find committee {0}".format(name)
	return cid

def cleanName(name):
	#for de Leon
	temp = name.split('\xc3\xb3')
	if(len(temp) > 1):
		name = temp[0] + "o" + temp[1];
	if(name == "Allen Travis"):
		name = "Travis Allen"
	if 'Donnell' in name:
		name = "O'Donnell"
	return name

def getPerson(cursor, filer_naml, floor):
	pid = -1
	filer_naml = cleanName(filer_naml)
	temp = filer_naml.split(' ')
	if(floor == 'ASSEMBLY'):
		floor = "Assembly"
	else:
		floor = "Senate"
	filer_namf = ''
	if(len(temp) > 1):
		filer_naml = temp[len(temp)-1]
		filer_namf = temp[0]
		select_pid = "SELECT pid, last, first FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	else:
		select_pid = "SELECT pid, last, first FROM Person WHERE last = %(filer_naml)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount == 1:
		pid = cursor.fetchone()[0]
	elif cursor.rowcount > 1:
		a = []
		for j in range(0, cursor.rowcount):
			temp = cursor.fetchone()
			a.append(temp[0])
		for j in range(0, cursor.rowcount):
			select_term = "SELECT pid, house FROM Term WHERE pid = %(pid)s AND house = %(house)s ORDER BY Term.pid;"
			cursor.execute(select_term, {'pid':a[j],'house':floor})
			if(cursor.rowcount == 1):
				pid = cursor.fetchone()[0]
	else:
		filer_naml = '%' + filer_naml + '%'
		select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(filer_naml)s AND pid < 130 ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
		if(cursor.rowcount > 0):
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
		#print "already have {0}".format(pid);
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
		#print i
		temp = conn.fetchone()
		if temp:
			cid = -1
			pid = getPerson(conn2, temp[3], temp[2])
			if(pid == -1):
				cid = findCommittee(conn2, temp[3], temp[2])
			pid = checkLegislator(conn2, pid)
			vid = temp[0]
			bid = findBill(conn2, vid)
			contribution = "none"
			if temp[4] == "LEAD_AUTHOR":
				contribution = "Lead Author"
			#print contribution
			if pid != -1 and vid is not 'none' and contribution is not 'none' and bid is not 'none':
				if temp[9] == 'Y':
					#print "adding author {0}".format(pid)
					j = j + addAuthor(conn2, pid, bid, vid, contribution)
			elif cid != -1:
				if temp[9] == 'Y':
					addCommitteeAuthor(conn2, cid, bid, vid)
				
	print j
	db2.commit()
	
except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()
db2.close()	