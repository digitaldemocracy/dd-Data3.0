'''
File: Author_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Inserts the authors from capublic.bill_version_authors_tbl into the DDDB2015Apr.authors or DDDB2015Apr.committeeAuthors
- This script runs under the update script
- Fills table:
	authors (pid, bid, vid, contribution)
	CommitteeAuthors (cid, bid, vid)

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
	- bill_version_author_tbl
'''

import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

#Insertion queries that are used
query_insert_author = "INSERT INTO authors (pid, bid, vid, contribution) VALUES (%s, %s, %s, %s);" 
query_insert_committee_author = "INSERT INTO CommitteeAuthors (cid, bid, vid) VALUES (%s, %s, %s);"

#database connections
db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn2 = db2.cursor(buffered = True)

#Adds a CommitteeAuthor
def addCommitteeAuthor(cursor, cid, bid, vid):
	select_stmt = "SELECT * FROM CommitteeAuthors where cid = %(cid)s AND bid = %(bid)s AND vid = %(vid)s;"
	cursor.execute(select_stmt, {'cid':cid, 'bid':bid, 'vid':vid})
	if cursor.rowcount == 0:
		cursor.execute(query_insert_committee_author, (cid, bid, vid))

#Cleans committee names
def cleanNameCommittee(name):
	#Removes the "Committee On" string inside the capublic name
	if "Committee on" in name:
		temp = ""
		name = name.split(' ')
		temp = name[2]
		for a in range(3, len(name)):
			temp = temp + ' ' + name[a]
		name = temp
	return name

#Attempt to find the Committee
def findCommittee(cursor, name, house):
	#Using -1 as a null value
	cid = -1
	#fix the house so it matches the DDDB2015Apr enum
	house = house.title()
	#Clean the name for checking with DDDB2015Apr.Committee
	name = cleanNameCommittee(name)
	#Check if there is a Committee with the same name
	select_stmt = "SELECT * FROM Committee where name = %(name)s;"
	cursor.execute(select_stmt, {'name':name})
	#If you find one Committee
	if cursor.rowcount == 1:
		cid = cursor.fetchone()[0]
	#If there is more than one committee, examine the house and find the right one
	elif cursor.rowcount > 1:
		#Try to find the Committee
		select_stmt = "SELECT * FROM Committee where name = %(name)s AND house = %(house)s;"
		cursor.execute(select_stmt, {'name':name, 'house':house})
		#if one is found, get the cid
		if cursor.rowcount == 1:
			cid = cursor.fetchone()[0]
		#Tell that you cannot find the committee
		else:
			pass
	#Tell you cannot find the committee
	else:
		pass
	#return whatever we found
	return cid

#Clean the name of the person and remove/replace wierd characters
def cleanName(name):
	#for de Leon
	temp = name.split('\xc3\xb3')
	if(len(temp) > 1):
		name = temp[0] + "o" + temp[1];
	#fixes the allen travis switch up
	if(name == "Allen Travis"):
		name = "Travis Allen"
	#for O'Donnell
	if 'Donnell' in name:
		name = "O'Donnell"
	return name

#Finds the Person using a combined name
def getPerson(cursor, filer_naml, floor):
	pid = -1
	filer_naml = cleanName(filer_naml)
	temp = filer_naml.split(' ')
	filer_namf = ""
	floor = floor.title()
	if(len(temp) > 1):
		filer_naml = temp[len(temp)-1]
		filer_namf = temp[0]
		select_pid = "SELECT Person.pid, last, first FROM Person, Legislator WHERE Legislator.pid = Person.pid AND last = %(filer_naml)s AND first = %(filer_namf)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	else:
		select_pid = "SELECT Person.pid, last, first FROM Person, Legislator WHERE Legislator.pid = Person.pid AND last = %(filer_naml)s ORDER BY Person.pid;"
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
		select_pid = "SELECT Person.pid, last, first FROM Person, Legislator WHERE Legislator.pid = Person.pid AND last LIKE %(filer_naml)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
		if(cursor.rowcount == 1):
			pid = cursor.fetchone()[0]
		else:
			pass
	return pid

#Finds the billVersion for the author
def findBill(cursor, vid):
	bid2 = "none"
	select_pid = "SELECT bid FROM BillVersion WHERE vid = %(vid)s;"
	cursor.execute(select_pid, {'vid':vid})
	if cursor.rowcount > 0:
		bid2 = cursor.fetchone()[0]
	return bid2

#Adds the author is they are not present in the database
def addAuthor(cursor, pid, bid, vid, contribution):
	select_stmt = "SELECT bid, pid, vid from authors where bid = %(bid)s AND pid = %(pid)s AND vid = %(vid)s"
	cursor.execute(select_stmt, {'bid':bid, 'pid':pid, 'vid':vid})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_author, (pid, bid, vid, contribution))
		return 1
	else:
		#print "already have {0}".format(pid);
		return 0

#All of the author logic
def getAuthors():
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
				elif pid == -1 and cid == -1:
					print "Could not find {0}".format(temp[3])
					
		print j
		db2.commit()
	
	except:
		db2.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		exit()

def main():
	getAuthors()
	db.close()
	db2.close()

if __name__ == "__main__":
	main()	