'''
File: Vote_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gets the Vote Data from capublic.bill_summary_vote into DDDB2015Apr.BillVoteSummary and capublic.bill_detail_vote into DDDB2015Apr.BillVoteDetail
- Used in daily update of DDDB2015Apr
- Fills Tables:
	BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result)
	BillVoteDetail (pid, voteId, result)

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
	- bill_summary_vote_tbl
	- bill_detail_vote_tbl

'''


import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_BillVoteSummary = "INSERT INTO BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
query_insert_BillVoteDetail = "INSERT INTO BillVoteDetail (pid, voteId, result) VALUES (%s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn2 = db2.cursor(buffered = True)

db3 = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn3 = db.cursor(buffered = True)

def findCommittee(cursor, name, house):
	select_stmt = "SELECT cid FROM Committee WHERE name = %(name)s AND house = %(house)s;"
	cursor.execute(select_stmt, {'name':name, 'house':house})
	if(cursor.rowcount == 1):
		return cursor.fetchone()[0]
	elif(cursor.rowcount > 1):
		return -1
	else:
		return -1

def getCommittee(cursor, location_code):
	select_stmt = "SELECT description, long_description FROM location_code_tbl WHERE location_code = %(location_code)s;"
	cursor.execute(select_stmt, {'location_code':location_code})
	if(cursor.rowcount > 0):
		print "found committee"
		temp = cursor.fetchone()
		name = temp[0]
		nam = temp[1]
		cid = 0
		print name
		print nam
		if "Water, Parks" in nam:
			nam = "Water, Parks, and Wildlife"
		if "Asm" in name or 'Assembly' in name:
			print "ASM"
			house = "Assembly"
			cid = findCommittee(conn2, nam, house) 
		elif "Sen" in name:
			house = "Senate"
			cid = findCommittee(conn2, nam, house) 
		else:
			house = "Joint"
			cid = findCommittee(conn2, nam, house)
	return cid 
		

def cleanName(name):
	#for de Leon
	temp = name.split('\xc3\xb3')
	if(len(temp) > 1):
		name = temp[0] + "o" + temp[1];
	if(name == "Allen Travis"):
		name = "Travis Allen"
	return name

def getPerson(cursor, filer_naml, floor):
	pid = -1
	filer_naml = cleanName(filer_naml)
	temp = filer_naml.split(' ')
	if(floor == 'AFLOOR'):
		floor = "Assembly"
	else:
		floor = "Senate"
	filer_namf = ''
	if(len(temp) > 1):
		filer_naml = temp[len(temp)-1]
		filer_namf = temp[0]
		select_pid = "SELECT pid, last, first FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf)s AND pid < 130 ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	else:
		select_pid = "SELECT pid, last, first FROM Person WHERE last = %(filer_naml)s AND pid < 130 ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount == 1:
		pid = cursor.fetchone()[0]
	elif cursor.rowcount > 1:
		a = []
		for j in range(0, cursor.rowcount):
			temp = cursor.fetchone()
			a.append(temp[0])
		for j in range(0, cursor.rowcount):
			select_term = "SELECT pid, house FROM Term WHERE pid = %(pid)s AND house = %(house)s;"
			cursor.execute(select_term, {'pid':a[j],'house':floor})
			if(cursor.rowcount == 1):
				pid = cursor.fetchone()[0]
	else:
		filer_naml = '%' + filer_naml + '%'
		select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(filer_naml)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
		if(cursor.rowcount > 0):
			pid = cursor.fetchone()[0]
		else:
			print "could not find {0}".format(filer_naml)
	return pid

def getVoteId(cursor, bid, mid, VoteDate):
	select_pid = "SELECT voteId FROM BillVoteSummary WHERE bid = %(bid)s AND mid = %(mid)s;"
	cursor.execute(select_pid, {'bid':bid, 'mid':mid})
	if cursor.rowcount == 1:
		temp = cursor.fetchone()
		return temp[0]
	else:
		#print "No such BillVoteSummary found"
		return -1;

def insert_BillVoteSummary(cursor, bid, mid, cid, VoteDate, ayes, naes, abstain, result):
	select_pid = "SELECT bid, mid, VoteDate FROM BillVoteSummary WHERE bid = %(bid)s AND mid = %(mid)s AND VoteDate = %(VoteDate)s;"
	cursor.execute(select_pid, {'bid':bid, 'mid':mid, 'VoteDate':VoteDate})
	if cursor.rowcount == 0:
		print "inserting..."
		print cid
		cursor.execute(query_insert_BillVoteSummary, (bid, mid, cid, VoteDate, ayes, naes, abstain, result))
	else:
		#print "already in"
		pass

def insert_BillVoteDetail(cursor, pid, voteId, result, temp):
	select_pid = "SELECT pid, voteId FROM BillVoteDetail WHERE pid = %(pid)s AND voteId = %(voteId)s;"
	cursor.execute(select_pid, {'pid':pid, 'voteId':voteId})
	if cursor.rowcount == 0:
		cursor.execute(query_insert_BillVoteDetail, (pid, voteId, result))
	else:
		#print "pid = {0}, voteId = {1}, result = {2} already in".format(pid, voteId, result)
		#print temp
		pass

def getSummaryVotes():
	try:
		select_count = "SELECT COUNT(*) FROM bill_summary_vote_tbl"
		conn.execute(select_count)
		temp = conn.fetchone()
		a = temp[0]
		print a
		select_stmt = "Select * from bill_summary_vote_tbl"
		conn.execute(select_stmt);
		for i in range(0, a):
			temp = conn.fetchone()
			if temp:
				bid = temp[0]
				mid = temp[4]
				cid = getCommittee(conn3, temp[1])
				print cid
				VoteDate = temp[10]
				ayes = temp[5]
				naes = temp[6]
				abstain = temp[7]
				result = temp[8]
				if(cid != -1):
					insert_BillVoteSummary(conn2, bid, mid, cid, VoteDate, ayes, naes, abstain, result)
		db2.commit()


	except:
		db2.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		exit()

def getDetailVotes():
	try:
		select_count = "SELECT COUNT(*) FROM bill_detail_vote_tbl"
		conn.execute(select_count)
		temp = conn.fetchone()
		a = temp[0]
		select_stmt = "Select * from bill_detail_vote_tbl"
		conn.execute(select_stmt);
		for i in range(0, a):
			temp = conn.fetchone()
			if temp:
				date = temp[8].strftime('%Y-%m-%d')
				pid = getPerson(conn2, temp[2], temp[1])
				voteId = getVoteId(conn2, temp[0], temp[6], date)
				result = temp[5]
				if(voteId != -1 and pid != -1):
					insert_BillVoteDetail(conn2, pid, voteId, result, temp)
				else:
					#print temp
					pass
		db2.commit()


	except:
		db2.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		exit()

def main():
	getSummaryVotes()
	getDetailVotes()
	db.close()
	db2.close()

if __name__ == "__main__":
	main()