import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_BillVoteSummary = "INSERT INTO BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"
query_insert_BillVoteDetail = "INSERT INTO BillVoteDetail (pid, voteId, result) VALUES (%s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
conn = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015AprTest', password = '')
conn2 = db2.cursor(buffered = True)

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
		select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(filer_naml)s AND pid < 130 ORDER BY Person.pid;"
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
		print "No such BillVoteSummary found"
		return -1;

def insert_BillVoteSummary(cursor, bid, mid, cid, VoteDate, ayes, naes, abstain, result):
	select_pid = "SELECT bid, mid, VoteDate FROM BillVoteSummary WHERE bid = %(bid)s AND mid = %(mid)s AND VoteDate = %(VoteDate)s;"
	cursor.execute(select_pid, {'bid':bid, 'mid':mid, 'VoteDate':VoteDate})
	if cursor.rowcount == 0:
		#print "inserting..."
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
		print "pid = {0}, voteId = {1}, result = {2} already in".format(pid, voteId, result)
		print temp

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
			cid = 26
			VoteDate = temp[10]
			ayes = temp[5]
			naes = temp[6]
			abstain = temp[7]
			result = temp[8]
			insert_BillVoteSummary(conn2, bid, mid, cid, VoteDate, ayes, naes, abstain, result)

	db2.commit()

except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

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
				print temp
	db2.commit()

except:
	db2.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

db.close()
db2.close()