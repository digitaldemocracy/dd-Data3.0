# Cal Access Extractor
# Daniel Mangin
# 11/13/2014

import mysql.connector
import re
import sys
import csv
import datetime

#Querys used to Insert into the Database
query_insert_lobbying_firm = "INSERT INTO LobbyingFirm (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"
query_insert_lobbyist = "INSERT INTO Lobbyist (pid, filer_id) VALUES(%s, %s);"
query_insert_person = "INSERT INTO Person (last, first) VALUES (%s, %s);"
query_insert_lobbyist_employer = "INSERT INTO LobbyistEmployer (filer_naml, filer_id, coalition) VALUES(%s, %s, %s);"
query_insert_lobbyist_employment = "INSERT INTO LobbyistEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"
query_insert_lobbyist_direct_employment = "INSERT INTO LobbyistDirectEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"
query_insert_lobbyist_contracts = "INSERT INTO LobbyingContracts (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"

#Currently a static table that is sized to 10,000
Lobbyist = [[0 for x in xrange(5)] for x in xrange(10000)]

#Changes the date into a linux format for the database
def format_date(str):
	temp = ''
	str = str.split('/');
	for x in range(0,3):
		if len(str[x]) == 1:
			str[x] = "0" + str[x]
	temp = '-'.join([str[2], str[0], str[1]])
	return temp
	
#Finds the pid of the Person (returns val if none is found)
def getPerson(cursor, filer_naml, filer_namf, val):
	pid = val
	select_pid = "SELECT pid FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	else:
		cursor.execute(query_insert_person, (filer_naml, filer_namf))
		select_pid = "SELECT pid FROM Person ORDER BY Person.pid DESC limit 1;"
		cursor.execute(select_pid);
		pid = cursor.fetchone()[0];
	return pid

#inserts into lobbyist employer	
def insert_lobbyist_employer(cursor, filer_naml, filer_id, coalition):
	select_stmt = "SELECT filer_id FROM LobbyistEmployer WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_employer, (filer_naml, filer_id, coalition))	

#inserts into lobbying firm
def insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr):	
	select_stmt = "SELECT filer_id FROM LobbyingFirm WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbying_firm, (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr))
		
#inserts into lobbyist
def insert_lobbyist(cursor, pid, filer_id):
	select_stmt = "SELECT pid FROM Lobbyist WHERE pid = %(pid)s"
	cursor.execute(select_stmt, {'pid':pid})
	if(cursor.rowcount > 0):
		return
	select_stmt = "SELECT filer_id FROM Lobbyist WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist, (pid, filer_id))

def insert_lobbyist_employment(cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT sender_id, rpt_date, ls_beg_yr FROM LobbyistEmployment WHERE sender_id = %(sender_id)s"
	cursor.execute(select_stmt, {'sender_id':sender_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_employment, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr))
		
def insert_lobbyist_direct_employment(cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT sender_id, rpt_date, ls_beg_yr FROM LobbyistDirectEmployment WHERE sender_id = %(sender_id)s"
	cursor.execute(select_stmt, {'sender_id':sender_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_direct_employment, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr))
		
def insert_lobbyist_contracts(cursor, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT filer_id, sender_id, rpt_date FROM LobbyingContracts WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_contracts, (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr))
	
#For case 4
#Goes through the lobbyist list and determines if they work for
#Lobbyist Employment or LobbyistDirectEmployment	
def find_lobbyist_employment(cursor, index):
	select_stmt = "SELECT filer_id FROM LobbyingFirm WHERE filer_id = %(sender_id)s"
	cursor.execute(select_stmt, {'sender_id':Lobbyist[index][1]})
	if(cursor.rowcount > 0):
		select_stmt = "SELECT * FROM LobbyistEmployment WHERE pid = %(pid)s AND sender_id = %(sender_id)s AND rpt_date = %(rpt_date)s AND ls_end_yr = %(ls_end_yr)s"
		cursor.execute(select_stmt, {'pid':Lobbyist[index][0], 'sender_id':Lobbyist[index][1], 'rpt_date':Lobbyist[index][2], 'ls_end_yr':Lobbyist[index][4]})
		if(cursor.rowcount == 0):
			cursor.execute(query_insert_lobbyist_employment, (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2], Lobbyist[index][3], Lobbyist[index][4]))
	select_stmt = "SELECT filer_id FROM LobbyistEmployer WHERE filer_id = %(sender_id)s"
	cursor.execute(select_stmt, {'sender_id':Lobbyist[index][1]})
	if(cursor.rowcount > 0):
		select_stmt = "SELECT * FROM LobbyistDirectEmployment WHERE pid = %(pid)s AND sender_id = %(sender_id)s AND rpt_date = %(rpt_date)s AND ls_end_yr = %(ls_end_yr)s"
		cursor.execute(select_stmt, {'pid':Lobbyist[index][0], 'sender_id':Lobbyist[index][1], 'rpt_date':Lobbyist[index][2], 'ls_end_yr':Lobbyist[index][4]})
		if(cursor.rowcount == 0):
			cursor.execute(query_insert_lobbyist_direct_employment, (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2], Lobbyist[index][3], Lobbyist[index][4]))
				

db = mysql.connector.connect(user = 'root', db = 'DDDB2015AprTest', password = '')
dd = db.cursor(buffered = True)

try:
	refOff = "SET foreign_key_checks = 0"
	dd.execute(refOff)
	with open('CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')
		
		val = 0
		index = 0

		for row in tsvin:
			form = row[3]
			sender_id = row[4]
			entity_cd = row[6]
			val = val + 1
			print val
			#case 1
			if form == "F601" and entity_cd == "FRM" and sender_id[:1] == 'F': 
				filer_naml = row[7]
				filer_id = row[5]
				rpt_date = row[12]
				rpt_date = rpt_date.split(' ')[0]
				rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				print "naml = {0}, id = {1}, date = {2}, beg = {3}, end = {4}\n".format(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbying_firm(dd, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
			#case 2
			elif form == "F604" and entity_cd == "LBY" and sender_id[:1] == 'F':
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				rpt_date = rpt_date.split(' ')[0]
				rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				pid = getPerson(dd, filer_naml, filer_namf, val)
				print "filer_id = {0}\n".format(filer_id)
				print "sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n".format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbyist(dd, pid, filer_id)
				insert_lobbyist_employment(dd, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
			#case 3
			elif form == "F604" and entity_cd == "LBY" and sender_id[:1] == 'E':
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				rpt_date = rpt_date.split(' ')[0]
				rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				pid = getPerson(dd, filer_naml, filer_namf, val)
				print "filer_id = {0}\n".format(filer_id)
				print "sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n".format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbyist(dd, pid, filer_id)
				insert_lobbyist_direct_employment(dd, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
			#case 4
			elif form == "F604" and entity_cd == "LBY" and sender_id.isdigit():
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				rpt_date = rpt_date.split(' ')[0]
				rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				firm_name = row[61]
				print "filer_id = {0}\n".format(filer_id)
				pid = getPerson(dd, filer_naml, filer_namf, val)
				insert_lobbyist(dd, pid, filer_id)
				print "inserting Lobbyist into index {0}\n".format(index)
				#insert the lobbyist into the array for later
				Lobbyist[index][0] = pid
				Lobbyist[index][1] = sender_id
				Lobbyist[index][2] = rpt_date
				Lobbyist[index][3] = ls_beg_yr
				Lobbyist[index][4] = ls_end_yr
				index += 1
			#case 5
			elif form == "F602" and entity_cd == "LEM":
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				rpt_date = rpt_date.split(' ')[0]
				rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				coalition = (filer_id[:1] == 'C') * 1
				print "filer_naml = {0}, filer_id = {1}, coalition = {2}\n".format(filer_naml, filer_id, coalition)
				insert_lobbyist_employer(dd, filer_naml, filer_id, coalition)
				insert_lobbyist_contracts(dd, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
			#case 6
			elif form == "F603" and entity_cd == "LEM":
				ind_cb = row[39]
				bus_cb = row[40]
				trade_cb = row[41]
				oth_cb = row[42]
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				rpt_date = row[12]
				rpt_date = rpt_date.split(' ')[0]
				rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				coalition = (filer_id[:1] == 'C') * 1
				print "filer_naml = {0}, filer_id = {1}, coalition = {2}\n".format(filer_naml, filer_id, coalition)
				if(ind_cb == 'X'):
					insert_lobbyist_employer(dd, filer_naml + filer_namf, filer_id, coalition)
				else:
					insert_lobbyist_employer(dd, filer_naml, filer_id,  coalition)
			#case 7
			elif form == "F606":
				print 'case 7'
			#case 8
			elif form == "F607" and entity_cd == "LEM":
				print 'case 8'
			#just to catch those that dont fit at all
			else:
				print 'Does not match any case!'
				
		#Goes through the Lobbyist table and finds employment
		#Continuation of case 4
		while index:
			index -= 1
			print "checking lobbyist {0}\n".format(index)
			find_lobbyist_employment(dd, index)
			
		db.commit()
	refOn = "Set foreign_key_checks = 1"
	dd.execute(refOn)		
		
#If there is an issue, will print it and rollback the Database
except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
#close all connections
db.close()
			
		
