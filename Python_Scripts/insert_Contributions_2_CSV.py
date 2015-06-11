import json
import urllib2
import re
import sys
import csv
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_contribution = "INSERT INTO Contribution (id, pid, year, date, house, donorName, donorOrg, amount) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);"

def checkLegislator(cursor, pid):
	select_pid = "SELECT pid FROM Legislator WHERE pid = %(pid)s ORDER BY Legislator.pid;"
	cursor.execute(select_pid, {'pid':pid})
	if cursor.rowcount > 0:
		return pid
	else:
		return -1

def getPerson(cursor, first, last, floor):
	pid = -1
	#print "{0} {1}".format(first, last)
	first = '%' + first + '%'
	select_pid = "SELECT pid, last, first FROM Person WHERE last = %(last)s AND first LIKE %(first)s ORDER BY Person.pid;"
	#print select_pid
	cursor.execute(select_pid, {'last':last,'first':first})
	#print cursor.rowcount
	if cursor.rowcount == 1:
		pid = cursor.fetchone()[0]
	elif cursor.rowcount > 1:
		#print "found more"
		a = []
		for j in range(0, cursor.rowcount):
			temp = cursor.fetchone()
			a.append(temp[0])
		for j in range(0, len(a)):
			select_term = "SELECT pid, house FROM Term WHERE pid = %(pid)s ORDER BY Term.pid;"
			cursor.execute(select_term, {'pid':a[j],'house':floor})
			if(cursor.rowcount == 1):
				pid = cursor.fetchone()[0]
			else:
				print "Too many duplicates"
	else:
		last = '%' + last + '%'
		select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(last)s AND first LIKE %(first)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'last':last,'first':first})
		if(cursor.rowcount > 0):
			pid = cursor.fetchone()[0]
		else:
			select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(last)s ORDER BY Person.pid;"
			cursor.execute(select_pid, {'last':last})
			if(cursor.rowcount == 1):
				pid = cursor.fetchone()[0]
			else:
				#print "could not find {0} {1}".format(first, last)
				pass
	return pid

def insert_Contributor(cursor, id, pid, year, date, house, donorName, donorOrg, amount):
	cursor.execute(query_insert_contribution, (id, pid, year, date, house, donorName, donorOrg, amount))

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn = db.cursor(buffered = True)

with open('cand_2001.csv', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter=',')
		
		val = 0
		index = 0

		for row in tsvin:
			print index
			try:
				year = row[1]
				id = row[4]
				date = row[5]
				amount = row[6]
				house = row[13]
				name = row[9]
				first = name.split(', ')[1]
				first = first.title()
				temp = first.split(' ')
				temp2 = []
				for i in range(0, len(temp)):
					if not '.' in temp[i]:
						temp2.append(temp[i])
				first = ' '.join(temp2)
				last = name.split(',')[0]
				last = last.title()
				temp = last.split(' ')
				temp2 = []
				for i in range(0, len(temp)):
					if not '.' in temp[i]:
						temp2.append(temp[i])
				last = ' '.join(temp2)
				if "Assembly" in house:
					house = "Assembly"
				elif "Senate" in house:
					house = "Senate"
				else:
					house = "null"
				district = row[14]
				donorName = row[15]
				donorOrg = row[21]
				pid = getPerson(conn, first, last, house)
				pid = checkLegislator(conn, pid)
				if house != "null" and pid != -1:
					insert_Contributor(conn, id, pid, year, date, house, donorName, donorOrg, amount)
					index = index + 1
					db.commit()
				else:
					val = val + 1
					#print 'did not go in successfully'
			except:
				print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		print val
		print index
db.close()





