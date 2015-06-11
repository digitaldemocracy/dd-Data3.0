import json
import urllib2
import re
import sys
import csv
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_update_Legislator = "UPDATE Legislator SET OfficialBio = %s WHERE pid = %s"

def update_Bio(cursor, pid, text):
	cursor.execute(query_update_Legislator, (text, pid))

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
	filer_naml = filer_naml.split(',')[0]
	temp = filer_naml.split(' ')
	for i in range(0, len(temp) - 1):
		print i
		if "." in temp[i]:
			del temp[i]
	print temp
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
		select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(filer_naml)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
		if(cursor.rowcount > 0):
			pid = cursor.fetchone()[0]
		else:
			print "could not find {0}".format(filer_naml)
	return pid

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn = db.cursor(buffered = True)

with open('Senate_and_Assembly_Biographies.csv', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter=',')
		
		val = 0
		index = 0

		for row in tsvin:
			try:
				house = ""
				if(row[0] == "Senator"):
					house = "Senate"
				else:
					house = "Assembly"
				name = row[1]
				print name
				pid = getPerson(conn, name, house)
				text = row[2]
				print pid
				update_Bio(conn, pid, text)	
			except:
				db.rollback()
				print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		db.commit()
		print val

db.close()





