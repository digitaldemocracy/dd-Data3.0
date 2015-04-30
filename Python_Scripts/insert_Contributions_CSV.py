import json
import urllib2
import re
import sys
import csv
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_contribution = "INSERT INTO Contribution (pid, law_eid, d_id, year, house, contributor, amount) VALUES (%s, %s, %s, %s, %s, %s, %s);"

def findHouse(cursor, pid):
	select_stmt = "SELECT house from Term where pid = %(pid)s;"
	cursor.execute(select_stmt, {'pid':pid})
	if cursor.rowcount > 0:
		return cursor.fetchone()[0]
	else:
		return "null"

def insert_Contributor(cursor, pid, law_eid, d_id, year, house, contributor, amount):
	cursor.execute(query_insert_contribution, (pid, law_eid, d_id, year, house, contributor, amount))

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn = db.cursor(buffered = True)

with open('leg_contribution.csv', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter=',')
		
		val = 0
		index = 0

		for row in tsvin:
			print index
			try:
				pid = row[0]
				amount = row[5]
				house = findHouse(conn, pid)
				year = row[6]
				contributor = row[3]
				law_eid = row[1]
				d_id = row[2]
				if house != "null":
					insert_Contributor(conn, pid, law_eid, d_id, year, house, contributor, amount)
					index = index + 1
				else:
					val = val + 1
			except:
				db.rollback()
				print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		db.commit()
		print val
db.close()





