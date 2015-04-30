import json
import urllib2
import re
import sys
import csv
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_update_Committee = "UPDATE Committee SET Type = %s WHERE cid = %s"

def update_Committee(cursor, type, cid):
	print cid
	print type
	cursor.execute(query_update_Committee, (type, cid))

def find_Committee(cursor, house, name):
	select_stmt = "SELECT * from Committee where house = %(house)s AND name = %(name)s;"
	cursor.execute(select_stmt, {'house':house,'name':name})
	if cursor.rowcount > 0:
		return cursor.fetchone()[0]
	else:
		return -1

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn = db.cursor(buffered = True)

with open('Committee_list.csv', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter=',')
		
		val = 0
		index = 0

		for row in tsvin:
			try:
				type = row[0]
				print row[0]
				if type != "Joint Committee":
					house = "Assembly"
				else:
					house = "Joint"
				type = type.split("Committee")[0]
				print type
				print row[1]
				cid = find_Committee(conn, house, row[1])
				print cid
				update_Committee(conn, type, cid)
				if(len(row) > 2):
					type = row[0]
					type = type.split("Committee")[0]
					if type != "Joint Committee":
						house = "Senate"
					else:
						house = "Joint"
					cid = find_Committee(conn, house, row[3])
					print cid
					update_Committee(conn, type, cid)	
			except:
				db.rollback()
				print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		db.commit()
		print val
db.close()





