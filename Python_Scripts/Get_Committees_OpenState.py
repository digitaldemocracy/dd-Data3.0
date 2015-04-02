import json
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_committee = "INSERT INTO Committee (cid, house, name) VALUES (%s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'DDDB2015', password = '')
dd = db.cursor(buffered = True)

def insert_Committee(cursor, cid, house, name):
	select_stmt = "SELECT * from Committee where cid = %(cid)s;"
	cursor.execute(select_stmt, {'cid':cid})
	if cursor.rowcount == 0:
		print 'inserting committee {0} called {1}'.format(cid, name)
		cursor.execute(query_insert_committee, (cid, house, name))

try:
	f = open('committees.txt', 'r')
	cid = 0
	for l in f:
		line = l.split(' ')
		house = line[0]
		name = ' '.join(line[1:]).rstrip('\n')
		print name
		insert_Committee(dd, cid, house, name)
		cid += 1
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()
		