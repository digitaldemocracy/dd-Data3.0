import json
import urllib2
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_contribution = "INSERT INTO Contribution (pid, year, house, contributor, amount) VALUES (%s, %s, %s, %s, %s);"

def insert_Contributor(cursor, pid, year, house, contributor, amount):
	select_stmt = "SELECT * from Contribution WHERE pid = %(pid)s AND year = %(year)s AND contributor = %(contributor)s;"
	cursor.execute(select_stmt, {'pid':pid, 'year':year, 'contributor':contributor})
	if cursor.rowcount == 0:
		cursor.execute(query_insert_contribution, (pid, year, house, contributor, amount))

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
conn = db.cursor(buffered = True)

with open('contributions.json') as data_file:    
	data = json.load(data_file)

for i in range(1, 120):
	try:
		for j in range(0, len(data[str(i)])):
			print "going in"
			pid = data[str(i)][j]["pid"]
			amount = data[str(i)][j]["amount"]
			house = data[str(i)][j]["house"].title()
			year = data[str(i)][j]["year"]
			contributor = data[str(i)][j]["contributor"]
			print "getting insert"
			insert_Contributor(conn, pid, year, house, contributor, amount)
		db.commit()
	except:
		db.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
db.close()





