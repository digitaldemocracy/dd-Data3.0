'''
DEPRECIATED SCRIPT. We use Get_Committees_Web.py instead
File: Committee_CSV_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers JSON data from OpenState and fills DDDB2015Apr.servesOn
-Used for daily update DDDB2015Apr
- Fills table:
	servesOn (pid, year, district, house, cid)

Sources
- OpenState

'''

import json
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_serveson = "INSERT INTO servesOn (pid, year, district, house, cid) VALUES(%s, %s, %s, %s, %s);"

#gets all of the committees in CA
url = urlopen('http://openstates.org/api/v1/committees/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce&state=ca').read()
result = json.loads(url)

for m in range(len(result)):
	print result[m]['committee']

def find_subcommittee(temp, house):
	for i in range(0, len(result)):
		if(temp == result[i]['subcommittee']):
			return result[i]['id']
	return "invalid"

def find_committee(temp, house):
	if temp.split()[0] == "Subcommittee":
		return find_subcommittee(temp, house)
	for i in range(0, len(result)):
		str = '';
		if result[i]['subcommittee'] is None:
			if "Standing" == result[i]['committee'].split()[0]:
				str = ' '.join(result[i]['committee'].split()[3:])
			else:
				str = result[i]['committee']
		#print 'from db: {0}, from json: {1}, are equal: {2}'.format(temp, ' '.join(result[i]['committee'].split()[3:]), temp == ' '.join(result[i]['committee'].split()[3:]))
		if str == temp:
			if (house == 'Senate' and result[i]['chamber'] == 'upper') or (house == 'Assembly' and result[i]['chamber'] == 'lower'):                        	
				print i
				temp = result[i]['id']
				print temp
				return temp
	return "invalid"

def getPerson(cursor, filer_naml, filer_namf):
	pid = -1
	print filer_naml
	print filer_namf
	filer_naml = '%' + filer_naml + '%'
	filer_namf = '%' + filer_namf + '%'
	select_pid = "SELECT pid FROM Person WHERE last LIKE %(filer_naml)s AND first LIKE %(filer_namf)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	return pid
	
def find_district(cursor, pid, year, house):
	select_stmt = "SELECT district FROM Term where pid = %(pid)s AND house = %(house)s AND year = %(year)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year})
	if(cursor.rowcount > 0):
		temp = cursor.fetchone()
		return temp[0]
	return 999
	
def insert_serveson(cursor, pid, year, district, house, cid):
	select_stmt = "SELECT * FROM servesOn where pid = %(pid)s AND house = %(house)s AND year = %(year)s AND cid = %(cid)s AND district = %(district)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year, 'cid':cid, 'district':district})
	if(cursor.rowcount == 0):
		#print 'insert'
		#print pid
		#print year
		#print district
		#print house
		#print cid
		cursor.execute(query_insert_serveson, (pid, year, district, house, cid))

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
dd = db.cursor(buffered = True)
de = db.cursor(buffered = True)

try:
	select_stmt = ("SELECT * FROM Committee")
	de.execute(select_stmt)
	a = de.rowcount
	print a
	for x in xrange(0,a):
		temp = de.fetchone()
		print x
		print "committee is {0}".format(temp[2])
		if temp:
                        print temp[2]
			id = find_committee(temp[2], temp[1])
			cid = temp[0]
			house = temp[1]
			print house
			print id
			print "Committee {0}".format(id)
			if id is not "invalid":
				print 'valid'
				str = 'http://openstates.org/api/v1/committees/' + id + '/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce'
				url2 = urlopen(str).read()
				#print url2
				#print str
				committee = json.loads(url2)
				print 'size of list:'
				print len(committee['members'])
				for m in range(0, len(committee['members'])):
					print m
					name = committee['members'][m]['name'].split(' ')
					index = 0
					if house == "Senate":
						index = 1
					last = ''
					first = ''
					if((len(name) > 0 and house == "Assembly") or (house == "Senate" and len(name) > 1)):
						last = name[len(name)-1]
						first = name[index]
					print 'finding person'
					pid = getPerson(dd, last, first)
					print pid
					if pid != -1:
						year = 2015
						district = find_district(dd, pid, year, house)
						print "pid = {0}, year = {1}, district = {2}, house = {3}, cid = {4}\n".format(pid, year, district, house, cid)
						if(district != 999):
							print 'inserting servesOn'
                                                        insert_serveson(dd, pid, year, district, house, cid)
	
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()

