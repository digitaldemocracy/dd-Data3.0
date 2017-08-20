#Grabs the Committees and their memberships from the SOS Assembly and Senate sites
#Fills the tables Committee and servesOn
#Relies on data from Person and Term

import json
import re
import sys
import urllib2
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_committee = "INSERT INTO Committee (cid, house, name) VALUES (%s, %s, %s);"
query_insert_serveson = "INSERT INTO servesOn (pid, year, district, house, cid) VALUES(%s, %s, %s, %s, %s);"

db = mysql.connector.connect(user = 'root', db = 'DDDB2015Test', password = '')
dd = db.cursor(buffered = True)

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
	print 'inserting {0}'.format(pid)
	if(cursor.rowcount == 0):
		#print 'insert'
		#print pid
		#print year
		#print district
		#print house
		#print cid
		cursor.execute(query_insert_serveson, (pid, year, district, house, cid))
	else:
		print 'servesOn exists'

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
	
def create_servesOn(cursor, name, house, cid):
	year = 2015
	name = name.split(' ')
	print len(name)
	first = name[0]
	last = name[len(name)-1]
	pid = getPerson(cursor, last, first)
	if pid != -1:
		district = find_district(cursor, pid, year, house)
		if district != 999:
			insert_serveson(dd, pid, year, district, house, cid)
		else:
			print 'District not found'
	else:
		print 'Person not Found'

def clean_name(name):
	name = name.split(',')[0]
	if "acute;" in name:
		print 'getting rid of acute character'
		name = ''.join(''.join(name.split('&')).split("acute;"))
	if "&#39;" in name:
		name = "'".join(name.split('&#39;'))
	if "&nsbp;" in name:
		name = name.split('&nsbp;')[0]
	return name.lstrip().rstrip()

def find_Committee(cursor, house, name):
	select_stmt = "SELECT * from Committee where house = %(house)s AND name = %(name)s;"
	cursor.execute(select_stmt, {'house':house,'name':name})
	if cursor.rowcount == 0:
		select_stmt = "SELECT count(*) from Committee"
		cursor.execute(select_stmt)
		cid = cursor.fetchone()[0]
		insert_Committee(cursor, cid, house, name)
		return cid
	else:
		return cursor.fetchone()[0]

def insert_Committee(cursor, cid, house, name):
	select_stmt = "SELECT * from Committee where cid = %(cid)s;"
	cursor.execute(select_stmt, {'cid':cid})
	if cursor.rowcount == 0:
		print 'inserting committee {0} called {1}'.format(cid, name)
		cursor.execute(query_insert_committee, (cid, house, name))

def get_members_assembly(imp, cid, house):
        link = imp.split('"')[1]
        if len(link.split('/')) == 3:
                link = link + "/membersstaff"
        page = urllib2.urlopen(link)
        html = page.read()
        matches = re.findall('<td>\n.+<.+</td>',html)
        for match in matches:
                name = match.split('>')[2].split('<')[0].split('(')[0]
                print name
		name = clean_name(name)
		print name
		create_servesOn(dd, name, house, cid)

def get_members_senate(imp, cid, house):
        link = imp.split('"')[1]
        page = urllib2.urlopen(link)
        html = page.read()
        matches = re.findall('<a href=.+>Senator.+',html)
        ''.join(matches)
        for match in matches:
                #print match
                parts = match.split('>')
                for part in parts:
                        if "Senator" in part:
                                name = part.split(">")[0].split("(")[0].split("<")[0]
                                name = ' '.join(name.split(' ')[1:])
				print name
				name = clean_name(name)
				print name
				create_servesOn(dd, name, house, cid)

response = urllib2.urlopen('http://assembly.ca.gov/committees')
html = response.read()
matches = re.findall('<span class="field-content">.+',html)
i = 0
try:
	for match in matches:
        #print match
        	parts = match.split('<')
        	imp = parts[2].split('>')
		house = "Assembly"
		if "Joint" in imp[1]:
			house = "Joint"
		print "Committee: {0}".format(imp[1])
		cid = find_Committee(dd, house, imp[1])
		house = "Assembly"
        	i = i + 1
        	get_members_assembly(imp[0], cid, house)
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

response = urllib2.urlopen('http://senate.ca.gov/committees')
html = response.read()
#print html
matches = re.findall('<div class="views-field views-field-title">.+\n.+',html)
f = open('committees.txt','a')
try:
	for match in matches:
        	match = match.split('\n')[1]
        	#print match
        	parts = match.split('<')
        	#print parts[1]
        	imp = parts[1].split('>')
		house = "Senate"
		print "Committee: {0}".format(imp[1])
		if "Joint" in imp[1]:
			house = "Joint"
		cid = find_Committee(dd, house, imp[1])
		house = "Senate"
        	print imp[1]
        	i = i + 1
        	get_members_senate(imp[0], cid, house)
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

db.close()
