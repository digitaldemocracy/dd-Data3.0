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

db = mysql.connector.connect(user = 'root', db = 'DDDB2015AprTest', password = '')
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
	if(cursor.rowcount == 0):
		#print 'insert'
		#print pid
		#print year
		#print district
		#print house
		#print cid
		print 'inserting {0}'.format(pid)
		cursor.execute(query_insert_serveson, (pid, year, district, house, cid))
	else:
		#print 'servesOn pid = {0}, house = {1}, cid = {2}, district = {3} exists'.format(pid, house, cid, district)
		pass

def getPerson(cursor, filer_naml, filer_namf):
	pid = -1
	#print filer_naml
	#print filer_namf
	filer_naml = '%' + filer_naml + '%'
	filer_namf = '%' + filer_namf + '%'
	select_pid = "SELECT pid FROM Person WHERE last LIKE %(filer_naml)s AND first LIKE %(filer_namf)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	else:
		print "couldn't find {0} {1}".format(filer_namf, filer_naml)
	return pid
	
def create_servesOn(cursor, name, house, cid):
	year = 2015
	name = name.split(' ')
	first = ''.join(name[0].split(' '))
	last = ''.join(name[len(name)-1].split(' '))
	pid = -1;
	if len(first) > 0 and len(last) > 0:
		pid = getPerson(cursor, last, first)
	else:
		print 'Missing first or last name';
	if pid != -1:
		district = find_district(cursor, pid, year, house)
		if district != 999:
			insert_serveson(dd, pid, year, district, house, cid)
		else:
			print 'District not found'
			pass
	else:
		#print 'Person not Found'
		pass

def clean_name(name):
	name = name.split(',')[0]
	if "acute;" in name:
		print 'getting rid of acute character'
		name = ''.join(''.join(name.split('&')).split("acute;"))
	if "&#39;" in name:
		name = "'".join(name.split('&#39;'))
	if "&nsbp;" in name:
		name = name.split('&nsbp;')[0]
	if "&nbsp;" in name:
		name = name.split('&nbsp;')[0]
	if "nbsp;" in name:
		name = name.split('nbsp;')[0]
	if "&rsquo;" in name:
		name = name.split('&rsquo;')[0]
	if "." in name:
		name = ' '.join(name.split('.'))
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
	#print imp
	if(imp.count('/') == 1):
		link = 'http://assembly.ca.gov' + link;
	#print imp
        if len(link.split('/')) == 3:
                link = link + "/membersstaff"
        page = urllib2.urlopen(link)
        html = page.read()
        matches = re.findall('<td>\n.+<.+</td>',html)
	i = 0
        for match in matches:
		i = i + 1
                name = match.split('>')[2].split('<')[0].split('(')[0]
		name = clean_name(name)
		create_servesOn(dd, name, house, cid)
	return i

def get_members_senate(imp, cid, house, joint):
	try:
		#print imp
        	link = imp.split('"')[1]
        	page = urllib2.urlopen(link)
        	html = page.read()
        	matches = re.findall('<a href=.+>Senator.+',html)
        	''.join(matches)
		i = 0
        	for match in matches:
			i = i + 1
                	#print match
                	parts = match.split('>')
                	for part in parts:
                        	if "Senator" in part:
                                	name = part.split(">")[0].split("(")[0].split("<")[0]
                                	name = ' '.join(name.split(' ')[1:])
					name = clean_name(name)
					create_servesOn(dd, name, house, cid)
		return i
	except:
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		return 0

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
        	i = i + get_members_assembly(imp[0], cid, house)
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
		joint = ""
		print "Committee: {0}".format(imp[1])
		if "Joint" in imp[1]:
			house = "Joint"
			joint = "Yes"
		cid = find_Committee(dd, house, imp[1])
		house = "Senate"
        	print imp[1]
        	i = i + get_members_senate(imp[0], cid, house, joint)
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()

print "There are {0} entries in servesOn".format(i)

db.close()
