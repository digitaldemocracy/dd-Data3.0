# -*- coding: utf-8 -*- 
'''
File: Get_Committees_Web.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Scrapes the Assembly and Senate Websites to gather current Committees and Membership and place them into
	DDDB2015Apr.Committee and DDDB2015Apr.servesOn
- Used for daily update Script
- Fills table:
	Committee (cid, house, name)
	servesOn (pid, year, district, house, cid)

Sources
- California Assembly Website
- California Senate Website

'''

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

#Database Queries used
query_insert_committee = "INSERT INTO Committee (cid, house, name) VALUES (%s, %s, %s);"
query_insert_serveson = "INSERT INTO servesOn (pid, year, district, house, cid) VALUES(%s, %s, %s, %s, %s);"

#Database Connections
db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
dd = db.cursor(buffered = True)

db2 = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
dd2 = db2.cursor(buffered = True)

#Inserts the floor members after we find their terms
def insertFloorMembers(cursor, cid, house):
	select_stmt = "SELECT * FROM Term WHERE house = %(house)s;"
	cursor.execute(select_stmt, {'house':house})
	print cursor.rowcount
	for i in range(0, cursor.rowcount):
		print 'inserting another'
		temp = cursor.fetchone()
		pid = temp[0]
		year = 2015
		district = temp[2]
		print 'servesOn pid = {0}, house = {1}, cid = {2}, district = {3}'.format(pid, house, cid, district)
		insert_serveson(dd, pid, year, district, house, cid)

#inserts the COmmittee Senate Floor
def insertSenateFloor(cursor):
	#insert the senate floor
	name = "Senate Floor"
	house = "Senate"
	select_stmt = "SELECT * from Committee where house = %(house)s AND name = %(name)s;"
	cursor.execute(select_stmt, {'house':house,'name':name})
	print 'rowcount'
	print cursor.rowcount
	if cursor.rowcount == 0:
		select_stmt = "SELECT cid from Committee ORDER BY cid DESC LIMIT 1"
		cursor.execute(select_stmt)
		cid = cursor.fetchone()[0]
		cid = cid + 1
		insert_Committee(cursor, cid, house, name)
	else:
		temp = cursor.fetchone()
		cid = temp[0]
	insertFloorMembers(dd2, cid, house) 

#inserts the Committee Assembly Floor
def insertAssemblyFloor(cursor):
	#insert the assembly floor
	name = "Assembly Floor"
	house = "Assembly"
	select_stmt = "SELECT * from Committee where house = %(house)s AND name = %(name)s;"
	cursor.execute(select_stmt, {'house':house,'name':name})
	if cursor.rowcount == 0:
		select_stmt = "SELECT count(*) from Committee"
		cursor.execute(select_stmt)
		cid = cursor.fetchone()[0]
		insert_Committee(cursor, cid, house, name)
	else:
		temp = cursor.fetchone()
		cid = temp[0]
	insertFloorMembers(dd2, cid, house) 
		
# Finds the district that a legislator serves using Term
def find_district(cursor, pid, year, house):
	select_stmt = "SELECT district FROM Term where pid = %(pid)s AND house = %(house)s AND year = %(year)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year})
	if(cursor.rowcount > 0):
		temp = cursor.fetchone()
		return temp[0]
	return 999

#checks if the legislator is already in database, otherwise input them in servesOn
def insert_serveson(cursor, pid, year, district, house, cid):
	select_stmt = "SELECT * FROM servesOn where pid = %(pid)s AND house = %(house)s AND year = %(year)s AND cid = %(cid)s AND district = %(district)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year, 'cid':cid, 'district':district})
	if(cursor.rowcount == 0):
		print 'inserting {0}'.format(pid)
		cursor.execute(query_insert_serveson, (pid, year, district, house, cid))
	else:
		#print 'servesOn pid = {0}, house = {1}, cid = {2}, district = {3} exists'.format(pid, house, cid, district)
		pass

#Finds the person
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

#Creates all the data needed for the servesOn insertion
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
	#name = name.split(',')[0]
	if "acute;" in name:
		print 'getting rid of acute character'
		name = ''.join(''.join(name.split('&')).split("acute;"))
	if "&#39;" in name:
		name = "'".join(name.split('&#39;'))
	if "&#039;" in name:
		name = "'".join(name.split('&#039;'))
#	if "&nsbp;" in name:
#		name = name.split('&nsbp;')[0]
	if "&nbsp;" in name:
		name = name.split('&nbsp;')[0]
	if "nbsp;" in name:
		name = name.split('nbsp;')[0]
	if "&rsquo;" in name:
		name = name.split('&rsquo;')[0]
	if "–" in name:
		name = '-'.join(name.split('–'))
#	if "." in name:
#		name = ' '.join(name.split('.'))
	#return name.lstrip().rstrip()
        return name.strip()

def find_Committee(cursor, house, name):
	name = clean_name(name)
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

def getAssemblyInformation():
	response = urllib2.urlopen('http://assembly.ca.gov/committees')
	html = response.read()
	matches = re.findall('<span class="field-content">.+',html)
	try:
		for match in matches:
			#print match
			parts = match.split('<')
			imp = parts[2].split('>')
			house = "Assembly"
			if "Joint" in imp[1]:
				house = "Joint"
			print "Committee: {0}".format(imp[1])
			#print house
			cid = find_Committee(dd, house, imp[1])
			house = "Assembly"
			get_members_assembly(imp[0], cid, house)
		insertAssemblyFloor(dd)
		db.commit()

	except:
		db.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		exit()

def getSenateInformation():
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
			get_members_senate(imp[0], cid, house, joint)
		insertSenateFloor(dd)
		db.commit()

	except:
		db.rollback()
		print 'error!', sys.exc_info()[0], sys.exc_info()[1]
		exit()

def main():
	getAssemblyInformation()
	getSenateInformation()
	db.close()
	db2.close()

if __name__ == "__main__":
   main()

