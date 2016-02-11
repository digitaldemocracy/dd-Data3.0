#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: import_authors_ny.py
Author: Min Eric Roh
Date: 12/26/2015
Description:
- Imports NY authors using senate API
- Fills authors
- Needs Bill, BillVersion, Person tables to be filled first
- Currently configured to test DB
'''
import requests
import MySQLdb
import loggingdb

#counter = 0

US_STATE = 'NY'

# URL
URL = ('http://legislation.nysenate.gov/api/3/%(restCall)s/%(year)s%(house)s' +
		'?full=true&limit=1000&key=IhV5AXQ1rhUS8ePXkfwsO4AvjQSodd4Q&offset=%(offset)s')

# INSERTS
QI_AUTHORS = '''	INSERT INTO authors
						(pid, bid, vid, contribution)
						VALUES
						(%(pid)s, %(bid)s, %(vid)s, %(contribution)s)'''

# SELECTS
QS_AUTHORS_CHECK = ''' 	SELECT pid
						FROM authors
						WHERE bid = %(bid)s
						 AND vid = %(vid)s
						 AND contribution = %(contribution)s'''
QS_PERSON = '''	SELECT pid
	            FROM Person
	            WHERE last = %s
	             AND first = %s
	            ORDER BY Person.pid'''
QS_BILL = '''	SELECT * FROM Bill
				WHERE bid = %s'''
QS_BILLVERSION = '''	SELECT * FROM BillVersion
						WHERE vid = %s'''

# UPDATE
QU_AUTHORS = '''	UPDATE authors
					SET pid = %(pid)s
					WHERE bid = %(bid)s
					 AND vid = %(vid)s
					 AND contribution = %(contribution)s'''

def call_senate_api(restCall, year, house, offset):
	if house != "":
		house = "/" + house
	url = URL % {'restCall':restCall, 'year':str(year), 'house':house, 'offset':str(offset)}
	r = requests.get(url)
	print url
	out = r.json()
	return (out["result"]["items"], out['total'])

def get_author_api(year):
	total = 1000
	cur_offset = 1
	ret_bills = list()

	while cur_offset < total:
		call = call_senate_api("bills", year, "", cur_offset)
		bills = call[0]
		total = call[1]
		for bill in bills:
			if bill['sponsor']['member'] is not None:
				b = dict()
				b['type'] = bill['basePrintNo']
#				print b['type']
				b['session'] = '0'
				fullName = bill['sponsor']['member']['fullName'].encode('utf-8')
				name = clean_name(fullName)
				b['last'] = name[1]
				b['first'] = name[0]
				b['versions'] = bill['amendments']['items']
				b['bid'] = "NY_" + str(year) + str(year + 1) + b['session'] + b['type']
				ret_bills.append(b)
		cur_offset += 1000
	print len(ret_bills)
	return ret_bills

def insert_authors_db(bill, dddb):
	global counter
	
	for key in bill['versions'].keys():
		a = dict()
		pid = get_pid_db(bill['first'], bill['last'], dddb)
		if pid is not None and check_bid_db(bill['bid'], dddb):
			a['pid'] = pid
			a['bid'] = bill['bid']
			a['vid'] = bill['bid'] + key
			a['contribution'] = 'Lead Author'
#			print a['vid']
			dddb.execute(QS_AUTHORS_CHECK, a)
			if dddb.rowcount == 0 and check_vid_db(a['vid'], dddb):
				dddb.execute(QI_AUTHORS, a)
				counter += 1
			elif dddb.fetchone()[0] != a['pid']:
				dddb.execute(QU_AUTHORS, a)
#			else:
#				print a['bid'], "already existing"
#		else:
#			print "fill Person, Bill table first"

def check_vid_db(vid, dddb):
	dddb.execute(QS_BILLVERSION, (vid,))
	if dddb.rowcount == 1:
		return True
	else:
		print vid, 'no vid'
		return False 

def check_bid_db(bid, dddb):
	dddb.execute(QS_BILL, (bid,))
	if dddb.rowcount == 1:
		return True
	else:
		print bid, 'no bid'
		return False

def clean_name(name):
    problem_names = {
        "Inez Barron":("Charles", "Barron"), 
        "Philip Ramos":("Phil", "Ramos"), 
        "Thomas McKevitt":("Tom", "McKevitt"), 
        "Albert Stirpe":("Al","Stirpe"), 
        "Peter Abbate":("Peter","Abbate, Jr."),
#        "Sam Roberts":("Pamela","Hunter"),
        "Herman Farrell":("Herman", "Farrell, Jr."),
        "Fred Thiele":("Fred", "Thiele, Jr."),
#       "William Scarborough":("Alicia", "Hyndman"),
        "Robert Oaks":("Bob", "Oaks"),
        "Andrew Goodell":("Andy", "Goodell"),
        "Peter Rivera":("José", "Rivera"),
        "Addie Jenne Russell":("Addie","Russell"),
        "Kenneth Blankenbush":("Ken","Blankenbush"),
#        "Alec Brook-Krasny":("Pamela","Harris"),
        "Mickey Kearns":("Michael", "Kearns"),
        "Steven Englebright":("Steve", "Englebright"),
        "HUNTER":("Pamela","Hunter"),
    }
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()      
    suffix = "";
    if len(name_arr) == 1 and name_arr[0] in problem_names.keys():
    	print name_arr
    	name_arr = list(problem_names[name_arr[0]])
    	print name_arr
    for word in name_arr:
#    	print "word", word
        if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
            name_arr.remove(word)
            if word in ending.keys():
                suffix = ending[word]            
#    print name_arr        
    first = name_arr.pop(0)
#    print "first", first
    while len(name_arr) > 1:
        first = first + ' ' + name_arr.pop(0)            
    last = name_arr[0]
#    print "last", last
    last = last.replace(' ' ,'') + suffix
    
    if (first + ' ' + last) in problem_names.keys():             
        return problem_names[(first + ' ' + last)]
#    print "return"
    return (first, last)

def get_pid_db(first, last, dddb):
	dddb.execute(QS_PERSON, (last, first))
	if dddb.rowcount >= 1:
		ret = dddb.fetchone()[0]
		return ret
	else:
		print first, last, 'not in database'
		return None

def add_authors_db(year, dddb):
	bills = get_author_api(year)

	for bill in bills:
		insert_authors_db(bill, dddb)

def main():
	with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
						user='awsDB',
						db='DDDB2015Dec',
						port=3306,
						passwd='digitaldemocracy789',
						charset='utf8') as dddb:
#		dddb = dddb_conn.cursor()
#		dddb_conn.autocommit(True)
		add_authors_db(2015, dddb)
#	print counter
main()