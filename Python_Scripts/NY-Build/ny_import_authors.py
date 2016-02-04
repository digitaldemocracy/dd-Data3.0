# -*- coding: utf8 -*-
'''
File: ny_import_authors.py
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

counter = 0;

def call_senate_api(restCall, year, house, offset):
	if house != "":
		house = "/" + house
	url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "?full=true&limit=1000&key=IhV5AXQ1rhUS8ePXkfwsO4AvjQSodd4Q&offset=" + str(offset)
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
			if bill['sponsor']['member'] is not None and len(bill['sponsor']['member']['fullName'].split()) > 1:
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
	insert_stmt = 	'''	INSERT INTO authors
						(pid, bid, vid, contribution)
						VALUES
						(%(pid)s, %(bid)s, %(vid)s, %(contribution)s)
						'''
	select_stmt = ''' 	SELECT *
						FROM authors
						WHERE pid = %(pid)s
						 AND bid = %(bid)s
						 AND vid = %(vid)s
						 AND contribution = %(contribution)s
						'''
	for key in bill['versions'].keys():
		a = dict()
		pid = get_pid_db(bill['first'], bill['last'], dddb)
		if pid is not None and check_bid_db(bill['bid'], dddb):
			a['pid'] = pid
			a['bid'] = bill['bid']
			a['vid'] = bill['bid'] + key
			a['contribution'] = 'Lead Author'
#			print a['vid']
			dddb.execute(select_stmt, a)
			if dddb.rowcount == 0 and check_vid_db(a['vid'], dddb):
				dddb.execute(insert_stmt, a)
				counter += 1
#			else:
#				print a['bid'], "already existing"
#		else:
#			print "fill Person, Bill table first"

def check_vid_db(vid, dddb):
	select_stmt = '''	SELECT * FROM BillVersion
						WHERE vid = %(vid)s
						'''
	dddb.execute(select_stmt, {'vid':vid})
	if dddb.rowcount == 1:
		return True
	else:
		print vid, 'no vid'
		return False 

def check_bid_db(bid, dddb):
	select_stmt = '''	SELECT * FROM Bill
						WHERE bid = %(bid)s
						'''
	dddb.execute(select_stmt, {'bid':bid})
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
        "Sam Roberts":("Pamela","Hunter"),
        "Herman Farrell":("Herman", "Farrell, Jr."),
        "Fred Thiele":("Fred", "Thiele, Jr."),
        "William Scarborough":("Alicia", "Hyndman"),
        "Robert Oaks":("Bob", "Oaks"),
        "Andrew Goodell":("Andy", "Goodell"),
        "Peter Rivera":("José", "Rivera"),
        "Addie Jenne Russell":("Addie","Russell"),
        "Kenneth Blankenbush":("Ken","Blankenbush"),
        "Alec Brook-Krasny":("Pamela","Harris"),
        "Mickey Kearns":("Michael", "Kearns"),
        "Steven Englebright":("Steve", "Englebright"),
        
    }
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()      
    suffix = "";               
    for word in name_arr:
#    	print "word", word
        if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
            name_arr.remove(word)
            if word in ending.keys():
                suffix = ending[word]            
            
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
	select_person = '''	SELECT * FROM Person
                     	WHERE first = %(first)s
                      	 AND last = %(last)s 
                  		'''
	dddb.execute(select_person, {'first':first,'last':last})
#	print (select_person %  {'first':first,'last':last})
	if dddb.rowcount == 1:
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
	dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
						user='awsDB',
						db='JohnTest',
						port=3306,
						passwd='digitaldemocracy789',
						charset='utf8')
	dddb = dddb_conn.cursor()
	dddb_conn.autocommit(True)
	add_authors_db(2015, dddb)
	print counter
main()
