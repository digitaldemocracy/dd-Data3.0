'''
File: import_authors_ny.py
Author: Min Eric Roh
Date: 12/26/2015
Description:
- Imports NY authors using senate API
- Fills authors
- Needs Bills table to be filled first
- Currently configured to test DB
'''
import requests
import MySQLdb

def call_senate_api(restCall, year, house, offset):
	if house != "":
		house = "/" + house
	url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "?full=true&limit=1000&key=IhV5AXQ1rhUS8ePXkfwsO4AvjQSodd4Q&offset=" + str(offset)
	r = requests.get(url)
	print url
	out = r.json()
	print type(out), type(out["result"]), type(out["result"]["items"])
	return (out["result"]["items"], out['total'])

def get_author_api(year):
	total = 1000
	cur_offset = 1
	ret_bills = list()
	x = 0

	while cur_offset < total:
		call = call_senate_api("bills", 2015, "", cur_offset)
		bills = call[0]
		total = call[1]
		for bill in bills:
			if bill['sponsor']['member'] is not None:
				b = dict()
				b['type'] = bill['basePrintNo']
				b['session'] = '0'
				name = bill['sponsor']['member']['fullName']
				sname = name.split(' ')
				split_index = len(sname) - 1
				
				if x == 0:
					print "sname", sname, "index", split_index
				b['first'] = ' '.join(sname[:split_index]).strip()
				b['last'] = ' '.join(sname[split_index:]).strip()
				if x == 0:
					print "first?", sname[:split_index]
					print "first",b['first'], "last", b['last']
					x += 1
				b['versions'] = bill['amendments']['items']
				b['bid'] = "NY_" + str(year) + str(year + 1) + b['session'] + b['type']
				ret_bills.append(b)
		cur_offset += 1000
	print len(ret_bills)
	return ret_bills

def insert_authors_db(bill, dddb):
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
		print bill['bid']
		pid = get_pid_db(bill['first'], bill['last'], dddb)
		if pid is not None and check_bid_db(bill['bid'], dddb):
			a['pid'] = pid
			a['bid'] = bill['bid']
			a['vid'] = bill['bid'] + key
			a['contribution'] = 'Lead Author'

			dddb.execute(select_stmt, a)
			if dddb.rowcount == 0:
				dddb.execute(insert_stmt, a)
			else:
				print a['bid'], "already existing"
		else:
			print "fill Person and Bill table first"

def check_bid_db(bid, dddb):
	select_stmt = '''	SELECT * FROM Bill
						WHERE bid = %(bid)s
						'''
	dddb.execute(select_stmt, {'bid':bid})
	if dddb.rowcount == 1:
		return True
	else:
		return False

def get_pid_db(first, last, dddb):
	select_person = '''	SELECT * FROM Person
                     	WHERE first = %(first)s
                      	 AND last = %(last)s 
                  		'''
	dddb.execute(select_person, {'first':first,'last':last})
#	print (select_person %  {'first':first,'last':last})
	if dddb.rowcount == 1:
		ret = dddb.fetchone()[0]
		print ret
		return ret
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
						passwd='digitaldemocracy789')
	dddb = dddb_conn.cursor()
	dddb_conn.autocommit(True)
	add_authors_db(2015, dddb)
main()
