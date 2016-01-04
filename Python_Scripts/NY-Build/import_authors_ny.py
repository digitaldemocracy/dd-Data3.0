'''
File: import_authors_ny.py
Author: Min Eric Roh
Date: 12/26/2015
Description:
- Imports NY authors using senate API
- Fills authors
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
			b = dict()
			b['number'] = bills['basePrintNo'][1:]
			b['type'] = bills['basePrintNo'][0:1]
			b['session'] = '0'
			name = b['sponsor']['member']['fullName']
			sname = first.split(' ')
			split_index = len(sname)
			b['first'] = ' '.join(sname[:split_index]).strip()
			b['last'] = ' '.join(sname[split_index:]).strip()
			b['versions'] = bill['amendments']['items']
			b['bid'] = "NY_" + str(year) + str(year + 1) + b['session'] + b['type'] + b['number']
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
	for key in bill['versions'].keys():
		a = dict()
		a['pid'] = get_pid_db(bill['first'], bill['last'], dddb)
		a['bid'] = bill['bid']
		a['vid'] = bill['bid'] + key
		a['contributions'] = 'Lead Author'

		dddb.execute(insert_stmt, a)

def get_pid_db(first, last, dddb):
	select_person = '''	SELECT * FROM Person
                     	WHERE first = %(first)s
                      	AND last = %(last)s 
                  		'''
	dddb.execute(select_person, {'first':first,'last':last})
	#print (select_person %  {'first':first,'last':last})
	query = dddb.fetchone();
	return query[0]

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
