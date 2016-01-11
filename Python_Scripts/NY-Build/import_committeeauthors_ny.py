'''
File: import_committeeauthors_ny.py
Author: Min Eric Roh
Date: 12/26/2015
Description:
- Imports NY CommitteeAuthors using senate API
- Fills authors
- Needs Committee table to be filled first
- Currently configured to test DB
'''
import requests
import MySQLdb

def call_senate_api(restCall, year, house, offset):
	if house != "":
		house = "/" + house
	url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "/search?term=sponsor.rules:true&full=true&limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=" + str(offset)
	r = requests.get(url)
	print url
	out = r.json()
	return out["result"]["items"]

def get_committeeauthors_api(year):
	bills = call_senate_api("bills", 2015, "", 1)
	ret_bills = list()

	for bill in bills:
		b = dict()
		b['type'] = bill['result']['basePrintNo']
		b['session'] = '0'
		b['versions'] = bill['result']['amendments']['items']
		b['bid'] = "NY_" + str(year) + str(year + 1) + b['session'] + b['type']
		ret_bills.append(b)
	print len(ret_bills)
	return ret_bills

def insert_committeeauthors_db(bill, cid, year, dddb):
	insert_stmt = 	'''	INSERT INTO CommitteeAuthors
						(cid, bid, vid, state)
						VALUES
						(%s, %s, %s, 'NY')
						'''
	select_stmt = 	'''	SELECT *
						FROM CommitteeAuthors
						WHERE cid = %s
						 AND bid = %s
						 AND vid = %s
						 AND state = 'NY'
						'''
	for key in bill['versions'].keys():
		if check_bid_db(bill['bid'], dddbe):
			a = dict()
			a['bid'] = bill['bid']
			a['vid'] = bill['bid'] + key

			dddb.execute(select_stmt, (str(cid), a['bid'], a['vid']))
			if dddb.rowcount == 0:
				dddb.execute(insert_stmt, (str(cid), a['bid'], a['vid']))
			else:
				print "already existing"
		else:
			print bill['bid'], "fill Bill table first"

def check_bid_db(bid, dddb):
	select_stmt = '''	SELECT * FROM Bill
						WHERE bid = %(bid)s
						'''
	dddb.execute(select_stmt, {'bid':bid})
	if dddb.rowcount == 1:
		return True
	else:
		return False

def add_committeeauthors_db(year, dddb):
	bills = get_committeeauthors_api(year)
	cid = get_cid_db(dddb)

	print "cid", cid
	if cid is not None:
		for bill in bills:
			insert_committeeauthors_db(bill, cid, year, dddb)
	else:
		print "Fill Committee table first"

def get_cid_db(dddb):
	select_comm = '''SELECT * FROM Committee
						WHERE house = 'Senate'
                      	AND name = 'Rules'
                      	AND state = 'NY'
                  '''
	dddb.execute(select_comm)
	
	if dddb.rowcount == 1:
		return dddb.fetchone()[0]
	return None

def main():
	dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
						user='awsDB',
						db='JohnTest',
						port=3306,
						passwd='digitaldemocracy789')
	dddb = dddb_conn.cursor()
	dddb_conn.autocommit(True)
	add_committeeauthors_db(2015, dddb)
main()
