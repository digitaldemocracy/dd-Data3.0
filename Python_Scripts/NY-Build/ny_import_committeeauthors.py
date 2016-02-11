#!/usr/bin/env python
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
import loggingdb

US_STATE = 'NY'

# URL
URL = ('http://legislation.nysenate.gov/api/3/%(restCall)s/%(year)s%(house)s/' +
	'search?term=sponsor.rules:true&full=true&limit=1000&key=' +
	'31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=%(offset)s')

# INSERTS
QI_COMMITTEEAUTHORS = '''	INSERT INTO CommitteeAuthors
							(cid, bid, vid, state)
							VALUES
							(%s, %s, %s, 'NY')'''

# SELECTS
QS_COMMITTEEAUTHORS_CHECK = '''	SELECT *
								FROM CommitteeAuthors
								WHERE cid = %s
								 AND bid = %s
								 AND vid = %s
								 AND state = 'NY' '''
QS_COMMITTEE = '''SELECT * FROM Committee
						WHERE house = 'Senate'
                      	AND name = 'Rules'
                      	AND state = 'NY' '''
QS_BILL = '''	SELECT * FROM Bill
				WHERE bid = %(bid)s'''


def call_senate_api(restCall, year, house, offset):
	if house != "":
		house = "/" + house
	url = URL % {'restCall':restCall, 'year':str(year), 'house':house, 'offset':str(offset)}
	r = requests.get(url)
	print url
	out = r.json()
	return out["result"]["items"]

def get_committeeauthors_api(year):
	bills = call_senate_api("bills", year, "", 1)
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
	for key in bill['versions'].keys():
		if check_bid_db(bill['bid'], dddb):
			a = dict()
			a['bid'] = bill['bid']
			a['vid'] = bill['bid'] + key

			dddb.execute(QS_COMMITTEEAUTHORS_CHECK, (str(cid), a['bid'], a['vid']))
			if dddb.rowcount == 0:
				dddb.execute(QI_COMMITTEEAUTHORS, (str(cid), a['bid'], a['vid']))
			else:
				print "already existing"
		else:
			print bill['bid'], "fill Bill table first"

def check_bid_db(bid, dddb):
	dddb.execute(QS_BILL, {'bid':bid})
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
	dddb.execute(QS_COMMITTEE)
	
	if dddb.rowcount == 1:
		return dddb.fetchone()[0]
	return None

def main():
	with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
						user='awsDB',
						db='DDDB2015Dec',
						port=3306,
						passwd='digitaldemocracy789',
						charset='utf8') as dddb:
#		dddb = dddb_conn.cursor()
#		dddb_conn.autocommit(True)
		add_committeeauthors_db(2015, dddb)
main()