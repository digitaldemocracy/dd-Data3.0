#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: fl_import_bills.py
Author: Eric Roh
Date: 
Description:
- Imports FL bills using sunlight API
- Fills Bill and BillVersion
- Currently configured to test DB
'''

import requests
import MySQLdb
import pprint

# URL
API_URL = '''http://openstates.org/api/v1/bills/?state=fl&chamber=upper&apikey=92645427ddcc46db90a8fb5b79bc9439'''
BILL_DETAIL_URL = 'http://openstates.org/api/v1/bills/fl/%(session)s/%(bill_id)s/?apikey=92645427ddcc46db90a8fb5b79bc9439'

# INSERTS
QI_BILL = '''INSERT INTO Bill
       (bid, type, number, billState, status, house, session, state, sessionYear)
       VALUES
       (%(bid)s, %(type)s, %(number)s, %(billState)s, %(status)s, 
       %(house)s, %(session)s, 'FL', %(sessionYear)s)'''
QI_BILLVERSION = '''INSERT INTO BillVersion
          (vid, bid, date, billState, subject, subject, appropriation, 
          substantive_changes, title, digest, text, state)
          VALUES
          (%(vid)s, %(bid)s, %(date)s, %(billState)s, %(subject)s, 
          %(subject)s, %(appropriation)s, %(substantive_changes)s, 
          %(title)s, %(digest)s, %(text)s, 'FL')'''


def call_api():
  r = requests.get(API_URL)
  print API_URL
  out = r.json()
  return out

def get_bill_info(session, bill_id):
  bid = bill_id.replace(' ', '%20')
  r = requests.get(BILL_DETAIL_URL % {'session':session, 'bill_id':bid})
  print BILL_DETAIL_URL % {'session':session, 'bill_id':bid}
  out = r.json()
  return out

def insert_bills(bills):
  print len(bills)
  total = 0
  for bill in bills:
    info = get_bill_info(bill['session'], bill['bill_id'])
    total += len(info['versions'])
  print total

def main():
  result = call_api()
  #print len(result), type(result)
  #print type(result[0]), result[0]
  info = get_bill_info(result[2]['session'], result[2]['bill_id'])
  print len(info['versions'])
  pp = pprint.PrettyPrinter(indent=2)
  pp.pprint(info['versions'])
  insert_bills(result)


main()
