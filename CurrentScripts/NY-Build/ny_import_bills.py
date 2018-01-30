#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: ny_import_bills.py
Author: John Alkire
maintained: Miguel Aguilar, Eric Roh, James Ly
Date: 11/26/2015
Last Update: 1/31/2017
Description:
- Imports NY bills using senate API
- Fills Bill and BillVersion
- Currently configured to test DB
'''

import json
import sys
import traceback
import requests
import MySQLdb
import time
from datetime import datetime
from Utils.Generic_Utils import *
from Utils.Database_Connection import connect

logger = None
INSERTED = 0
BV_INSERTED = 0
UPDATED = 0
BV_UPDATED = 0

update_billversion = '''UPDATE BillVersion
                        SET bid = %(bid)s, date = %(date)s, state = %(state)s, 
                         subject = %(subject)s, title = %(title)s, text = %(text)s,
                         digest = %(digest)s, billState = %(billState)s
                        WHERE vid = %(vid)s;'''

update_bill = '''UPDATE Bill
                  SET number = %(number)s, type = %(type)s, status = %(status)s, 
                   house = %(house)s, state = %(state)s, session = %(session)s, 
                   sessionYear = %(sessionYear)s, billState = %(billState)s
                  WHERE bid = %(bid)s;'''

update_Bill_billState = '''UPDATE Bill
                            SET billState = %(billState)s
                            WHERE bid = %(bid)s'''

update_BillVersion_billState = '''UPDATE BillVersion
                                    SET billState = %(billState)s
                                    WHERE vid = %(vid)s'''

insert_bill = '''INSERT INTO Bill
                  (bid, number, type, status, house, state, session, sessionYear, billState)
                 VALUES
                  (%(bid)s, %(number)s, %(type)s, %(status)s, %(house)s, %(state)s,
                  %(session)s, %(sessionYear)s, %(billState)s);'''

insert_billversion = '''INSERT INTO BillVersion
                         (vid, bid, date, state, subject, title, text, digest, billState)
                        VALUES
                         (%(vid)s, %(bid)s, %(date)s, %(state)s, %(subject)s, %(title)s,
                         %(text)s, %(digest)s, %(billState)s);'''

select_bill = '''SELECT bid 
                 FROM Bill   
                 WHERE bid = %(bid)s'''

select_billversion = '''SELECT vid 
                        FROM BillVersion   
                        WHERE vid = %(vid)s'''

API_YEAR = datetime.now().year
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}&"
STATE = 'NY'
BILL_API_INCREMENT = 1000


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'NY'
    }


# calls NY Senate API and returns a tuple with the list of results and the total number of results
def call_senate_api(restCall, house, offset, resolution):
    if house != "":
        house = "/" + house

    if resolution:
        api_url = API_URL + "term=billType.resolution:true"
    else:
        api_url = API_URL + "term=billType.resolution:false"

    url = api_url.format(restCall, API_YEAR, house, offset)
    print url
    r = requests.get(url)
    out = r.json()

    return (out["result"]["items"], out['total'])


# function to compile all bill data from NY senate API. There are over 1000 bills
# so the API is looped over in 1000 bill increments. Data is placed into lists
# and dictionaries. List of bill dictionaries is returned
def get_bills_api(resolution):
    total = BILL_API_INCREMENT
    cur_offset = 0
    ret_bills = list()

    while cur_offset < total:
        call = call_senate_api("bills", "/search", cur_offset, resolution)
        bills = call[0]
        # TODO change this back if the api is fixed. API doesnt allow for offset greater than 10k
        # total = call[1]
        total = call[1]

        for bill in bills:
            bill = bill['result']
            b = dict()
            b['number'] = bill['basePrintNo'][1:]
            b['type'] = bill['basePrintNo'][0:1]
            b['status'] = bill['status']['statusDesc']
            b['house'] = bill['billType']['chamber'].title()
            b['state'] = STATE
            b['session'] = '0'
            b['sessionYear'] = bill['session']
            b['title'] = bill['title']
            b['versions'] = bill['amendments']['items']
            b['bid'] = STATE + "_" + str(bill['session']) + str(int(bill['session']) + 1)
            b['bid'] += b['session'] + b['type'] + b['number']
            b['summary'] = bill['summary']
            ret_bills.append(b)

        cur_offset += BILL_API_INCREMENT

    # print "Downloaded %d bills..." % len(ret_bills)
    return ret_bills


# function to insert a new bill or update an existing one. Returns true if
# insertion occurs, false otherwise
def insert_bill_db(bill, dddb):
    global INSERTED, UPDATED
    temp = {'bid': bill['bid'], 'number': bill['number'], 'type': bill['type'], 'status': bill['status'],
            'house': bill['house'], 'state': bill['state'], 'session': bill['session'],
            'sessionYear': bill['sessionYear'], 'billState': bill['status']}

    lastVersion = str(bill['versions'].keys()[-1])
    billVid = bill['bid'] + lastVersion
    if billVid[-1].isalpha():
        temp['billState'] = "Amended " + billVid[-1].upper();
    elif billVid[-1].isdigit():
        temp['billState'] = "Introduced"

    if not is_bill_in_db(bill, dddb):
        try:
            dddb.execute(insert_bill, temp)
            INSERTED += dddb.rowcount
        except MySQLdb.Error:
            logger.exception('Insert failed for Bill', (insert_bill % temp))

    else:
        try:
            dddb.execute(update_bill, temp)
            UPDATED += dddb.rowcount
        except MySQLdb.Error:
            logger.exception('Update failed for Bill', (update_bill % temp))

            return False

    return True


# checks if a bill is in the DB based on a generated BID
# returns true/false as expected
def is_bill_in_db(bill, dddb):
    # print type(bill), bill.keys()
    dddb.execute(select_bill, {'bid': bill['bid'], })
    bill_bid = dddb.fetchone()

    if bill_bid is None:
        return False
    if bill_bid[0] == bill['bid']:
        return True

    return False


# checks if a billversion is in the DB based on a generated VID
# returns true/false as expected
def is_bv_in_db(bv, dddb):
    dddb.execute(select_billversion, bv)
    bv_vid = dddb.fetchone()

    if bv_vid is None:
        return False
    if bv_vid[0] == bv['vid']:
        return True

    return False


# function to parse over and insert billversions. Called once per bill;
# inserts or updates corresponding number of bill versions
def insert_billversions_db(bill, dddb):
    global BV_INSERTED, BV_UPDATED
    for key in bill['versions'].keys():
        bv = dict()
        bv['bid'] = bill['bid']
        bv['vid'] = bill['bid'] + key
        bv['date'] = bill['versions'][key]['publishDate']
        bv['state'] = STATE
        bv['subject'] = bill['title']
        bv['title'] = bill['versions'][key]['actClause']
        bv['text'] = bill['versions'][key]['fullText']
        bv['digest'] = bill['summary']
        bv['billState'] = bill['status']

        if bv['vid'][-1].isalpha():
            bv['billState'] = 'Amended ' + str(bv['vid'][-1]).upper()
        elif bv['vid'][-1].isdigit():
            bv['billState'] = 'Introduced'

        if not is_bv_in_db(bv, dddb):
            try:
                dddb.execute(insert_billversion, bv)
                BV_INSERTED += dddb.rowcount
            except MySQLdb.Error:
                logger.exception('Insert failed for BillVersion', (insert_billversion % bv))
        else:
            try:
                if bv['digest'] == '':
                    bv['digest'] = None
                dddb.execute(update_billversion, bv)
                BV_UPDATED += dddb.rowcount
            except MySQLdb.Error:
                logger.exception('Update failed for BillVersion', (update_billversion % bv))


# function to loop over all bills and insert bills and bill versions
def add_bills_db(dddb):
    # Resolution passed in, if 'True' then gets all the resolutions
    bills = get_bills_api(False)
    bills.extend(get_bills_api(True))
    bcount = 0

    for bill in bills:
        if insert_bill_db(bill, dddb):
            bcount = bcount + 1
        insert_billversions_db(bill, dddb)

        # print "Inserted %d bills" % bcount


def main():
    global API_YEAR
    API_YEAR = datetime.now().year

    with connect() as dddb:
        add_bills_db(dddb)


    LOG = {'tables': [{'state': 'NY', 'name': 'Bill', 'inserted': INSERTED, 'updated': UPDATED, 'deleted': 0},
                      {'state': 'NY', 'name': 'BillVersion', 'inserted': BV_INSERTED, 'updated': BV_UPDATED,
                       'deleted': 0}]}
    sys.stdout.write(json.dumps(LOG))
    logger.info(LOG)


if __name__ == '__main__':
    logger = create_logger()
    main()
