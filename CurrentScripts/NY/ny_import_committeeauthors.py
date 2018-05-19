#!/usr/bin/python3
"""
File: import_committeeauthors_ny.py
Author: Min Eric Roh
Date: 12/26/2015
Description:
- Imports NY CommitteeAuthors using senate API
- Fills authors
- Needs Committee table to be filled first
- Currently configured to test DB
"""

import json
import sys
import traceback
import requests
import MySQLdb
from Utils.Generic_Utils import *
from Utils.Generic_MySQL import get_session_year
from Utils.Database_Connection import connect

logger = None

INSERTED = 0

US_STATE = 'NY'

# URL
URL = ('http://legislation.nysenate.gov/api/3/%(restCall)s/%(year)s%(house)s/' +
       'search?term=sponsor.rules:true&full=true&limit=1000&key=' +
       '31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=%(offset)s')

# INSERTS
QI_COMMITTEEAUTHORS = ''' INSERT INTO CommitteeAuthors
              (cid, bid, vid, state)
              VALUES
              (%s, %s, %s, 'NY')'''

# SELECTS
QS_COMMITTEEAUTHORS_CHECK = ''' SELECT *
                FROM CommitteeAuthors
                WHERE cid = %s
                 AND bid = %s
                 AND vid = %s
                 AND state = 'NY' '''
QS_COMMITTEE = '''SELECT * FROM Committee
            WHERE house = 'Senate'
                        AND short_name = 'Rules'
                        AND state = 'NY'
                        AND session_year = %(year)s'''
QS_BILL = ''' SELECT * FROM Bill
        WHERE bid = %(bid)s'''


def call_senate_api(restCall, year, house, offset):
    if house != "":
        house = "/" + house
    url = URL % {'restCall': restCall, 'year': str(year), 'house': house, 'offset': str(offset)}
    r = requests.get(url)
    print(url)
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
    print(len(ret_bills))
    return ret_bills


def insert_committeeauthors_db(bill, cid, year, dddb):
    global INSERTED
    for key in bill['versions'].keys():
        if check_bid_db(bill['bid'], dddb):
            a = dict()
            a['bid'] = bill['bid']
            a['vid'] = bill['bid'] + key

            dddb.execute(QS_COMMITTEEAUTHORS_CHECK, (str(cid), a['bid'], a['vid']))
            if dddb.rowcount == 0:
                try:
                    dddb.execute(QI_COMMITTEEAUTHORS, (str(cid), a['bid'], a['vid']))
                    INSERTED += dddb.rowcount
                except MySQLdb.Error:
                    logger.exception(format_logger_message('Insert failed for CommitteeAuthors',
                                                           (QI_COMMITTEEAUTHORS % (str(cid), a['bid'], a['vid']))))


def check_bid_db(bid, dddb):
    dddb.execute(QS_BILL, {'bid': bid})
    if dddb.rowcount == 1:
        return True
    else:
        logger.exception('Bill not found ' + bid)
        return False


def add_committeeauthors_db(year, dddb):
    bills = get_committeeauthors_api(year)
    cid = get_cid_db(dddb, year)

    print("cid", cid)
    if cid is not None:
        for bill in bills:
            insert_committeeauthors_db(bill, cid, year, dddb)
    else:
        print("Fill Committee table first")


def get_cid_db(dddb, year):
    dddb.execute(QS_COMMITTEE, {'year': year})

    if dddb.rowcount == 1:
        return dddb.fetchone()[0]
    return None


def main():
    with connect() as dddb:
        year = get_session_year(dddb, 'NY', logger)
        add_committeeauthors_db(year, dddb)

    LOG = {'tables': [{'state': 'NY', 'name': 'CommitteeAuthors', 'inserted': INSERTED, 'updated': 0, 'deleted': 0}]}
    sys.stderr.write(json.dumps(LOG))
    logger.info(LOG)


if __name__ == '__main__':
    logger = create_logger()
    main()
