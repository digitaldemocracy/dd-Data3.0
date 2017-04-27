#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: tx_import_legislators.py
Author: Nick Russo
Maintained: Nick Russo
Date: 04/25/2017
Last Updated: 03/18/2017

Description:
  - This script populates the database with the Texas state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
'''

import datetime
import MySQLdb
import traceback
import sys
from Database_Connection import mysql_connection
from graylogger.graylogger import GrayLogger
from legislators_API_helper import *
from import_legislator import *
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None




if __name__ == "__main__":
    dbinfo = mysql_connection(sys.argv)
    # MUST SPECIFY charset='utf8' OR BAD THINGS WILL HAPPEN.
    with MySQLdb.connect(host=dbinfo['host'],
                                      port=dbinfo['port'],
                                      db=dbinfo['db'],
                                      user=dbinfo['user'],
                                      passwd=dbinfo['passwd'],
                                      charset='utf8') as dddb:
        pi_count = ti_count = 0
        with GrayLogger(API_URL) as _logger:
            logger = _logger
            add_legislators_db(dddb, get_legislators_list("tx"))
            logger.info(__file__ + ' terminated successfully.',
            full_msg='Updated ' + str(T_UPDATE) + ' rows in Legislator',
            additional_fields={'_affected_rows':'Legislator'+str(T_UPDATE),
                '_updated':'BillVersion:'+str(T_UPDATE),
                '_state':'TX',
                '_log_type':'Database'})
    LOG = {'tables': [{'state': 'TX', 'name': 'Texas Legislator', 'Legislators inserted':L_INSERT , 'Term inserted':T_INSERT, 'deleted': 0}]}
    sys.stderr.write(json.dumps(LOG))

