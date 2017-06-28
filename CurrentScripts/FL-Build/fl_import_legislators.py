#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: fl_import_legislators.py
Author: Miguel Aguilar
Maintained: Nick Russo
Date: 07/05/2016
Last Updated: 03/18/2017

Description:
  - This script populates the database with the Florida state legislators

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
from Constants.Legislator_Queries import *
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import *

logger = None
API_URL = 'http://openstates.org/api/v1/legislators/?state=fl&chamber={0}&apikey=c12c4c7e02c04976865f3f9e95c3275b'

#Globals
P_INSERT = 0
L_INSERT = 0
T_INSERT = 0
T_UPDATE = 0



'''
The function checks to see if a term entry already exists
in the DB. 
'''
def is_term_in_db(dddb, leg):
  global T_UPDATE

  dddb.execute(QS_TERM, leg)
  query = dddb.fetchone()

  if query is None:
    return False
  print(leg['district'])
  if query[0] != leg['district']:
    try:
      dddb.execute(QU_TERM, leg)
      T_UPDATE += dddb.rowcount
      return True
    except MySQLdb.Error:
      logger.warning('Update Failed', full_msg=traceback.format_exc(),
                  additional_fields=create_payload('Term', (QU_TERM%leg)))
      return False

  return True


'''
This function checks to see if a legislator is already 
in the DB. Returns true or false.
'''
def is_leg_in_db(dddb, leg):
  try:
    dddb.execute(QS_LEGISLATOR, leg)
    query = dddb.fetchone()
    if query is None:
      return False
  except:
    return False

  return query[0]


'''
This function adds the legislators into the Person, Term, and Legislator
table, if it doesn't exist already in the DB. 
'''
def add_legislators_db(dddb, leg_list):
  global P_INSERT
  global T_INSERT
  global L_INSERT

  #For all the legislators from OpenStates API
  for leg in leg_list:
    pid = is_leg_in_db(dddb, leg)
    leg['pid'] = pid

    #Insert into Person table first
    if not pid:
      try:
        dddb.execute(QI_PERSON, leg)
        pid = dddb.lastrowid
        leg['pid'] = pid
        dddb.execute(QI_PERSONSTATE, leg)
        dddb.execute(QI_ALTID, leg)
        P_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Person', (QI_PERSON%leg)))

      #Insert into Legislator table next
      try:
        print(leg)
        dddb.execute(QI_LEGISLATOR, leg)
        L_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Legislator', (QI_LEGISLATOR%leg)))

    #Finally insert into Term table
    test = is_term_in_db(dddb, leg)
    print(test)
    if test  == False:
      try:
        dddb.execute(QI_TERM, leg)
        T_INSERT += dddb.rowcount
      except MySQLdb.Error:
        print(traceback.format_exc())
        print((QI_TERM%leg))
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Term', (QI_TERM%leg)))


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
        with GrayLogger(GRAY_LOGGER_URL) as _logger:
            logger = _logger
            add_legislators_db(dddb, get_legislators_list("fl"))
            logger.info(__file__ + ' terminated successfully.',
            full_msg='Updated ' + str(T_UPDATE) + ' rows in Legislator',
            additional_fields={'_affected_rows':'Legislator'+str(T_UPDATE),
                '_updated':'BillVersion:'+str(T_UPDATE),
                '_state':'FL',
                '_log_type':'Database'})
    LOG = {'tables': [{'state': 'FL', 'name': 'Florida Legislator', 'Legislators inserted':L_INSERT , 'Term inserted':T_INSERT, 'deleted': 0}]}
    sys.stderr.write(json.dumps(LOG))

