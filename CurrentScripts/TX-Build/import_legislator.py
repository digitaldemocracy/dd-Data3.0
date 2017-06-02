#!/usr/bin/env python2.6
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
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None
API_URL = 'http://openstates.org/api/v1/legislators/?state=fl&chamber={0}&apikey=c12c4c7e02c04976865f3f9e95c3275b'

#Globals
P_INSERT = 0
L_INSERT = 0
T_INSERT = 0
T_UPDATE = 0

#Selects
QS_LEGISLATOR = '''
                SELECT p.pid
                FROM Legislator l, Person p
                WHERE first=%(first)s
                AND last=%(last)s
                AND state=%(state)s
                AND l.pid=p.pid
                '''

QS_TERM = '''
          SELECT district
          FROM Term
          WHERE pid=%(pid)s
          AND state=%(state)s
          AND year=%(year)s
          AND house=%(house)s
          '''

#Inserts
QI_LEGISLATOR = '''
                INSERT INTO Legislator
                  (pid,state,capitol_phone,capitol_fax,website_url,room_number)
                VALUES
                  (%(pid)s,%(state)s,%(capitol_phone)s,%(capitol_fax)s,%(website_url)s,%(room_number)s)
                '''

QI_PERSON = '''
            INSERT INTO Person
              (first,middle,last, source, image)
            VALUES
              (%(first)s,%(middle)s,%(last)s,%(source)s,%(image)s)
            '''
QI_PERSONSTATE = '''
                INSERT INTO PersonStateAffiliation
                    (pid, state)
                VALUES
                    (%(pid)s,%(state)s)
                 '''
QI_ALTID = '''
           INSERT INTO AlternateId (pid, alt_id, source)
            VALUES (%(pid)s, %(alt_id)s, %(source)s)
            '''

QI_TERM = '''
          INSERT INTO Term
            (pid,year,house,state,district,party,current_term, start)
          VALUES
            (%(pid)s,%(year)s,%(house)s,%(state)s,%(district)s,%(party)s,%(current_term)s,%(start)s)
          '''

QU_TERM = '''
          UPDATE Term
          SET district=%(district)s
          WHERE pid=%(pid)s
          AND state=%(state)s
          AND year=%(year)s
          AND house=%(house)s
'''


def create_payload(table, sqlstmt):                                             
  return {
    '_table': table,
    '_sqlstmt': sqlstmt,
    '_state': 'FL'
  }

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
    print("here")
    if test  == False:
      try:
        dddb.execute(QI_TERM, leg)
        T_INSERT += dddb.rowcount
      except MySQLdb.Error:
        print(traceback.format_exc())
        print((QI_TERM%leg))
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Term', (QI_TERM%leg)))
