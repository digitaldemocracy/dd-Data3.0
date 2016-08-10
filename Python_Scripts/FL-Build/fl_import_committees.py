#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: fl_import_committees.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 04/26/2016
Last Updated: 07/16/2016

Description:
  - This script populates the database with the Florida state committees
  and its members.

Source:
  - Open States API

Populates:
  - Committee (cid, house, name, type, state)
  - servesOn (pid, year, house, cid, state, position)
'''

import requests
import MySQLdb
import sys
import traceback
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

QS_COMMITTEE_MAX = '''SELECT cid FROM Committee
                      ORDER BY cid DESC
                      LIMIT 1'''

QS_PERSON = '''SELECT p.pid
              FROM Person p, Legislator l
              WHERE first = %(first)s AND last = %(last)s
              AND state = %(state)s AND p.pid = l.pid'''

QS_COMMITTEE = '''SELECT house
                  FROM Committee
                  WHERE cid = %(cid)s'''

QI_COMMITTEE = '''INSERT INTO Committee 
                  (cid, house, name, type, state)
                  VALUES 
                  (%(cid)s, %(house)s, %(name)s, %(type)s, %(state)s)'''

QI_SERVESON = '''INSERT INTO servesOn 
                  (pid, year, house, cid, state, position)
                  VALUES 
                  (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s, %(position)s)'''


API_URL = 'http://openstates.org/api/v1/committees/?state=fl&apikey={0}'
API_URL2 = 'http://openstates.org/api/v1/committees/{0}/?apikey={1}'
API_KEY = '92645427ddcc46db90a8fb5b79bc9439'
API_YEAR = 2015


def create_payload(table, sqlstmt):                                             
  return {
    '_table': table,
    '_sqlstmt': sqlstmt,
    '_state': 'FL'
  }

def get_member_pid_db(dddb, name):
  pass

def get_comm_members_api(dddb, comm_id):
  url = API_URL2.format(comm_id, API_KEY)
  comm_json = requests.get(url).json()

  member_list = []
  for mem in comm_json['members']:
    member = {}
    member['name'] = mem['name']
    member['role'] = mem['role']
    member['pid'] = get_member_pid_db(dddb, mem['name'])

    member_list.append(member)

  return member_list

def get_committees_api(dddb):
  url = API_URL.format(API_KEY)
  comm_json = requests.get(url).json()

  comm_list = []
  for entry in comm_json:
    comm = {}
    if entry['subcommittee'] is None:
      comm['type'] = 'Standing'
      comm['name'] = entry['committee']
    else:
      comm['type'] = 'Subcommittee'
      comm['name'] = entry['subcommittee']

    if 'Joint' in entry['committee']:
      comm['type'] = 'Joint'
    elif 'Select' in entry['committee']:
      comm['type'] = 'Select'
    elif ''

    if entry['chamber'] == 'upper':
      comm['house'] = 'Senate'
    elif entry['chamber'] == 'lower':
      comm['house'] = 'Assembly'
    else:
      comm['house'] = 'Joint'

    comm['state'] = 'FL'
    comm['members'] = get_comm_members_api(dddb, entry['id'])

    comm_list.append(comm)

  return comm_list 

def insert_committees_db(dddb, comm_list):
  pass

def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='MikeyTest',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    #Add the FL committees 
    insert_committees_db(dddb, get_committees_api(dddb))

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()