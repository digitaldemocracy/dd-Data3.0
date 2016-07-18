#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: fl_import_committees.py
Author: Miguel Aguilar
Date: 04/26/2016

Description:
  - This script populates the database with the Florida state committees
  and its members.

Source:
  - Open States API

Populates:
  - Committee (cid, house, name, type, state)
  - servesOn (pid, year, house, cid, state, position)
  - House (name, state, type)
'''

import requests
import MySQLdb
import sys
import traceback
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

select_last_committee = '''
                        SELECT cid FROM Committee
                        ORDER BY cid DESC
                        LIMIT 1;
                        '''

select_person = '''
                SELECT p.pid
                FROM Person p, Legislator l
                WHERE first = %(first)s AND last = %(last)s
                AND state = %(state)s AND p.pid = l.pid;
                '''

select_house = '''
                SELECT *
                FROM House
                WHERE name = %(name)s AND state = %(state)s;
                '''

select_committee = '''
                    SELECT house
                    FROM Committee
                    WHERE cid = %(cid)s;
                    '''

insert_committee = '''
                    INSERT INTO Committee (cid, house, name, state)
                    VALUES (%(cid)s, %(house)s, %(name)s, %(state)s);
                    '''

insert_servesOn = '''
                    INSERT INTO servesOn (pid, year, house, cid, state, position)
                    VALUES (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s, %(position)s);
                    '''

insert_house = '''
                INSERT INTO House (name, state, type)
                VALUES (%(name)s, %(state)s, %(type)s);
                '''

API_url1 = 'http://openstates.org/api/v1/{0}/?state={1}&apikey={2}'
API_url2 = 'http://openstates.org/api/v1/{0}/{1}/?&apikey={2}'
API_key = '92645427ddcc46db90a8fb5b79bc9439'
API_year = 2015


def add_committees(dddb, comm_list):
  print('Nothing')
  #for comm in comm_list:
      #dddb.execute(insert_committee, comm)


def get_committees(cid):
  url = API_url1.format('committees', 'fl', API_key)
  comm_json = requests.get(url).json()

  id_cid = {}
  comm_list = []
  for j in comm_json:
    cid +=1

    if j['chamber'] == 'joint':
        chamber = 'Joint'
    elif j['chamber'] == 'upper':
        chamber = 'Senate'
    elif j['chamber'] == 'lower':
        chamber = 'Assembly'
    else:
        chamber = ''

    id_cid[cid] = j['id']
    committee = {'name': j['committee'], 'house': chamber, 'cid': cid, 'state':'FL'}
    comm_list.append(committee)

  return (comm_list, id_cid)


def get_last_cid(dddb):
  dddb.execute(select_last_committee)
  cid_fetch = dddb.fetchone()
  cid = cid_fetch[0]
  return cid

def add_fl_committees(dddb):
  cid = get_last_cid(dddb)
  comm_ids = get_committees(cid)
  add_committees(dddb, comm_ids[0])
  return comm_ids[1]

def add_fl_members(dddb, id_cid):
  for cid, ids in id_cid.iteritems():
    url = API_url2.format('committees', ids, API_key)
    comm_json = requests.get(url).json()
    #print(comm_json)

    mem_list = comm_json['members']
    for mem in mem_list:
      names = mem['name']
      print(names.encode('utf-8'))
      
    '''
    GET PID AND POSITION
    RETURN DIC OF PID AND (POSITION & CID)
    SELECT Committee AND GET House
    INSERT servesOn
    '''

def add_fl_house(dddb):
  a_fl = {'name': 'Assembly', 'state': 'FL', 'type': 'lower'}
  j_fl = {'name': 'Joint', 'state': 'FL', 'type': None}
  s_fl = {'name': 'Senate', 'state': 'FL', 'type': 'upper'}

  dddb.execute(select_house, a_fl)
  tmp = dddb.fetchone()
  if tmp is None:
    dddb.execute(insert_house, a_fl)

  dddb.execute(select_house, j_fl)
  tmp = dddb.fetchone()
  if tmp is None:
    dddb.execute(insert_house, j_fl)

  dddb.execute(select_house, s_fl)
  tmp = dddb.fetchone()
  if tmp is None:
    dddb.execute(insert_house, s_fl)


def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='MikeyTest',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    #Add the FL committees
    add_fl_house(dddb)
    id_cid = add_fl_committees(dddb)
    add_fl_members(dddb, id_cid)

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()