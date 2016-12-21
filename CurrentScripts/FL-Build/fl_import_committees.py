#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: fl_import_committees.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 04/26/2016
Last Updated: 08/12/2016

Description:
  - This script populates the database with the Florida state committees
  and its members.

Source:
  - Open States API

Populates:
  - Committee (cid, house, name, type, state)
  - servesOn (pid, year, house, cid, state, position)
'''

import re
import datetime
import requests
import MySQLdb
import sys
import traceback
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

#Globals
C_INSERT = 0
S_INSERT = 0
S_DELETE = 0

#Selects
QS_FLOOR = '''SELECT p.pid
              FROM Legislator l, Person p, Term t
              WHERE l.state=%(state)s
              AND t.house=%(house)s
              AND l.pid=p.pid
              AND t.pid=p.pid'''

QS_COMMITTEE_MAX = '''SELECT cid FROM Committee
                      ORDER BY cid DESC
                      LIMIT 1'''

QS_PERSON = '''SELECT p.pid
              FROM Person p, Legislator l
              WHERE first = %(first)s AND last = %(last)s
              AND state = %(state)s AND p.pid = l.pid'''

QS_COMMITTEE = '''SELECT cid
                  FROM Committee
                  WHERE house = %(house)s
                  AND (name = %(name)s OR short_name = %(short_name)s)
                  AND state = %(state)s'''

QS_SERVESON = '''SELECT pid 
                FROM servesOn
                WHERE pid = %(pid)s 
                AND year = %(year)s 
                AND house = %(house)s 
                AND cid = %(cid)s 
                AND state = %(state)s'''

#Inserts
QI_COMMITTEE = '''INSERT INTO Committee 
                  (cid, house, name, type, state, short_name)
                  VALUES 
                  (%(cid)s, %(house)s, %(name)s, %(type)s, %(state)s, %(short_name)s)'''

QI_SERVESON = '''INSERT INTO servesOn 
                  (pid, year, house, cid, state, position)
                  VALUES 
                  (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s, %(position)s)'''

#Deletes
QD_SERVESON = '''DELETE FROM servesOn
                WHERE cid = %s
                AND house = %s
                AND year <= %s
                AND state = %s'''


API_URL = 'http://openstates.org/api/v1/committees/?state=fl&apikey={0}'
API_URL2 = 'http://openstates.org/api/v1/committees/{0}/?apikey={1}'
API_KEY = '92645427ddcc46db90a8fb5b79bc9439'
API_YEAR = 2015
STATE = 'FL'
YEAR = datetime.datetime.now().year


def create_payload(table, sqlstmt):                                             
  return {
    '_table': table,
    '_sqlstmt': sqlstmt,
    '_state': 'FL'
  }

'''
This function cleans the name of the legislators
into a common format.
'''
def clean_name(name):
  problem_names = {
    "Miguel Diaz de la Portilla":("Miguel", "Diaz de la Portilla"),
    "Charles Van Zant":("Charles", "Van Zant"), 
    "Mike La Rosa":("Mike", "La Rosa"),
    "Charlie Dean, Sr.":("Charles", "Dean, Sr."), 
    "Mike Hill":("Walter Bryan", "Hill"), 
    "Bob Cortes":("Robert","Cortes"), 
    "Danny Burgess, Jr.":("Daniel Wright","Burgess, Jr."),
    "Coach P Plasencia":("Rene","Plasencia"),
    }
    
  #IF THERE IS ONE OR TWO COMMAS THEN FLIP THE NAME
  if name.count(',') == 1:
    name_flip = name.split(',')
    if ' Jr.' not in name_flip and ' Sr.' not in name_flip:
      name_flip.reverse()
    name = ', '.join(name_flip)
  elif name.count(',') > 1:
    name_flip = name.split(',')
    if name_flip[2] == ' Jr.' or name_flip[2] == ' Sr.':
      name_flip[1], name_flip[0] = name_flip[0], name_flip[1]
    elif name_flip[1] == ' Jr.' or name_flip[1] == ' Sr.':
      name_flip[2], name_flip[0] = name_flip[0], name_flip[2]
    name = ', '.join(name_flip)

  ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
  name = name.replace(',', ' ')
  name = name.replace('.', ' ')
  name = name.replace('  ', ' ')
  name_arr = name.split()      
  suffix = "";         

  if len(name_arr) == 1 and name_arr[0] in problem_names.keys():
    name_arr = list(problem_names[name_arr[0]])

        
  for word in name_arr:
    if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
      name_arr.remove(word)
      if word in ending.keys():
        suffix = ending[word]            
          
  first = name_arr.pop(0)
  
  while len(name_arr) > 1:
    first = first + ' ' + name_arr.pop(0)     
             
  last = name_arr[0]
  last = last.replace(' ' ,'') + suffix
  
  if (first + ' ' + last) in problem_names.keys():             
    return problem_names[(first + ' ' + last)]

  return (first, last)

'''
This function gets the legislative member ID (pid).
Includes handling of problem names. 
'''
def get_member_pid_db(dddb, name):
  prob_names = ['Darryl Ervin','Victor Manuel', 'W Travis', 'Jared Evan', 'Ray Wesley', 'Maria Lorts']
  per = {}

  if 'Representative' in name or 'Senator' in name:
    name = ' '.join(name.split()[1:])

  per['first'], per['last'] = clean_name(name)
  per['state'] = STATE

  if '"' in per['first']:
    per['first'] = re.sub(' "(.*?)"', '', per['first'])
  if len(per['first']) > 1 and per['first'] in prob_names:
    per['first'] = per['first'].split()[0]
  if 'Javier' in per['first']:
    per['first'] = 'José'
  if per['last'] == 'Moraitis':
    per['first'] = 'George'
    per['last'] = per['last'] + ', Jr.'
  if per['last'] == 'Braynon':
    per['first'] = 'Oscar'
    per['last'] = per['last'] + ' II'
  if per['last'] == 'Brandes':
    per['first'] = 'Jeffrey'

  dddb.execute(QS_PERSON, per)
  if dddb.rowcount == 0:
    print 'Name not found'
    pid = None
  else:
    pid = dddb.fetchone()[0]

  return pid

'''
This function gets all the committee members given a 
committee id and a house through OpenStates API.
'''
def get_comm_members_api(dddb, comm_id, house):
  url = API_URL2.format(comm_id, API_KEY)
  comm_json = requests.get(url).json()

  member_list = []
  for mem in comm_json['members']:
    member = {}
    member['name'] = mem['name']
    member['position'] = mem['role'].capitalize()
    #NOT SURE if alternating chair is the same as co-chair
    if mem['role'] == 'Alternating Chair':
      member['position'] = 'Co-Chair'
    elif 'Members' in mem['role']:
      member['position'] = 'Member' 
    if member['position'] not in ['Chair', 'Vice-Chair', 'Co-Chair', 'Member']:
      print 'START TRUNCATED'
      print mem['role']
      print 'END TRUNCATED'
    member['year'] = YEAR
    member['state'] = STATE
    member['house'] = house
    member['pid'] = get_member_pid_db(dddb, mem['name'])

    if member['pid'] is None:
      print 'Member Not Found'
    else:
      member_list.append(member)

  return member_list

'''
This gets the list of Florida committees from OpenStates API.
Every committee is cleaned-up and formated into a dictionary.
Returns a list of dictionaries.
'''
def get_committees_api(dddb):
  url = API_URL.format(API_KEY)
  comm_json = requests.get(url).json()

  comm_list = []
  for entry in comm_json:
    comm = {}
    if entry['subcommittee'] is None:
      comm['type'] = 'Standing'
      comm['name'] = entry['committee']
      comm['short_name'] = entry['committee']
    else:
      comm['type'] = 'Subcommittee'
      if entry['committee'] in entry['subcommittee']:
        comm['name'] = entry['subcommittee'] + 'Subcommittee'
        comm['short_name'] = ' '.join(entry['subcommittee'].split()[:-1])
      else:
        comm['name'] = ' '.join([entry['subcommittee'], entry['committee'], 'Subcommittee']) 
        comm['short_name'] = entry['subcommittee']

    if 'Joint' in entry['committee']:
      comm['type'] = 'Joint'
    elif 'Select' in entry['committee']:
      comm['type'] = 'Select'

    if entry['chamber'] == 'upper':
      comm['house'] = 'Senate'
    elif entry['chamber'] == 'lower':
      comm['house'] = 'Assembly'
    else:
      comm['house'] = 'Joint'

    comm['state'] = 'FL'
    comm['members'] = get_comm_members_api(dddb, entry['id'], comm['house'])

    comm_list.append(comm)

  return comm_list 

'''
This function clears the current people in the servesOn table
given a committee id (cid) and house
'''
def clear_servesOn_db(dddb, cid, house):
  global S_DELETE

  try:
    # Delete previous entries in order to insert the latest ones
    dddb.execute(QD_SERVESON, (cid, house, YEAR, STATE))
    S_DELETE += dddb.rowcount
  except MySQLdb.Error:
    logger.warning('Delete Failed', full_msg=traceback.format_exc(),
      additional_fields=create_payload('servesOn',(QD_SERVESON%(cid, house, YEAR, STATE))))

'''
This function inserts an entry into the servesOn table.
'''
def insert_servesOn_db(dddb, mem_list, cid):
  global S_INSERT

  for mem in mem_list:
    mem['cid'] = cid
    dddb.execute(QS_SERVESON, mem)
    if dddb.rowcount == 0:
      try:
        dddb.execute(QI_SERVESON, mem)
        S_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('servesOn', (QI_SERVESON%mem)))

'''
This function inserts the rest of the committees into the Committee table
excludes the floors.
'''
def insert_committees_db(dddb, comm_list):
  global C_INSERT

  for comm in comm_list:
    dddb.execute(QS_COMMITTEE, comm)
    if dddb.rowcount == 0:
      dddb.execute(QS_COMMITTEE_MAX)
      comm['cid'] = dddb.fetchone()[0] + 1
      try:
        dddb.execute(QI_COMMITTEE, comm)
        C_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Committee', (QI_COMMITTEE%comm)))
    else:
      comm['cid'] = dddb.fetchone()[0]

    clear_servesOn_db(dddb, comm['cid'], comm['house'])
    insert_servesOn_db(dddb, comm['members'], comm['cid'])

'''
This function inserts the floor committees into the Committee table
and it also inserts the legislators that serve on them into the
servesOn table.
'''
def insert_floor_committees_db(dddb):
  global C_INSERT
  comm_list = [{'cid':'', 'house':'Assembly', 'name':'Assembly Floor', 'type':'Floor', 'state':'FL', 'short_name':'Assembly Floor'},
              {'cid':'', 'house':'Senate', 'name':'Senate Floor', 'type':'Floor', 'state':'FL', 'short_name':'Senate Floor'}]

  #Insert the floor committees into Committee table
  for comm in comm_list:
    dddb.execute(QS_COMMITTEE, comm)
    if dddb.rowcount == 0:
      dddb.execute(QS_COMMITTEE_MAX)
      comm['cid'] = dddb.fetchone()[0] + 1
      try:
        dddb.execute(QI_COMMITTEE, comm)
        C_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Committee', (QI_COMMITTEE%comm)))
    else:
      comm['cid'] = dddb.fetchone()[0]

    dddb.execute(QS_FLOOR, comm)
    query = dddb.fetchall()
    pid_list = [int(x[0]) for x in query]

    #Insert legislative members into the servesOn table
    mem_list = []
    for pid in pid_list:
      mem = {}
      mem['pid'] = pid
      mem['position'] = 'Member'
      mem['year'] = YEAR
      mem['house'] = comm['house']
      mem['state'] = comm['state']
      mem_list.append(mem)
    clear_servesOn_db(dddb, comm['cid'], comm['house'])
    insert_servesOn_db(dddb, mem_list, comm['cid'])


def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    #Add the FL committees
    insert_floor_committees_db(dddb)
    insert_committees_db(dddb, get_committees_api(dddb))

    logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(C_INSERT) + ' rows in Committee and inserted ' 
                    + str(S_INSERT) + ' rows in servesOn',
          additional_fields={'_affected_rows':'Committee:'+str(C_INSERT)+
                                         ', servesOn:'+str(S_INSERT),
                             '_inserted':'Committee:'+str(C_INSERT)+
                                         ', servesOn:'+str(S_INSERT),
                             '_state':'FL'})

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()