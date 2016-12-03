#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: fl_import_legislators.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 07/05/2016
Last Updated: 08/12/2016

Description:
  - This script populates the database with the Florida state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
'''

import datetime
import requests
import MySQLdb
import traceback
from graylogger.graylogger import GrayLogger
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
                  (pid,state,capitol_phone,website_url,room_number)
                VALUES
                  (%(pid)s,%(state)s,%(capitol_phone)s,%(website_url)s,%(room_number)s)
                '''

QI_PERSON = '''
            INSERT INTO Person
              (last,first,image,middle)
            VALUES
              (%(last)s,%(first)s,%(image)s,%(middle)s)
            '''

QI_TERM = '''
          INSERT INTO Term
            (pid,year,house,state,district,party)
          VALUES
            (%(pid)s,%(year)s,%(house)s,%(state)s,%(district)s,%(party)s)
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


def get_legislators_api(dddb, house):
  url = API_URL.format(house)
  req = requests.get(url)
  ret_list = req.json()

  leg_list = []
  for entry in ret_list:
    leg = {}

    clean_leg_name = clean_name(entry['full_name'])
    leg['middle'] = entry['middle_name']
    leg['first'] = clean_leg_name[0]
    leg['last'] = clean_leg_name[1]

    if leg['middle'] is not None:
      if len(leg['middle']) > 1 and leg['middle'] in clean_leg_name[0]:
        leg['first'] = clean_leg_name[0].split()[0]
      if len(leg['middle'])==1:
        leg['middle'] = leg['middle'] + '.'

    if ' "' in leg['first']:
      if len(leg['first'].split()[0]) > 1:
        leg['first'] = ' '.join(clean_leg_name[0].split(' "')[:-1])

    leg['image'] = entry['photo_url'].split('/')[-1]
    leg['district'] = entry['district']
    leg['state'] = entry['state'].upper()
    leg['website_url'] = entry['url']

    #Not sure what the term year should be, so I did the current year
    leg['year'] = datetime.datetime.now().year

    if entry['party'] == 'Republican':
      leg['party'] = entry['party']
    else:
      leg['party'] = 'Democrat'

    if house == 'upper':
      leg['house'] = 'Senate'
    else:
      leg['house'] = 'Assembly'

    for office in entry['offices']:
      if office['type'] == 'capitol':
        leg['room_number'] = office['address'].split()[0]
        leg['capitol_phone'] = office['phone']

    leg_list.append(leg)

  return leg_list

def is_term_in_db(dddb, leg):
  global T_UPDATE

  dddb.execute(QS_TERM, leg)
  query = dddb.fetchone()

  if query is None:
    return False

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

def is_leg_in_db(dddb, leg):
  try:
    dddb.execute(QS_LEGISLATOR, leg)
    query = dddb.fetchone()

    if query is None:
      return False
  except:
    return False

  return query[0]

def add_legislators_db(dddb, leg_list):
  global P_INSERT
  global T_INSERT
  global L_INSERT

  for leg in leg_list:
    pid = is_leg_in_db(dddb, leg)
    leg['pid'] = pid

    if not pid:
      try:
        dddb.execute(QI_PERSON, leg)
        pid = dddb.lastrowid
        leg['pid'] = pid
        P_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Person', (QI_PERSON%leg)))

      try:
        dddb.execute(QI_LEGISLATOR, leg)
        L_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Legislator', (QI_LEGISLATOR%leg)))

    if is_term_in_db(dddb, leg) == False:
      try:
        dddb.execute(QI_TERM, leg)
        T_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Term', (QI_TERM%leg)))


def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    #Insert Counter for Person and Term
    pi_count = ti_count = 0
    for house in ['upper', 'lower']:
      add_legislators_db(dddb, get_legislators_api(dddb, house))

    logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(P_INSERT) + ' rows in Person, inserted ' +
                   str(L_INSERT) + ' rows in Legislator and inserted '
                    + str(T_INSERT) + ' and updated ' + str(T_UPDATE) + ' rows in Term',
          additional_fields={'_affected_rows':'Person:'+str(P_INSERT)+
                                         ', Legislator:'+str(L_INSERT)+
                                         ', Term:'+str(T_INSERT+T_UPDATE),
                             '_inserted':'Person:'+str(P_INSERT)+
                                         ', Legislator:'+str(L_INSERT)+
                                         ', Term:'+str(T_INSERT),
                             '_updated':'Term:'+str(T_UPDATE),
                             '_state':'FL'})

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()