#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: fl_import_legislators.py
Author: Miguel Aguilar
Date: 07/05/2016

Description:
  - This script populates the database with the Florida state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
'''

import requests
import MySQLdb
import traceback
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None
API_URL = 'http://openstates.org/api/v1/legislators/?state=fl&chamber={0}&apikey=c12c4c7e02c04976865f3f9e95c3275b'

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

def clean_name(name):
  problem_names = {
    #GET NEW NAMES CAUSE THESE ARE FROM NY NOT FL
    "Inez Barron":("Charles", "Barron"), 
    "Philip Ramos":("Phil", "Ramos"), 
    "Thomas McKevitt":("Tom", "McKevitt"), 
    "Albert Stirpe":("Al","Stirpe"), 
    "Peter Abbate":("Peter","Abbate, Jr."),
    "Sam Roberts":("Pamela","Hunter"),
    "Herman Farrell":("Herman", "Farrell, Jr."),
    "Fred Thiele":("Fred", "Thiele, Jr."),
    "William Scarborough":("Alicia", "Hyndman"),
    "Robert Oaks":("Bob", "Oaks"),
    "Andrew Goodell":("Andy", "Goodell"),
    "Peter Rivera":("José", "Rivera"),
    "Addie Jenne Russell":("Addie","Russell"),
    "Kenneth Blankenbush":("Ken","Blankenbush"),
    "Alec Brook-Krasny":("Pamela","Harris"),
    "Mickey Kearns":("Michael", "Kearns"),
    "Steven Englebright":("Steve", "Englebright"),
    "WILLIAMS":("Jamie", "Williams"),
    "PEOPLES-STOKE":("Crystal", "Peoples-Stoke"),
    "KAMINSKY":("Todd", "Kaminsky"),
    "HYNDMAN":("Alicia", "Hyndman"),
    "HUNTER":("Pamela", "Hunter"),
    "HARRIS":("Pamela", "Harris"),
    "CASTORINA":("Ron", "Castorina", "Jr"),
    "CANCEL":("Alice", "Cancel"),
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

    print 'NOT CLEAN: %s'%entry['full_name']
    clean_leg_name = clean_name(entry['full_name'])
    print 'YES CLEAN: %s'%(' '.join(clean_leg_name))
    leg['first'] = clean_leg_name[0]
    leg['last'] = clean_leg_name[1]
    leg['middle'] = entry['middle_name']

  return leg_list

def is_leg_in_db(dddb, leg):
  dddb.execute(QS_LEGISLATOR, leg)
  query = dddb.fetchone()

  if query is None:
    return False

  return True

def add_legislators_db(dddb, leg_list):
  for leg in leg_list:
    if not is_leg_in_db(dddb, leg):
      pass

def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='MikeyTest',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    for house in ['upper', 'lower']:
      add_legislators_db(dddb, get_legislators_api(dddb, house))

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()