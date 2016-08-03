#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: import_lobbyists_ny.py
Author: John Alkire
Maintained: Miguel Aguilar
Date: 5/20/2016
Last Modified: 7/26/2016

Description:
  - Imports NY lobbyist data using NY API
  - Note that there is not filer ID in the NY data, which means LobbyingFirmState, LobbyistEmployment, LobbyistEmployer
    cannot be filled. We need to decide on a method to either create filer IDs or alter the schema. 
    - We decided on: filer_id and sender_id being the name of the NY lobbyist (either person name or firm name)

Fills:
  
  - LobbyistEmployment
    - (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistDirectEmployment
    - (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyingContracts
    - (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)

  - Person
    - (first, last)
    - Get pid
  - Lobbyist
    - (pid, filer_id, state)
  - LobbyingFirm
    - (filer_naml)
  - LobbyingFirmState
    - (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state)
  - LobbyistEmployer
    - (filer_id, oid, state)
  - Organizations
    - (name, city, stateHeadquartered, type)
    - Get oid

Source:
  - data.ny.gov API
    - https://data.ny.gov/Transparency/Registered-Lobbyist-Disclosures-Beginning-2007/djsm-9cw7
'''

import re
import sys
import traceback
import requests
import MySQLdb
from graylogger.graylogger import GrayLogger
API_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

QI_PERSON = '''INSERT INTO Person
                (last, first)
                VALUES
                (%(last)s, %(first)s)'''

QI_LOBBYIST = '''INSERT INTO Lobbyist
                (pid, state)
                VALUES
                (%(pid)s, %(state)s)'''
                
QI_LOBBYINGFIRMSTATE = '''INSERT INTO LobbyingFirmState
                        (filer_id, filer_naml, state)
                        VALUES
                        (%(filer_id)s, %(filer_naml)s, %(state)s)'''

QI_LOBBYINGFIRM = '''INSERT INTO LobbyingFirm
                    (filer_naml)
                    VALUES
                    (%(filer_naml)s)'''   

QI_ORGANIZATIONS = '''INSERT INTO Organizations
                          (name, city, stateHeadquartered)
                          VALUES
                          (%s, %s, %s)''' 
QI_LOBBYISTEMPLOYER = '''INSERT INTO LobbyistEmployer
                         (oid, filer_id, state)
                         VALUES
                         (%s, %s, %s)'''

# SELECT

QS_PERSON = '''SELECT pid
                FROM Person
                WHERE first = %(first)s
                AND last = %(last)s'''

QS_LOBBYIST = '''SELECT p.pid 
                     FROM Person p, Lobbyist l
                     WHERE p.first = %(first)s AND p.last = %(last)s
                      AND p.pid = l.pid'''

QS_LOBBYINGFIRM = '''SELECT filer_naml
                         FROM LobbyingFirm
                         WHERE filer_naml = %(filer_naml)s'''

QS_ORGANIZATIONS = '''SELECT oid
                          FROM Organizations
                          WHERE name like %s
                          AND city = %s
                          AND stateHeadquartered = %s'''

QS_ORGANIZATIONS_MAX_OID = '''SELECT oid
                              FROM Organizations
                              ORDER BY oid DESC
                              LIMIT 1'''

QS_LOBBYISTEMPLOYER = '''SELECT filer_id
                          FROM LobbyistEmployer
                          WHERE oid = %s
                          AND filer_id = %s
                          AND state = %s'''
                                               
name_checks = ['(', '\\' ,'/', 'OFFICE', 'LLC', 'INC', 'PLLC', 'LP', 'PC', 'CO', 'LTD']
reporting_period = {'JF':0, 'MA':1, 'MJ':2, 'JA':3, 'SO':4, 'ND':5}

def clean_name(name):
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV', 'SR':', Sr.', 'JR':', Jr.'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name_arr = name.split()      
    suffix = "";
    
    for word in name_arr:
        if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
            name_arr.remove(word)
            if word in ending.keys():
                suffix = ending[word]

    first = name_arr.pop(0)
    
    while len(name_arr) > 1:
        first = first + ' ' + name_arr.pop(0)
                    
    last = name_arr[0] + suffix
    return (first, last)
        
def get_names(names):
    names = names.replace(', III', ' III')
    names = names.replace(', MD', '')
    names = names.replace(', DR.', '')
    names = names.replace(", JR", " JR")
    names = names.replace(", SR", " SR")
    names = names.replace("NULL", "")
    names = names.replace(', ESQ.', '')
    ret_names = list()
    for per in names.split(','):
        if len(per) > 4:            
            ret_names.append(per)
    return ret_names

def call_lobbyist_api():
    url = 'https://data.ny.gov/resource/mbmr-kxth.json?$limit=300000'
    r = requests.get(url)
    lobbyists_api = r.json()

    return lobbyists_api
    

def insert_lobbyistEmployer_db(dddb, lobby):
  dddb.execute(QS_LOBBYISTEMPLOYER, (lobby['client_oid'], lobby['filer_id'], lobby['state']))
  query = dddb.fetchone()
  
  try:
    if query is None:
      dddb.execute(QI_LOBBYISTEMPLOYER, (lobby['client_oid'], lobby['filer_id'], lobby['state']))
  except MySQLdb.Error:
    print traceback.format_exc()
    #print QS_LOBBYISTEMPLOYER%(lobby['client_oid'], lobby['filer_id'], lobby['state'])
    #print QI_LOBBYISTEMPLOYER%(lobby['client_oid'], lobby['filer_id'], lobby['state'])


def insert_organization_db(dddb, lobby):
  client_name = ' '.join([n.lower().capitalize() if n not in name_checks \
                                        else n for n in lobby['client_name'].split()])
  dddb.execute(QS_ORGANIZATIONS, (client_name,lobby['client_city'],lobby['client_state']))
  query = dddb.fetchone()

  if query is None:              
    dddb.execute(QI_ORGANIZATIONS, (client_name,lobby['client_city'],lobby['client_state']))
    dddb.execute(QS_ORGANIZATIONS_MAX_OID)
    query = dddb.fetchone()

  return query[0]
  

def is_person(lobbyist_name):
  if ',' in lobbyist_name:
    for inc in name_checks:
      if inc in lobbyist_name:
        return False
  else:
      return False

  return True


## CHANGE TO DICT TO DICT
# WHERE LEVEL 1 => Key: Name of Lobbyist  Value: dict with info
#       LEVEL 2 => Key: Client Name and Reporting Year tuple   Value: dict with info   
def get_lobbyists_api(dddb, lobbyists_api):
  # Key: Name of Lobbyist      Value: dict with info
  lobbyists = dict()

  for entry in lobbyists_api:
    if 'lobbyist_name' in entry:

      if entry['lobbyist_name'] in lobbyists:
        if (entry['client_name'], entry['reporting_year']) not in lobbyists[entry['lobbyist_name']]:
          client = lobbyists[entry['lobbyist_name']]
          client[(entry['client_name'], entry['reporting_year'])] = get_lobby_info(dddb, entry)
        else:
          pass
          #ASK CHRISTINE ABOUT THE REPORTING YEAR FOR CONTINUOUS YEARS

#          if reporting_period[entry['reporting_period']] > lobby['rpt_period'] \
#            and entry['client_name'] == lobby['client_name']:
#            lobby['rpt_period'] = reporting_period[entry['reporting_period']]
#            if lobby['rpt_period'] == 5:
#              lobby['rpt_year'] += 1

#         if 'additional_lobbyists_lr' in entry and entry['additional_lobbyists_lr'] != 'NULL':
#            lobby['lobbyists'] += get_names(entry['additional_lobbyists_lr'])
#          if 'additional_lobbyists_lbr' in entry and entry['additional_lobbyists_lbr'] != 'NULL':
#            lobby['lobbyists'] += get_names(entry['additional_lobbyists_lbr'])
#          lobby['lobbyists'] = list(set(lobby['lobbyists']))
      else:
        #entry['lobbyist_name'],  entry['additional_lobbyists_lr'],  entry['additional_lobbyists_lbr']
        #entry['client_name'],  entry['lr_responsible_party_first_name'],  entry['lr_responsible_party_last_name']
        #entry['client_state'], entry['client_city'],  entry['client_bussiness_nature'],  entry['reporting_year']
        #entry['reporting_period'],

        clients = dict()
        
        clients[(entry['client_name'], entry['reporting_year'])] = get_lobby_info(dddb, entry)
        lobbyists[entry['lobbyist_name']] = clients


  return lobbyists

def get_lobby_info(dddb, entry):
  lobby = dict()

  cleaned_name = re.sub(r'\(.*', '', entry['lobbyist_name'])
  lobby['person'] = is_person(cleaned_name)

  if lobby['person'] and entry['lr_responsible_party_first_name'] in entry['lobbyist_name'] \
    and entry['lr_responsible_party_last_name'] in entry['lobbyist_name']:
    lobby['first'] = entry['lr_responsible_party_first_name']
    lobby['last'] = entry['lr_responsible_party_last_name']

  # CLEAN NAME for ID (a bit more)
  # Some names are about VARCHAR(100) length
  # ALSO Double check with Andrew to change all lobbying tables 
  # to VARCHAR(100) for filer_id
  lobby['filer_id'] = cleaned_name
  lobby['filer_naml'] = ' '.join([n.lower().capitalize() if n not in name_checks \
                                  else n for n in cleaned_name.split()])
  lobby['state'] = entry['lobbyist_state']

  lobby['rpt_period'] = reporting_period[entry['reporting_period']]
  lobby['rpt_year'] = int(entry['reporting_year'])
  if lobby['rpt_period'] == 5:
    lobby['rpt_year'] += 1

  lobby['client_name'] = entry['client_name']
  lobby['client_state'] = entry['client_state']
  lobby['client_city'] = entry['client_city']
  lobby['client_type'] = entry['client_business_nature']

  lobby['client_oid'] = insert_organization_db(dddb, lobby)
  insert_lobbyistEmployer_db(dddb, lobby)

  lobby['lobbyists'] = list()
  if 'additional_lobbyists_lr' in entry and entry['additional_lobbyists_lr'] != 'NULL':
    lobby['lobbyists'] += get_names(entry['additional_lobbyists_lr'])
  if 'additional_lobbyists_lbr' in entry and entry['additional_lobbyists_lbr'] != 'NULL':
    lobby['lobbyists'] += get_names(entry['additional_lobbyists_lbr'])
  lobby['lobbyists'] = list(set(lobby['lobbyists']))

  return lobby

       
def insert_lobbyist_db(dddb, lobbyist):
  dddb.execute(QS_LOBBYIST, lobbyist)
       
  if dddb.rowcount == 0:
    dddb.execute(QS_PERSON, lobbyist)
    pid = dddb.fetchone()[0]
    if dddb.rowcount == 0:
      dddb.execute(QI_PERSON, lobbyist)
      pid = dddb.lastrowid   
    lobbyist['pid'] = pid

    dddb.execute(QI_LOBBYIST, lobbyist)  

def insert_lobbying_contracts_db(dddb, lobbyist):
  pass

def insert_lobbyingfirm_db(dddb, lobbyist):
  dddb.execute(QS_LOBBYINGFIRM, lobbyist)
  if dddb.rowcount == 0:
    dddb.execute(QI_LOBBYINGFIRM, lobbyist)

    dddb.execute(QS_LOBBYINGFIRMSTATE, lobbyist)
    if dddb.rowcount == 0:
      dddb.execute(QI_LOBBYINGFIRMSTATE, lobbyists)

  for person in lobbyist['lobbyists']:
    per = dict()
    try:
      name = clean_name(person)
      #NOT SURE IF IT'S ALWAYS GONNA BE NY; NO DATA FOR IT
      per['state'] = 'NY'
      per['first'] = name[0]
      per['last'] = name[1]
      insert_lobbyist_db(per)
    except:
      print per

  if lobbyist['client_name'] != lobbyist['lobbyist_name']:
    insert_lobbying_contracts_db(dddb, lobbyist)
    
def insert_lobbyists_db(dddb, lobbyists):
  for lobby_key, lobby_val in lobbyists.iteritems():
    for client_year_key, client_year_val in lobby_val.iteritems():
        if client_year_val['person']:            
            insert_lobbyist_db(dddb, client_year_val)
        else:
            insert_lobbyingfirm_db(dddb, client_year_val)
            
def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
      user='awsDB',
      db='MikeyTest',
      port=3306,
      passwd='digitaldemocracy789',
      charset='utf8') as dddb:
    lobbyists_api = call_lobbyist_api()
    lobbyists = get_lobbyists_api(dddb, lobbyists_api)
    insert_lobbyists_db(dddb, lobbyists)

if __name__ == '__main__':
#  with GrayLogger(API_URL) as _logger:
#    logger = _logger 
    main()
