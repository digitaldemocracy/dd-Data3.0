#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: import_lobbyists_ny.py
Author: John Alkire
Maintained: Miguel Aguilar
Date: 5/20/2016
Last Modified: 8/11/2016

Description:
  - Imports NY lobbyist data using NY API
  - Note that there is not filer ID in the NY data, which means LobbyingFirmState, LobbyistEmployment, LobbyistEmployer
    cannot be filled. We need to decide on a method to either create filer IDs or alter the schema. 
    - We decided on: filer_id being the name of the NY lobbyist (either person name or firm name)

Populates:
  - Person
    - (first, last)
  - Lobbyist
    - (pid, filer_id, state)
  - LobbyingFirm
    - (filer_naml)
  - LobbyingFirmState
    - (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state)
  - LobbyistEmployment (X not populated X)
    - (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistDirectEmployment
    - (pid, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyingContracts
    - (filer_id, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistEmployer
    - (filer_id, oid, state)
  - Organizations
    - (name, city, stateHeadquartered, type)

Source:
  - data.ny.gov API
    - https://data.ny.gov/Transparency/Registered-Lobbyist-Disclosures-Beginning-2007/djsm-9cw7
'''

import re
import sys
from datetime import date
import traceback
import requests
import MySQLdb
from graylogger.graylogger import GrayLogger
API_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

# GLOBALS

P_INSERT = 0
L_INSERT = 0
O_INSERT = 0
LFS_INSERT = 0
LF_INSERT = 0
LE_INSERT = 0
LDE_INSERT = 0
LC_INSERT = 0


# INSERTS

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
                        (%s, %s, %s)'''

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

QI_LOBBYINGEMPLOYMENT = '''INSERT INTO LobbyistEmployment
                          (pid, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
                          VALUES
                          (%s, %s, %s, %s, %s, %s)'''

QI_LOBBYISTDIRECTEMPLOYMENT = '''INSERT INTO LobbyistDirectEmployment
                                  (pid, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
                                  VALUES
                                  (%s, %s, %s, %s, %s, %s)'''

QI_LOBBYINGCONTRACTS = '''INSERT INTO LobbyingContracts
                          (filer_id, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
                          VALUES
                          (%s, %s, %s, %s, %s, %s)'''

# SELECTS

QS_PERSON = '''SELECT pid
                FROM Person
                WHERE first = %(first)s
                AND last = %(last)s'''

QS_LOBBYIST = '''SELECT p.pid 
                 FROM Person p, Lobbyist l
                 WHERE p.first = %(first)s AND p.last = %(last)s
                 AND p.pid = l.pid'''

QS_LOBBYIST_2 = '''SELECT pid
                    FROM Lobbyist
                    WHERE filer_id = %s
                    AND state = %s'''

QS_LOBBYINGFIRM = '''SELECT filer_naml
                     FROM LobbyingFirm
                     WHERE filer_naml = %(filer_naml)s'''

QS_LOBBYINGFIRMSTATE = '''SELECT filer_id
                          FROM LobbyingFirmState
                          WHERE filer_naml = %s
                          AND state = %s'''

QS_ORGANIZATIONS = '''SELECT oid
                      FROM Organizations
                      WHERE name like %s'''

QS_ORGANIZATIONS_MAX_OID = '''SELECT oid
                              FROM Organizations
                              ORDER BY oid DESC
                              LIMIT 1'''

QS_LOBBYISTEMPLOYER = '''SELECT oid
                          FROM LobbyistEmployer
                          WHERE oid = %s
                          AND state = %s'''

QS_LOBBYISTEMPLOYMENT = '''SELECT pid
                          FROM LobbyistEmployment
                          WHERE pid = %s
                          AND lobbyist_employer = %s
                          AND ls_beg_yr = %s
                          AND ls_end_yr = %s
                          AND state = %s'''

QS_LOBBYISTDIRECTEMPLOYMENT = '''SELECT pid
                                FROM LobbyistDirectEmployment
                                WHERE pid = %s
                                AND lobbyist_employer = %s
                                AND ls_beg_yr = %s
                                AND ls_end_yr = %s
                                AND state = %s'''

QS_LOBBYINGCONTRACTS = '''SELECT *
                          FROM LobbyingContracts
                          WHERE filer_id = %s
                          AND lobbyist_employer = %s
                          AND ls_beg_yr = %s
                          AND ls_end_yr = %s
                          AND state = %s'''
                                 

name_checks = ['(', '\\' ,'/', 'OFFICE', 'LLC', 'LLP', 'INC', 'PLLC', 'LP', 'PC', 'CO', 'LTD', 
                'ASSOCIATES', 'ASSOCIATION', 'AFFILIATES', 'CORPORATION', '&', 'INTERNATIONAL', 
                'UNION', 'SOCIETY', 'CHAPTER', 'NATIONAL', 'FOUNDATION', 'PUBLIC', 'MANAGEMENT']
name_acronyms = ['LLC', 'LLP', 'INC', 'PLLC', 'LP', 'PC', 'CO', 'LTD', 'II']
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
    
    if len(name_arr) >= 1: 
      last = name_arr[0] + suffix
    else:
      last = ''

    name_split = name.split()
    if len(name_split) >= 2:
      if len(name_split[1]) == 1:
        first = name_split[0]
        last = name_split[1]+'.'

    if len(first) == 1:
      first = first+'.'

    return (first, last)

def cleanup_name(name):
  cleaned_name = re.sub(r'\(.*', '', name)
  #cleaned_name = cleaned_name.replace('.', '')
  fixed_name = ' '.join([n.lower().capitalize() if n not in name_acronyms \
                                  else n for n in cleaned_name.split()])
  if '\\' in fixed_name:
    fixed_name = ' '.join(fixed_name.split('\\'))
  elif '/' in fixed_name:
    fixed_name = ' '.join(fixed_name.split('/'))

  return fixed_name
        
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
        ret_names.append(per.title())
    return ret_names


def call_lobbyist_api():
    url = 'https://data.ny.gov/resource/mbmr-kxth.json?$limit=300000'
    r = requests.get(url)
    lobbyists_api = r.json()

    return lobbyists_api
    

def insert_lobbyistEmployer_db(dddb, lobby):
  global LE_INSERT

  dddb.execute(QS_LOBBYISTEMPLOYER, (lobby['client_oid'], 'NY'))
  query = dddb.fetchone()
  
  if query is None:
    try:
      dddb.execute(QI_LOBBYISTEMPLOYER, (lobby['client_oid'], lobby['client_name'], 'NY'))
      LE_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('LobbyistEmployer', 
                        (QI_LOBBYISTEMPLOYER%(lobby['client_oid'], lobby['client_name'], 'NY'))))


def insert_organization_db(dddb, lobby):
  global O_INSERT

  dddb.execute(QS_ORGANIZATIONS, (lobby['client_name']))
  query = dddb.fetchone()

  if query is None:
    try:
      dddb.execute(QI_ORGANIZATIONS, (lobby['client_name'], lobby['client_city'], lobby['client_state']))
      O_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('Organizations', 
                        (QI_ORGANIZATIONS%(lobby['client_name'], lobby['client_city'], lobby['client_state']))))

    dddb.execute(QS_ORGANIZATIONS_MAX_OID)
    query = dddb.fetchone()

  return query[0]
  

def is_person(lobbyist_name):
  if ',' in lobbyist_name:
    for inc in name_checks:
      if inc.lower() in lobbyist_name.replace('.', '').lower():
        return False
  else:
      return False

  return True


## CHANGE TO DICT TO DICT
# WHERE LEVEL 1 => Key: Name of Lobbyist  Value: dict with info
#       LEVEL 2 => Key: Client Name and Reporting Year tuple   Value: dict with info

##### MAYBE CHANGE LEVEL 2 => KEY: Client Name   Value: dict with info
def get_lobbyists_api(dddb, lobbyists_api):
  lobbyists = dict()

  for entry in lobbyists_api:
    if 'lobbyist_name' in entry and 'client_name' in entry:
      lobbyist_name = cleanup_name(entry['lobbyist_name'])
      client_name = cleanup_name(entry['client_name'])

      if lobbyist_name in lobbyists:

        if client_name not in lobbyists[lobbyist_name]:
          client = lobbyists[lobbyist_name]
          client[client_name] = get_lobby_info(dddb, entry)
        else:
          #ASK CHRISTINE ABOUT THE REPORTING YEAR FOR CONTINUOUS YEARS
          client = lobbyists[lobbyist_name]
          lobby = client[client_name]

          if int(entry['reporting_year']) < lobby['ls_beg_yr'] and client_name == lobby['client_name']:
            lobby['ls_beg_yr'] = int(entry['reporting_year'])
          if int(entry['reporting_year']) > lobby['ls_end_yr'] and client_name == lobby['client_name']:
            lobby['ls_end_yr'] = int(entry['reporting_year'])

          if reporting_period[entry['reporting_period']] > lobby['rpt_period'] \
            and client_name == lobby['client_name']:
            lobby['rpt_period'] = reporting_period[entry['reporting_period']]
            if lobby['rpt_period'] == 5:
              lobby['rpt_year'] += 1

          if 'additional_lobbyists_lr' in entry and entry['additional_lobbyists_lr'] != 'NULL':
            lobby['lobbyists'] += get_names(entry['additional_lobbyists_lr'])
          if 'additional_lobbyists_lbr' in entry and entry['additional_lobbyists_lbr'] != 'NULL':
            lobby['lobbyists'] += get_names(entry['additional_lobbyists_lbr'])
          lobby['lobbyists'] = list(set(lobby['lobbyists']))

      else:
        clients = dict()
        
        clients[client_name] = get_lobby_info(dddb, entry)
        lobbyists[lobbyist_name] = clients

  return lobbyists


def get_lobby_info(dddb, entry):
  lobby = dict()

  lobby['filer_id'] = lobby['filer_naml'] = cleanup_name(entry['lobbyist_name'])
  lobby['person'] = is_person(lobby['filer_naml'])

  lobby['state'] = entry['lobbyist_state']
  lobby['city'] = entry['lobbyist_city']
  #SPECIAL CASE TYPO => SHOULD BE NJ
  if lobby['state'] == 'NU':
    lobby['state'] = 'NJ'

  if lobby['person'] and entry['lr_responsible_party_first_name'] in entry['lobbyist_name'] \
    and entry['lr_responsible_party_last_name'] in entry['lobbyist_name']:
    lobby['first'] = entry['lr_responsible_party_first_name'].title()
    lobby['last'] = entry['lr_responsible_party_last_name'].title()

  name_split = cleanup_name(entry['lobbyist_name']).split(', ')
  if lobby['person'] and len(name_split) == 2:
    lobby['first'] = name_split[1]
    lobby['last'] = name_split[0]
  elif lobby['person'] and 'Jr.' in name_split:
    lobby['first'] = name_split[2]
    lobby['last'] = ' '.join(name_split[:-1])

  lobby['rpt_period'] = reporting_period[entry['reporting_period']]
  lobby['rpt_year'] = int(entry['reporting_year'])
  lobby['ls_beg_yr'] = lobby['ls_end_yr'] = lobby['rpt_year']
  if lobby['rpt_period'] == 5:
    lobby['rpt_year'] += 1

  lobby['client_name'] = cleanup_name(entry['client_name'])
  lobby['client_state'] = entry['client_state']
  if lobby['client_state'] == '0':
    lobby['client_state'] = 'UN'
  #SPECIAL CASE => TYPO SHOULD BE: PA
  elif lobby['client_state'] == 'PE':
    lobby['client_state'] = 'PA'

  lobby['client_city'] = entry['client_city'].title()
  lobby['client_type'] = entry['client_business_nature'].title()

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
  global L_INSERT
  global P_INSERT

  dddb.execute(QS_LOBBYIST, lobbyist)
  if dddb.rowcount == 0:
    dddb.execute(QS_PERSON, lobbyist)
    if dddb.rowcount == 0:
      try:
        dddb.execute(QI_PERSON, lobbyist)
        pid = dddb.lastrowid
        P_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('Person', 
                        (QI_PERSON%lobbyist)))
    else:
      pid = dddb.fetchone()[0]
    lobbyist['pid'] = pid
    try:
      dddb.execute(QI_LOBBYIST, lobbyist)
      L_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('Lobbyist', 
                        (QI_LOBBYIST%lobbyist)))
  else:
    pid = dddb.fetchone()[0]

  return pid

def insert_direct_employment_db(dddb, lobbyist):
  global LDE_INSERT

  for person in lobbyist['lobbyists']:
    per = dict()
    try:
      name = clean_name(person)
      #NOT SURE IF IT'S ALWAYS GONNA BE NY; NO DATA FOR IT
      per['state'] = 'NY'
      per['first'] = name[0]
      per['last'] = name[1]
      #MAKE FILER_ID just the name of the person
      per['filer_id'] = person
      pid = insert_lobbyist_db(dddb, per)

      dddb.execute(QS_LOBBYISTDIRECTEMPLOYMENT, (pid, lobbyist['client_oid'], lobbyist['ls_beg_yr'], lobbyist['ls_end_yr'], 'NY'))
      if dddb.rowcount == 0:
        rpt_period = (lobbyist['rpt_period']+1)*2+1
        if rpt_period == 13:
          rpt_period = 1
        rpt_date = date(lobbyist['rpt_year'], rpt_period, 1)
        dddb.execute(QI_LOBBYISTDIRECTEMPLOYMENT, (pid, lobbyist['client_oid'], rpt_date, lobbyist['ls_beg_yr'], lobbyist['ls_end_yr'], 'NY'))
        LDE_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('LobbyistDirectEmployment', 
                        (QI_LOBBYISTDIRECTEMPLOYMENT%(pid, lobbyist['client_oid'], rpt_date, lobbyist['ls_beg_yr'], lobbyist['ls_end_yr'], 'NY'))))


def insert_additional_lobbyists_db(dddb, lobbyist):
  for person in lobbyist['lobbyists']:
    per = dict()
    try:
      name = clean_name(person)
      #NOT SURE IF IT'S ALWAYS GONNA BE NY; NO DATA FOR IT
      per['state'] = 'NY'
      per['first'] = name[0]
      per['last'] = name[1]
      #MAKE FILER_ID just the name of the person
      per['filer_id'] = person
      insert_lobbyist_db(dddb, per)
    except:
      print traceback.format_exc()


def insert_lobbying_contracts_db(dddb, lobbyist):
  global LC_INSERT

  dddb.execute(QS_ORGANIZATIONS, (lobbyist['client_name']))
  oid = dddb.fetchone()[0]
  dddb.execute(QS_LOBBYISTEMPLOYER, (oid, 'NY'))
  lobbyist_employer = dddb.fetchone()[0]

  dddb.execute(QS_LOBBYINGCONTRACTS, (lobbyist['filer_id'], lobbyist_employer, lobbyist['ls_beg_yr'], lobbyist['ls_end_yr'], 'NY'))

  if dddb.rowcount == 0:
    rpt_period = (lobbyist['rpt_period']+1)*2+1
    if rpt_period == 13:
      rpt_period = 1
    rpt_date = date(lobbyist['rpt_year'], rpt_period, 1)
    try:
      dddb.execute(QI_LOBBYINGCONTRACTS, (lobbyist['filer_id'], lobbyist_employer, rpt_date, lobbyist['ls_beg_yr'], lobbyist['ls_end_yr'], 'NY'))
      LC_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('LobbyingContracts', 
                        (QI_LOBBYINGCONTRACTS%(lobbyist['filer_id'], lobbyist_employer, rpt_date, lobbyist['ls_beg_yr'], lobbyist['ls_end_yr'], 'NY'))))


def insert_lobbyingfirm_db(dddb, lobbyist):
  global LF_INSERT
  global LFS_INSERT

  dddb.execute(QS_LOBBYINGFIRM, lobbyist)
  if dddb.rowcount == 0:
    try:
      dddb.execute(QI_LOBBYINGFIRM, lobbyist)
      LF_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('LobbyingFirm', (QI_LOBBYINGFIRM%lobbyist)))

  dddb.execute(QS_LOBBYINGFIRMSTATE, (lobbyist['filer_naml'], 'NY'))
  if dddb.rowcount == 0:
    try:
      dddb.execute(QI_LOBBYINGFIRMSTATE, (lobbyist['filer_id'], lobbyist['filer_naml'], 'NY'))
      LFS_INSERT += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                      additional_fields=create_payload('LobbyingFirmState', 
                        (QI_LOBBYINGFIRMSTATE%(lobbyist['filer_id'], lobbyist['filer_naml'], 'NY'))))

  insert_additional_lobbyists_db(dddb, lobbyist)

  if lobbyist['client_name'] != lobbyist['filer_naml']:
    insert_lobbying_contracts_db(dddb, lobbyist)
  else:
    insert_direct_employment_db(dddb, lobbyist)
   

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

    logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(P_INSERT) + ' rows in Person, inserted ' 
                    + str(L_INSERT) + ' rows in Lobbyist, inserted '
                    + str(O_INSERT) + ' rows in Organizations, inserted ' 
                    + str(LFS_INSERT) + ' rows in LobbyingFirmState, inserted '
                    + str(LF_INSERT) + ' rows in LobbyingFirm, inserted ' 
                    + str(LE_INSERT) + ' rows in LobbyistEmployer, inserted '
                    + str(LDE_INSERT) + ' rows in LobbyistDirectEmployment, and inserted ' 
                    + str(LC_INSERT) + ' rows in LobbyingContracts',
          additional_fields={'_affected_rows':'Person:'+str(P_INSERT)+
                                         ', Lobbyist:'+str(L_INSERT)+
                                         ', Organizations:'+str(O_INSERT)+
                                         ', LobbyingFirmState:'+str(LFS_INSERT)+
                                         ', LobbyingFirm:'+str(LF_INSERT)+
                                         ', LobbyistEmployer:'+str(LE_INSERT)+
                                         ', LobbyistDirectEmployment:'+str(LDE_INSERT)+
                                         ', LobbyingContracts:'+str(LC_INSERT),
                             '_inserted':'Person:'+str(P_INSERT)+
                                         ', Lobbyist:'+str(L_INSERT)+
                                         ', Organizations:'+str(O_INSERT)+
                                         ', LobbyingFirmState:'+str(LFS_INSERT)+
                                         ', LobbyingFirm:'+str(LF_INSERT)+
                                         ', LobbyistEmployer:'+str(LE_INSERT)+
                                         ', LobbyistDirectEmployment:'+str(LDE_INSERT)+
                                         ', LobbyingContracts:'+str(LC_INSERT),
                             '_state':'NY'})

if __name__ == '__main__':
  with GrayLogger(API_URL) as _logger:
    logger = _logger 
    main()
