#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: Cal-Access-Accessor.py
Author: Daniel Mangin
Modified By: Mandy Chan, Freddy Hernandez, Matt Versaggi, Miguel Aguilar, James Ly
Last Modified: 12/21/16

Description:
- Goes through the file CVR_REGISTRATION_CD.TSV and places the data into DDDB
- This script runs under the update script

Sources:
  - db_web_export.zip (California Access)
    - CVR_REGISTRATION_CD.TSV

Populates:
  - LobbyingFirm (filer_naml)
  - LobbyingFirmState (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml)
  - Lobbyist (pid, filer_id)
  - Person (last, first)
  - Organizations (name, type, city, stateHeadquartered)
  - LobbyistEmployer (filer_id, oid, coalition)
  - LobbyistEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
  - LobbyistDirectEmployment (pid, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr)
  - LobbyingContracts (filer_id, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr)
'''
import csv
import json

import refactored_Lobbying_Firm_Name_Fix
import refactored_Person_Name_Fix
from Utils.Database_Connection import *
from Utils.Generic_Utils import *
from clean_name import clean_name

logger = None

# Global counters
LF_INSERT = 0
FS_INSERT = 0
L_INSERT = 0
P_INSERT = 0
O_INSERT = 0
OSA_INSERT = 0 
ER_INSERT = 0
EM_INSERT = 0
DE_INSERT = 0
LC_INSERT = 0

DIRECT_MISS = 0
CONTRACT_MISS = 0

# U.S. State
state = 'CA'

# Querys used to Insert into the Database
QI_LOBBYING_FIRM = '''INSERT INTO LobbyingFirm (filer_naml)
                      VALUES (%s)'''
QI_LOBBYING_FIRM_STATE = '''INSERT INTO LobbyingFirmState (filer_id, rpt_date,
                             ls_beg_yr, ls_end_yr, filer_naml, state)
                            VALUES (%s, %s, %s, %s, %s, %s)'''
QI_LOBBYIST = '''INSERT INTO Lobbyist (pid, filer_id, state)
                 VALUES (%s, %s, %s)'''
QI_PERSON = '''INSERT INTO Person (last, first)
               VALUES (%s, %s)'''
QI_ORGANIZATIONS = '''INSERT INTO Organizations (name, city, stateHeadquartered, source)
                      VALUES (%s, %s, %s, %s)'''
QI_ORG_STATE_AFF = '''INSERT INTO OrganizationStateAffiliation (oid, state)
                      VALUES (%s, %s)'''
QI_LOBBYIST_EMPLOYER = '''INSERT INTO LobbyistEmployer (filer_id, oid,
                           coalition, state)
                          VALUES (%s, %s, %s, %s)'''
QI_LOBBYIST_EMPLOYMENT = '''INSERT INTO LobbyistEmployment (pid, sender_id,
                             rpt_date, ls_beg_yr, ls_end_yr, state)
                            VALUES (%s, %s, %s, %s, %s, %s)'''
QI_LOBBYIST_DIRECT_EMPLOYMENT = '''INSERT INTO LobbyistDirectEmployment (pid,
                                    lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr,
                                    state)
                                   VALUES (%s, %s, %s, %s, %s, %s)'''
QI_LOBBYING_CONTRACTS = '''INSERT INTO LobbyingContracts (filer_id, lobbyist_employer,
                            rpt_date, ls_beg_yr, ls_end_yr, state)
                           VALUES (%s, %s, %s, %s, %s, %s)'''
QS_LOBBYIST = '''SELECT pid, filer_id, state
                 FROM Lobbyist
                 WHERE filer_id = %s
                  AND state = %s 
                 ORDER BY pid'''
QS_PERSON_FL = '''SELECT pid
                  FROM Person
                  WHERE last = %s
                   AND first = %s
                  ORDER BY Person.pid'''
QS_PERSON_MAX_PID = '''SELECT pid
                       FROM Person
                       ORDER BY Person.pid DESC
                       LIMIT 1'''
QS_STATE = '''SELECT abbrev
              FROM State
              WHERE abbrev = %s'''
QS_ORGANIZATIONS = '''SELECT oid
                      FROM Organizations
                      WHERE name = %s
                       AND stateHeadquartered = %s'''
QS_ORGANIZATIONS_OID = '''SELECT oid
                          FROM Organizations
                          WHERE name like %s'''
QS_ORGANIZATIONS_MAX = '''SELECT oid
                          FROM Organizations
                          ORDER BY oid DESC
                          LIMIT 1'''
QS_ORG_STATE_AFF = '''SELECT oid
                      FROM OrganizationStateAffiliation
                      WHERE oid = %s'''
QS_LOBBYIST_EMPLOYER = '''SELECT oid
                          FROM LobbyistEmployer
                          WHERE oid = %s
                          AND state = %s'''
QS_LOBBYIST_EMPLOYER_2 = '''SELECT oid
                          FROM LobbyistEmployer
                          WHERE filer_id = %s
                          AND state = %s'''
QS_LOBBYING_FIRM = '''SELECT filer_naml
                      FROM LobbyingFirm
                      WHERE filer_naml = %s'''
QS_LOBBYING_FIRM_STATE = '''SELECT filer_id, filer_naml
                            FROM LobbyingFirmState
                            WHERE filer_id = %s
                             AND state = %s'''
QS_LOBBYIST_PID = '''SELECT pid
                     FROM Lobbyist
                     WHERE pid = %s
                      AND state = %s'''
QS_LOBBYIST_EMPLOYMENT = '''SELECT pid, sender_id, rpt_date, ls_end_yr
                            FROM LobbyistEmployment
                            WHERE pid = %s
                             AND sender_id = %s
                             AND rpt_date = %s
			                       AND ls_beg_yr = %s
                             AND ls_end_yr = %s
                             AND state = %s'''
QS_LOBBYIST_DIRECT_EMPLOYMENT = '''SELECT lobbyist_employer, rpt_date, ls_beg_yr, state
                                   FROM LobbyistDirectEmployment
                                   WHERE pid = %s
                                    AND lobbyist_employer = %s
				                            AND rpt_date = %s
                                    AND ls_beg_yr = %s
                                    AND ls_end_yr = %s
                                    AND state = %s'''
QS_LOBBYING_CONTRACTS = '''SELECT filer_id, lobbyist_employer, rpt_date, state
                           FROM LobbyingContracts
                           WHERE filer_id = %s
                            AND lobbyist_employer = %s 
                            AND rpt_date = %s 
                            AND state = %s'''
# Currently a static table that is sized to 10,000
Lobbyist= [[0 for x in xrange(7)] for x in xrange(10000)]
Lobbyist_2= [[0 for x in xrange(6)] for x in xrange(50000)]
Lobbyist_3= [[0 for x in xrange(9)] for x in xrange(50000)]


# Changes the date into a linux format for the database
def format_date(date_str):
  date = ''
  date_str = date_str.split('/');
  for x in range(0,3):
    if len(date_str[x]) == 1:
      date_str[x] = '0' + date_str[x]
  date = '-'.join([date_str[2], date_str[0], date_str[1]])
  return date
  
'''
Finds the corresponding pid, filer_id, and U.S. state for the given Person 
name.

|pid|: Person id
|filer_id|: Filer identification number
|filer_naml|: Last name of Person
|filer_namf|: First name of Person
|val|: Default value type

Returns (pid, filer_id, state) if Person is found. Otherwise, return None.
'''
def get_person(dd_cursor, filer_id, filer_naml, filer_namf, val):
  global P_INSERT
  dd_cursor.execute(QS_LOBBYIST, (filer_id, state))
  if dd_cursor.rowcount == 1:
    return dd_cursor.fetchone()[0]
  pid = val

  split_index = len(filer_namf.split(' '))
  name = '%s %s' % (filer_namf, filer_naml)
  cleaned_name = clean_name(name,
                            refactored_Person_Name_Fix.name_clean).split(' ')
  filer_namf = ' '.join(cleaned_name[:split_index])
  filer_naml = ' '.join(cleaned_name[split_index:])
  
  dd_cursor.execute(QS_PERSON_FL, (filer_naml, filer_namf))
  if dd_cursor.rowcount >= 1:
    pid = dd_cursor.fetchone()[0]
  else:
    try:
      dd_cursor.execute(QI_PERSON, (filer_naml, filer_namf))
      P_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:                                              
      logger.exception(format_logger_message('Insert Failed Person',
            (QI_PERSON % (filer_naml, filer_namf))))
    dd_cursor.execute(QS_PERSON_MAX_PID)
    pid = dd_cursor.fetchone()[0]
  return pid

'''
Given an organization's name, city, and state, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Organization's name
|bus_city|: Organization's city
|bus_state|: Organization's state
'''
def insert_organization(dd_cursor, filer_naml, bus_city, bus_state):
  global O_INSERT
  filer_naml = clean_name(filer_naml, refactored_Lobbying_Firm_Name_Fix.clean)
  filer_naml = re.sub(r'[^a-zA-Z0-9 ]', '', filer_naml)
  #In case the city has the state too like => Sacramento, Ca
  if len(bus_city.split(',')) == 2:
    bus_city = bus_city.split(',')[0]
  #In case the state is not a real state or another country other than US and Canada
  dd_cursor.execute(QS_STATE, (bus_state,))
  if dd_cursor.rowcount == 0:
    #print bus_state
    bus_state = 'UN'

  dd_cursor.execute(QS_ORGANIZATIONS, (filer_naml, bus_state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_ORGANIZATIONS, (filer_naml, bus_city, bus_state, 'refactored-Cal-Access-Accessor.py'))
      dd_cursor.execute(QS_ORGANIZATIONS_MAX)
      oid = dd_cursor.fetchone()[0]
      O_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.exception(format_logger_message('Insert Failed for Organizations',
            (QI_ORGANIZATIONS % (filer_naml, bus_city, state))))
  else:
    oid = dd_cursor.fetchone()[0]

  insert_org_state_aff(dd_cursor, oid, bus_state)

  return oid

'''
Given an organization id and state, check if oid is in DDDB, if not, try to add it
|dd_cursor|: DDDB_cursor
|oid|: organization id
|bus_state|: state that the organization is affiliated with
'''

def insert_org_state_aff(dd_cursor, oid, bus_state):
  global OSA_INSERT

  dd_cursor.execute(QS_ORG_STATE_AFF, (oid,))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_ORG_STATE_AFF, (oid, bus_state))
      OSA_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.exception(format_logger_message('Insert Failed for OrganizationStateAffliation',
            (QI_ORG_STATE_AFF % (oid, bus_state))))



'''
Given a person's information, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Person's name
|filer_id|: Person's identification number
|coalition|: Person's coalition
'''
def insert_lobbyist_employer(dd_cursor, filer_naml, filer_id, oid, bus_city, bus_state, coalition):
  global ER_INSERT

  dd_cursor.execute(QS_LOBBYIST_EMPLOYER_2, (filer_id, state))
  if dd_cursor.rowcount == 1:
    oid = dd_cursor.fetchone()[0]
  elif dd_cursor.rowcount > 1:
    #print 'ERROR: MORE THAN ONE FILER_ID  ==>  %s'%(filer_id)
    oid = dd_cursor.fetchone()[0]

  dd_cursor.execute(QS_LOBBYIST_EMPLOYER, (oid, state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYIST_EMPLOYER, (filer_id, oid, coalition, state))
      ER_INSERT += dd_cursor.rowcount
      #print 'GOOD!!  OID: %s   FILER_ID: %s'%(oid, filer_id)
    except MySQLdb.Error:
      #print 'BAD...  OID: %s   FILER_ID: %s'%(oid, filer_id)
      logger.exception(format_logger_message('Insert Failed for LobbyiestEmployer',
            (QI_LOBBYIST_EMPLOYER % (filer_id, oid, coalition, state))))

'''
Given a lobbying firm's name, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Lobbying firm's name

'''
def insert_lobbying_firm(dd_cursor, filer_naml):
  global LF_INSERT
  filer_naml = clean_name(filer_naml, refactored_Lobbying_Firm_Name_Fix.clean)
  filer_naml = re.sub(r'[^a-zA-Z0-9 ]', '', filer_naml)
  dd_cursor.execute(QS_LOBBYING_FIRM, (filer_naml,))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYING_FIRM, (filer_naml,))
      LF_INSERT += dd_cursor.rowcount
      print LF_INSERT, filer_naml
    except MySQLdb.Error:                                              
      logger.exception(format_logger_message('Insert Failed for LobbyingFirm',
            (QI_LOBBYING_FIRM % (filer_naml,))))

'''
Given a lobbying firm's information and state, check if it's in DDDB. If not, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Lobbying Firm's name
|rpt_date|: Report date
|ls_beg_yr|: Lease begin year
|ls_end_yr|: Lease end year
'''
def insert_lobbying_firm_state(dd_cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr):
  global FS_INSERT
  dd_cursor.execute(QS_LOBBYING_FIRM_STATE, (filer_id, state))
  if dd_cursor.rowcount == 0:
    filer_naml = clean_name(filer_naml, refactored_Lobbying_Firm_Name_Fix.clean)
    filer_naml = re.sub(r'[^a-zA-Z0-9 ]', '', filer_naml)
    try:
      dd_cursor.execute(QI_LOBBYING_FIRM_STATE, (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state))
      FS_INSERT += dd_cursor.rowcount
    except MySQLdb.Error as error:                                              
      logger.exception(format_logger_message('Insert Failed for LobbyingFirmState',
          (QI_LOBBYING_FIRM_STATE % (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state))))

'''
Given a lobbyist information, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|pid|: Person id
|filer_id|: Lobbyist identification number
'''
def insert_lobbyist(dd_cursor, pid, filer_id):
  global L_INSERT
  dd_cursor.execute(QS_LOBBYIST_PID, (pid, state))
  if dd_cursor.rowcount > 0:
    return
  dd_cursor.execute(QS_LOBBYIST, (filer_id, state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYIST, (pid, filer_id, state))
      L_INSERT += dd_cursor.rowcount
    except MySQLdb.Error as error:                                              
      logger.exception(format_logger_message('Insert Failed for Lobbyist',
            (QI_LOBBYIST % (pid, filer_id, state))))

'''
Given a lobbyist and their lobbying firm informations, check if it's in DDDB.
Otherwise, add.

|dd_cursor|: DDDB database cursor
|pid|: Person id
|sender_id|: Employer id
|rpt_date|: Report date
|ls_beg_yr|: Lease begin year
|ls_end_yr|: Lease end year
'''
def insert_lobbyist_employment(dd_cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
  global EM_INSERT
  dd_cursor.execute(QS_LOBBYIST_EMPLOYMENT, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYIST_EMPLOYMENT, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
      EM_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.exception(format_logger_message('Insert Failed for LobbyistEmployment',
            (QI_LOBBYIST_EMPLOYMENT % (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))))
    
'''
Given a lobbyist and their direct employer informations, check if it's in DDDB.
Otherwise, add.

|dd_cursor|: DDDB database cursor
|pid|: Person id
|sender_id|: Employer id
|rpt_date|: Report date
|ls_beg_yr|: Lease begin year
|ls_end_yr|: Lease end year
'''
def insert_lobbyist_direct_employment(dd_cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
  global DE_INSERT
  global DIRECT_MISS

  dd_cursor.execute(QS_LOBBYIST_EMPLOYER_2, (sender_id, state))
  if dd_cursor.rowcount == 0:
    #print 'DIRECT_EMPLOYMENT  ==>  EMPLOYER NOT FOUND'
    DIRECT_MISS += 1
  else:
    oid = dd_cursor.fetchone()[0]

    dd_cursor.execute(QS_LOBBYIST_DIRECT_EMPLOYMENT, (pid, oid, rpt_date, ls_beg_yr, ls_end_yr, state))
    if dd_cursor.rowcount == 0:
      try:
        dd_cursor.execute(QI_LOBBYIST_DIRECT_EMPLOYMENT, (pid, oid, rpt_date, ls_beg_yr, ls_end_yr, state))
        DE_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for LobbyistDirectEmployment',
              (QI_LOBBYIST_DIRECT_EMPLOYMENT % (pid, oid, rpt_date, ls_beg_yr, ls_end_yr, state))))

'''
Given a lobbyist and their employer informations, check if it's in DDDB.
Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_id|: Lobbyist id
|sender_id|: Employer id
|rpt_date|: Report date
|ls_beg_yr|: Lease begin year
|ls_end_yr|: Lease end year
'''
def insert_lobbyist_contracts(dd_cursor, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
  global LC_INSERT
  global CONTRACT_MISS

  dd_cursor.execute(QS_LOBBYIST_EMPLOYER_2, (sender_id, state))
  if dd_cursor.rowcount == 0:
    #print 'CONTRACTS  ==>  EMPLOYER NOT FOUND'
    CONTRACT_MISS += 1
  else:
    oid = dd_cursor.fetchone()[0]

    dd_cursor.execute(QS_LOBBYING_CONTRACTS,
      (filer_id, oid, rpt_date, state))
    if dd_cursor.rowcount == 0:
      try:
        dd_cursor.execute(QI_LOBBYING_CONTRACTS,
            (filer_id, oid, rpt_date, ls_beg_yr, ls_end_yr, state))
        LC_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for LobbingContracts',
              (QI_LOBBYING_CONTRACTS % (filer_id, oid, rpt_date, ls_beg_yr, ls_end_yr, state))))
  
# For case 4
# Goes through the lobbyist list and determines if they work for
# Lobbyist Employment or LobbyistDirectEmployment  
def find_lobbyist_employment(dd_cursor, index):
  global EM_INSERT, DE_INSERT
  global DIRECT_MISS

  dd_cursor.execute(QS_LOBBYING_FIRM, (Lobbyist[index][6],))
  if dd_cursor.rowcount > 0:
    dd_cursor.execute(QS_LOBBYIST_EMPLOYMENT,
      (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2],
      Lobbyist[index][3], Lobbyist[index][4], state))
    if dd_cursor.rowcount == 0 and is_in_lobbyingFirmState(dd_cursor, Lobbyist[index][1]):
      try:
        dd_cursor.execute(QI_LOBBYIST_EMPLOYMENT, (Lobbyist[index][0], 
          Lobbyist[index][1], Lobbyist[index][2],
          Lobbyist[index][3], Lobbyist[index][4], state))
        EM_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for LobbyistEmployment',
              (QI_LOBBYIST_EMPLOYMENT % (Lobbyist[index][0], 
              Lobbyist[index][1], Lobbyist[index][2],
              Lobbyist[index][3], Lobbyist[index][4], state))))

  dd_cursor.execute(QS_LOBBYIST_EMPLOYER_2, (Lobbyist[index][1], state))
  if dd_cursor.rowcount == 0:
    #print 'DIRECT_EMPLOYMENT  ==>  EMPLOYER NOT FOUND'
    DIRECT_MISS += 1
  else:
    oid = dd_cursor.fetchone()[0]
    
    dd_cursor.execute(QS_LOBBYIST_DIRECT_EMPLOYMENT,
      (Lobbyist[index][0], oid, Lobbyist[index][2],
      Lobbyist[index][3], Lobbyist[index][4], state))
    if dd_cursor.rowcount == 0:
      try:
        dd_cursor.execute(QI_LOBBYIST_DIRECT_EMPLOYMENT, (Lobbyist[index][0], 
          oid, Lobbyist[index][2], Lobbyist[index][3], Lobbyist[index][4], state))
        DE_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for LobbyistDirectEmployment',
              (QI_LOBBYIST_DIRECT_EMPLOYMENT % (Lobbyist[index][0], 
                oid, Lobbyist[index][2], Lobbyist[index][3], Lobbyist[index][4], state))))
   
def is_in_lobbyingFirmState(dd_cursor, filer_id):
  dd_cursor.execute(QS_LOBBYING_FIRM_STATE, (filer_id, state))
  if dd_cursor.rowcount == 0:
    return False
  else:
    return True

def main():
  with connect() as dd_cursor:
    # Turn off foreign key checks
    #dd_cursor.execute('SET foreign_key_checks = 0')
    with open('/home/data_warehouse_common/dd-Data3.0/CurrentScripts/CA/CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
      #dd_cursor = mysql_connection() 
      tsvin_reader = csv.reader(tsvin, delimiter='\t')
      
      val = 0
      index = 0
      index_2 = index_3 = 0

      for row in tsvin_reader:
        form = row[3]
        sender_id = row[4]
        entity_cd = row[6]
        val = val + 1

        #case 1 - Lobbying Firm
        if form == 'F601' and entity_cd == 'FRM' and (sender_id[:1] == 'F' or sender_id[:1].isdigit()) and sender_id == row[5]: 
          filer_naml = row[7]
          filer_id = row[5]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]

          insert_lobbying_firm(dd_cursor, filer_naml)
          insert_lobbying_firm_state(dd_cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
        #case 2 - Lobbyist and their employer
        elif form == 'F604' and entity_cd == 'LBY' and sender_id[:1] == 'F':
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          org_name = row[61]
          pid = get_person(dd_cursor, filer_id, filer_naml, filer_namf, val)

          insert_lobbyist(dd_cursor, pid, filer_id)
          if is_in_lobbyingFirmState(dd_cursor, sender_id):
            insert_lobbyist_employment(dd_cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
        #case 3 - lobbyist and their direct employment under a lobbying firm
        elif form == 'F604' and entity_cd == 'LBY' and sender_id[:1] == 'E':
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          org_name = row[61]
          pid = get_person(dd_cursor, filer_id, filer_naml, filer_namf, val)

          Lobbyist_2[index_2][0] = pid
          Lobbyist_2[index_2][1] = filer_id
          Lobbyist_2[index_2][2] = sender_id
          Lobbyist_2[index_2][3] = rpt_date
          Lobbyist_2[index_2][4] = ls_beg_yr
          Lobbyist_2[index_2][5] = ls_end_yr
          index_2 += 1
        #case 4 - found a lobbyist, but must determine later if they are under a firm or an employer
        elif form == 'F604' and entity_cd == 'LBY' and sender_id.isdigit():
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          firm_name = row[61]

          pid = get_person(dd_cursor, filer_id, filer_naml, filer_namf, val)
          insert_lobbyist(dd_cursor, pid, filer_id)

          # insert the lobbyist into the array for later
          Lobbyist[index][0] = pid
          Lobbyist[index][1] = sender_id
          Lobbyist[index][2] = rpt_date
          Lobbyist[index][3] = ls_beg_yr
          Lobbyist[index][4] = ls_end_yr
          Lobbyist[index][5] = firm_name
          Lobbyist[index][6] = filer_naml
          index += 1
        #case 5 - Found an employer who is under a contract
        elif form == 'F602' and entity_cd == 'LEM':
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          bus_city = row[17].lower().title()
          bus_state = row[18]
          coalition = (filer_id[:1] == 'C') * 1

          Lobbyist_3[index_3][0] = filer_naml
          Lobbyist_3[index_3][1] = bus_city
          Lobbyist_3[index_3][2] = bus_state
          Lobbyist_3[index_3][3] = filer_id
          Lobbyist_3[index_3][4] = sender_id
          Lobbyist_3[index_3][5] = coalition
          Lobbyist_3[index_3][6] = rpt_date
          Lobbyist_3[index_3][7] = ls_beg_yr
          Lobbyist_3[index_3][8] = ls_end_yr
          index_3 += 1
        #case 6 - Lobbyist Employer and Organization
        elif form == 'F603' and entity_cd == 'LEM':
          ind_cb = row[39]
          bus_cb = row[40]
          trade_cb = row[41]
          oth_cb = row[42]
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          bus_city = row[17].lower().title()
          bus_state = row[18]
          coalition = (filer_id[:1] == 'C') * 1

          if ind_cb == 'X':
            oid = insert_organization(dd_cursor, filer_naml + filer_namf, bus_city, bus_state)
            insert_lobbyist_employer(dd_cursor, filer_naml + filer_namf, filer_id, oid, bus_city, bus_state, coalition)
          else:
            oid = insert_organization(dd_cursor, filer_naml, bus_city, bus_state)
            insert_lobbyist_employer(dd_cursor, filer_naml, filer_id, oid, bus_city, bus_state, coalition)
        #case 7 - IGNORE
        elif form == 'F606':
          pass
          #print 'case 7'
        #case 8 - IGNORE
        elif form == 'F607' and entity_cd == 'LEM':
          pass
          #print 'case 8'
        #just to catch those that dont fit at all
        else:
          pass
          #print 'Does not match any case!'
          
      # Goes through the Lobbyist table and finds employment
      # Continuation of case 4
      while index:
        index -= 1
        find_lobbyist_employment(dd_cursor, index)

      # Continuation of case 3
      while index_2:
        index_2 -= 1
        insert_lobbyist(dd_cursor, Lobbyist_2[index_2][0], Lobbyist_2[index_2][1])
        insert_lobbyist_direct_employment(dd_cursor, Lobbyist_2[index_2][0], Lobbyist_2[index_2][2], 
                                          Lobbyist_2[index_2][3], Lobbyist_2[index_2][4], Lobbyist_2[index_2][5])

      # Continuation of case 5
      while index_3:
        index_3 -= 1
        oid = insert_organization(dd_cursor, Lobbyist_3[index_3][0], Lobbyist_3[index_3][1], Lobbyist_3[index_3][2])
        insert_lobbyist_employer(dd_cursor, Lobbyist_3[index_3][0], Lobbyist_3[index_3][3], oid, Lobbyist_3[index_3][1], 
                                Lobbyist_3[index_3][2], Lobbyist_3[index_3][5])
        #Check if the sender_id is in LobbyingFirmState
        if is_in_lobbyingFirmState(dd_cursor, Lobbyist_3[index_3][4]):
          insert_lobbyist_contracts(dd_cursor, Lobbyist_3[index_3][4], Lobbyist_3[index_3][3], Lobbyist_3[index_3][6], 
                                    Lobbyist_3[index_3][7], Lobbyist_3[index_3][8])

    #print 'TOTAL MISSES:   DIRECT => %d    CONTRACT => %d'%(DIRECT_MISS, CONTRACT_MISS)

  LOG = {'tables': [{'state': 'CA', 'name': 'LobbingFirm', 'inserted':LF_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'LobbyingFirmState', 'inserted':FS_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'Lobbyist', 'inserted':L_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'Person', 'inserted':P_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'Organizations', 'inserted':O_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'LobbyistEmployer', 'inserted':ER_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'LobbyistEmployment', 'inserted':EM_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'LobbyistDirectEmployment', 'inserted':DE_INSERT, 'updated': 0, 'deleted': 0},
          {'state': 'CA', 'name': 'LobbyingContracts', 'inserted':LC_INSERT, 'updated': 0, 'deleted': 0}]}
  sys.stdout.write(json.dumps(LOG))
      
if __name__ == '__main__':
    logger = create_logger()
    main()
