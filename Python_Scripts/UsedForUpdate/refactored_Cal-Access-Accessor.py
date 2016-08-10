#!/usr/bin/env python
'''
File: Cal-Access-Accessor.py
Author: Daniel Mangin
Modified By: Mandy Chan, Freddy Hernandez, Matt Versaggi, Miguel Aguilar
Last Modified: August 10th, 2016

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
import MySQLdb
import traceback
from clean_name import clean_name
import refactored_Lobbying_Firm_Name_Fix
import refactored_Person_Name_Fix
from graylogger.graylogger import GrayLogger                                    
API_URL = 'http://development.digitaldemocracy.org:12202/gelf'                  
logger = None
LF_INSERT = 0
FS_INSERT = 0
L_INSERT = 0
P_INSERT = 0
O_INSERT = 0
ER_INSERT = 0
EM_INSERT = 0
DE_INSERT = 0
LC_INSERT = 0

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
QI_ORGANIZATIONS = '''INSERT INTO Organizations (name, city, stateHeadquartered)
                      VALUES (%s, %s, %s)'''
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
QS_ORGANIZATIONS = '''SELECT oid
                      FROM Organizations
                      WHERE name = %s
                       AND city = %s
                       AND stateHeadquartered = %s'''
QS_ORGANIZATIONS_OID = '''SELECT oid
                          FROM Organizations
                          WHERE name like %s'''
QS_ORGANIZATIONS_MAX = '''SELECT oid
                          FROM Organizations
                          ORDER BY oid DESC
                          LIMIT 1'''
QS_LOBBYIST_EMPLOYER = '''SELECT filer_id
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
Lobbyist = [[0 for x in xrange(7)] for x in xrange(10000)]

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA'
  }

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
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Person', 
            (QI_PERSON % (filer_naml, filer_namf))))
    dd_cursor.execute(QS_PERSON_MAX_PID)
    pid = dd_cursor.fetchone()[0]
  return pid

'''
Given an organization's name, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Organization's name
'''
def get_organization(dd_cursor, filer_naml):
  filer_naml = clean_name(filer_naml, refactored_Lobbying_Firm_Name_Fix.clean)
  dd_cursor.execute(QS_ORGANIZATIONS_OID, (filer_naml))
  if dd_cursor.rowcount == 0:
    oid = insert_organization(dd_cursor, filer_naml, 'NULL', 'UN')
  else:
    oid = dd_cursor.fetchone()[0]

  return oid

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
  dd_cursor.execute(QS_ORGANIZATIONS, (filer_naml, bus_city, bus_state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_ORGANIZATIONS, (filer_naml, bus_city, bus_state))
      dd_cursor.execute(QS_ORGANIZATIONS_MAX)
      oid = dd_cursor.fetchone()[0]
      O_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Organizations', 
            (QI_ORGANIZATIONS % (filer_naml, bus_city, state))))
  else:
    oid = dd_cursor.fetchone()[0]

    return oid

'''
Given a person's information, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Person's name
|filer_id|: Person's identification number
|coalition|: Person's coalition
'''
def insert_lobbyist_employer(dd_cursor, filer_naml, filer_id, bus_city, bus_state, coalition):
  global ER_INSERT
  dd_cursor.execute(QS_LOBBYIST_EMPLOYER, (filer_id, state))
  if dd_cursor.rowcount == 0:
    filer_naml = clean_name(filer_naml, refactored_Lobbying_Firm_Name_Fix.clean)
    dd_cursor.execute(QS_ORGANIZATIONS, (filer_naml, bus_city, bus_state))
    oid = dd_cursor.fetchone()[0]
#    print 'Inserting into LobbyistEmployer: filer_id:{0}, oid:{1}, coalition:{2}, state:{3}\n'.format(filer_id, oid, coalition, state)
    try:
      dd_cursor.execute(QI_LOBBYIST_EMPLOYER, (filer_id, oid, coalition, state))
      ER_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('LobbyiestEmployer',
            (QI_LOBBYIST_EMPLOYER % (filer_id, oid, coalition, state))))

'''
Given a lobbying firm's name, check if it's in DDDB. Otherwise, add.

|dd_cursor|: DDDB database cursor
|filer_naml|: Lobbying firm's name

'''
def insert_lobbying_firm(dd_cursor, filer_naml):
  global LF_INSERT
  filer_naml = clean_name(filer_naml, refactored_Lobbying_Firm_Name_Fix.clean)
  dd_cursor.execute(QS_LOBBYING_FIRM, (filer_naml,))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYING_FIRM, (filer_naml,))
      LF_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:                                              
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('LobbyingFirm', 
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
    try:
      dd_cursor.execute(QI_LOBBYING_FIRM_STATE, (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state))
      FS_INSERT += dd_cursor.rowcount
    except MySQLdb.Error as error:                                              
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('LobbyingFirmState', 
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
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Lobbyist', 
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
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('LobbistEmployment',
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
  dd_cursor.execute(QS_LOBBYIST_DIRECT_EMPLOYMENT, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYIST_DIRECT_EMPLOYMENT, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
      DE_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('LobbyistDirectEmployment',
            (QI_LOBBYIST_DIRECT_EMPLOYMENT % (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))))

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
  dd_cursor.execute(QS_LOBBYING_CONTRACTS,
    (filer_id, sender_id, rpt_date, state))
  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_LOBBYING_CONTRACTS,
          (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
      LC_INSERT += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('LobbingContracts',
            (QI_LOBBYING_CONTRACTS % (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))))
  
# For case 4
# Goes through the lobbyist list and determines if they work for
# Lobbyist Employment or LobbyistDirectEmployment  
def find_lobbyist_employment(dd_cursor, index):
  global EM_INSERT, DE_INSERT
  dd_cursor.execute(QS_LOBBYING_FIRM, (Lobbyist[index][6],))
  if dd_cursor.rowcount > 0:
    dd_cursor.execute(QS_LOBBYIST_EMPLOYMENT,
      (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2],
      Lobbyist[index][3], Lobbyist[index][4], state))
    if dd_cursor.rowcount == 0:
      try:
        dd_cursor.execute(QI_LOBBYIST_EMPLOYMENT, (Lobbyist[index][0], 
          Lobbyist[index][1], Lobbyist[index][2],
          Lobbyist[index][3], Lobbyist[index][4], state))
        EM_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('LobbyistEmployment',
              (QI_LOBBYIST_EMPLOYMENT % (Lobbyist[index][0], 
              Lobbyist[index][1], Lobbyist[index][2],
              Lobbyist[index][3], Lobbyist[index][4], state))))

  dd_cursor.execute(QS_LOBBYIST_EMPLOYER, (Lobbyist[index][5], state))
  if dd_cursor.rowcount > 0:
    
    dd_cursor.execute(QS_LOBBYIST_DIRECT_EMPLOYMENT,
      (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2],
      Lobbyist[index][3], Lobbyist[index][4], state))
    if dd_cursor.rowcount == 0:
      try:
        dd_cursor.execute(QI_LOBBYIST_DIRECT_EMPLOYMENT, (Lobbyist[index][0], 
          Lobbyist[index][1], Lobbyist[index][2], 
          Lobbyist[index][3], Lobbyist[index][4], state))
        DE_INSERT += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('LobbyistDirectEmployment',
              (QI_LOBBYIST_DIRECT_EMPLOYMENT % (Lobbyist[index][0], 
                Lobbyist[index][1], Lobbyist[index][2], Lobbyist[index][3], 
                Lobbyist[index][4], state))))
        
def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789') as dd_cursor:
    # Turn off foreign key checks
    dd_cursor.execute('SET foreign_key_checks = 0')
    with open('/home/data_warehouse_common/scripts/CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
      tsvin_reader = csv.reader(tsvin, delimiter='\t')
      
      val = 0
      index = 0

      for row in tsvin_reader:
        form = row[3]
        sender_id = row[4]
        entity_cd = row[6]
        val = val + 1
#        print val
        #case 1 - Lobbying Firm
        if form == 'F601' and entity_cd == 'FRM' and (sender_id[:1] == 'F' or sender_id[:1].isdigit()) and sender_id == row[5]: 
          filer_naml = row[7]
          filer_id = row[5]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
#          print 'naml = {0}, id = {1}, date = {2}, beg = {3}, end = {4}\n'.format(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
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
          pid = get_person(dd_cursor, filer_id, filer_naml, filer_namf, val)
#          print 'filer_id = {0}\n'.format(filer_id)
#          print 'sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n'.format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
          insert_lobbyist(dd_cursor, pid, filer_id)
          insert_lobbyist_employment(dd_cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
        #case 3 - lobbyist and their direct employment under a lobbying firm
        elif form == 'F604' and entity_cd == 'LBY' and sender_id[:1] == 'E':
          pass
          '''
          Moved below for second iteration because some organizations wouldn't 
          be in the Organizations table until after the first iteration adding 
          all organizations and employers.
          '''
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
#          print 'filer_id = {0}\n'.format(filer_id)
          pid = get_person(dd_cursor, filer_id, filer_naml, filer_namf, val)
          insert_lobbyist(dd_cursor, pid, filer_id)
#          print 'inserting Lobbyist into index {0}\n'.format(index)
          # insert the lobbyist into the array for later
          Lobbyist[index][0] = pid
          Lobbyist[index][1] = sender_id
          Lobbyist[index][2] = rpt_date
          Lobbyist[index][3] = ls_beg_yr
          Lobbyist[index][4] = ls_end_yr
          Lobbyist[index][5] = filer_id
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
          bus_city = row[17]
          bus_state = row[18]
          coalition = (filer_id[:1] == 'C') * 1
#          print 'filer_naml = {0}, filer_id = {1}, coalition = {2}\n'.format(filer_naml, filer_id, coalition)
          oid = insert_organization(dd_cursor, filer_naml, bus_city, bus_state)
          insert_lobbyist_employer(dd_cursor, filer_naml, oid, bus_city, bus_state, coalition)
          insert_lobbyist_contracts(dd_cursor, filer_id, oid, rpt_date, ls_beg_yr, ls_end_yr)
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
          bus_city = row[17]
          bus_state = row[18]
          coalition = (filer_id[:1] == 'C') * 1
#          print 'filer_naml = {0}, filer_id = {1}, coalition = {2}\n'.format(filer_naml, filer_id, coalition)
          if ind_cb == 'X':
            oid = insert_organization(dd_cursor, filer_naml + filer_namf, bus_city, bus_state)
            insert_lobbyist_employer(dd_cursor, filer_naml + filer_namf, oid, bus_city, bus_state, coalition)
          else:
            oid = insert_organization(dd_cursor, filer_naml, bus_city, bus_state)
            insert_lobbyist_employer(dd_cursor, filer_naml, oid, bus_city, bus_state, coalition)
        #case 7 - IGNORE
        elif form == 'F606':
          print 'case 7'
        #case 8 - IGNORE
        elif form == 'F607' and entity_cd == 'LEM':
          print 'case 8'
        #just to catch those that dont fit at all
        else:
          print 'Does not match any case!'
          
      # Goes through the Lobbyist table and finds employment
      # Continuation of case 4
      while index:
        index -= 1
#        print 'checking lobbyist {0}\n'.format(index)
        find_lobbyist_employment(dd_cursor, index)

      val = 0
      index = 0
      tsvin.seek(0)

      #Iterate a second time so it can insert case 3 scenarios
      for row in tsvin_reader:
        form = row[3]
        sender_id = row[4]
        entity_cd = row[6]
        val = val + 1

        #case 3 - lobbyist and their direct employment under a lobbying firm
        if form == 'F604' and entity_cd == 'LBY' and sender_id[:1] == 'E':
          #print 'Case 3'
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
#          print 'filer_id = {0}\n'.format(filer_id)
#          print 'sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n'.format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
          oid = get_organization(dd_cursor, org_name)
          insert_lobbyist(dd_cursor, pid, filer_id)
          insert_lobbyist_direct_employment(dd_cursor, pid, oid, rpt_date, ls_beg_yr, ls_end_yr)

        
    # Set foreign key checks on afterwards
    dd_cursor.execute('SET foreign_key_checks = 1')

    logger.info(__file__ + ' terminated successfully.', 
        full_msg='Inserted ' + str(LF_INSERT) + ' rows in LobbingFirm, inserted ' 
                  + str(FS_INSERT) + ' rows in LobbyingFirmState, inserted ' 
                  + str(L_INSERT) + ' rows in Lobbyist, inserted ' 
                  + str(P_INSERT) + ' rows in Person, inserted ' 
                  + str(O_INSERT) + ' rows in Organizations, inserted ' 
                  + str(ER_INSERT) + ' rows in LobbyistEmployer, inserted '
                  + str(EM_INSERT) + ' rows in LobbyistEmployment, inserted ' 
                  + str(DE_INSERT) + ' rows in LobbyistDirectEmployment, and inserted'
                  + str(LC_INSERT) + ' rows in LobbyingContracts.',
        additional_fields={'_affected_rows':str(LF_INSERT + FS_INSERT + L_INSERT + P_INSERT + O_INSERT + ER_INSERT + EM_INSERT + DE_INSERT + LC_INSERT),
                           '_inserted':'LobbingFirm:'+str(LF_INSERT)
                                       +', LobbyingFirmState:'+str(FS_INSERT)
                                       +', Lobbyist:'+str(L_INSERT)
                                       +', Person:'+str(P_INSERT)
                                       +', Organizations:'+str(O_INSERT)
                                       +', LobbyistEmployer:'+str(ER_INSERT)
                                       +', LobbyistEmployment:'+str(EM_INSERT)
                                       +', LobbyistDirectEmployment:'+str(DE_INSERT)
                                       +', LobbyingContracts.:'+str(LC_INSERT),
                           '_state':'CA'})
      
if __name__ == '__main__':
  with GrayLogger(API_URL) as _logger:                                          
    logger = _logger                                                            
    main()
