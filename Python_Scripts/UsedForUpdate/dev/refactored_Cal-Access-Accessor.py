#!/usr/bin/env python
'''
File: Cal-Access-Accessor.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Goes through the file CVR_REGISTRATION_CD.TSV and places the data into DDDB2015Apr
- This script runs under the update script
- Fills table:
  LobbyingFirm (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
  Lobbyist (pid, filer_id)
  Person (last, first)
  LobbyistEmployer (filer_naml, filer_id, coalition)
  LobbyistEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
  LobbyistDirectEmployment (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
  LobbyingContracts (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr)

Sources:
- db_web_export.zip (California Access)
  - CVR_REGISTRATION_CD.TSV
'''

import loggingdb
import csv
import refactored_Person_Name_Fix
import refactored_Lobbying_Firm_Name_Fix
from clean_name import clean_name

# U.S. State
state = 'CA'

#Querys used to Insert into the Database
QI_LOBBYING_FIRM = '''INSERT INTO LobbyingFirm (filer_naml, filer_id, rpt_date,
                       ls_beg_yr, ls_end_yr)
                      VALUES (%s, %s, %s, %s, %s)'''
QI_LOBBYIST = '''INSERT INTO Lobbyist (pid, filer_id, state)
                 VALUES (%s, %s, %s)'''
QI_PERSON = '''INSERT INTO Person (last, first)
               VALUES (%s, %s)'''
QI_LOBBYIST_EMPLOYER = '''INSERT INTO LobbyistEmployer (filer_naml, filer_id,
                           coalition, state)
                          VALUES (%s, %s, %s, %s)'''
QI_LOBBYIST_EMPLOYMENT = '''INSERT INTO LobbyistEmployment (pid, sender_id,
                             rpt_date, ls_beg_yr, ls_end_yr, state)
                            VALUES (%s, %s, %s, %s, %s, %s)'''
QI_LOBBYIST_DIRECT_EMPLOYMENT = '''INSERT INTO LobbyistDirectEmployment (pid,
                                    sender_id, rpt_date, ls_beg_yr, ls_end_yr,
                                    state)
                                   VALUES (%s, %s, %s, %s, %s, %s)'''
QI_LOBBYING_CONTRACTS = '''INSERT INTO LobbyingContracts (filer_id, sender_id,
                            rpt_date, ls_beg_yr, ls_end_yr, state)
                           VALUES (%s, %s, %s, %s, %s, %s)'''

#Currently a static table that is sized to 10,000
Lobbyist = [[0 for x in xrange(5)] for x in xrange(10000)]

#Changes the date into a linux format for the database
def format_date(str):
  temp = ''
  str = str.split('/');
  for x in range(0,3):
    if len(str[x]) == 1:
      str[x] = "0" + str[x]
  temp = '-'.join([str[2], str[0], str[1]])
  return temp
  
#Finds the pid of the Person (returns val if none is found)
def getPerson(cursor, filer_id, filer_naml, filer_namf, val, state):
  select_stmt = "SELECT pid, filer_id, state FROM Lobbyist WHERE filer_id = %(filer_id)s AND state = %(state)s ORDER BY pid"
  cursor.execute(select_stmt, {'filer_id':filer_id, 'state':state})
  if(cursor.rowcount == 1):
    return cursor.fetchone()[0]
  pid = val

  split_index = len(filer_namf.split(' '))
  name = '%s %s' % (filer_namf, filer_naml)
  cleaned_name = clean_name(name,
                            refactored_Person_Name_Fix.name_clean).split(' ')
  filer_namf = ' '.join(cleaned_name[:split_index])
  filer_naml = ' '.join(cleaned_name[split_index:])
  
  select_pid = "SELECT pid FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf)s ORDER BY Person.pid;"
  cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
  if cursor.rowcount == 1:
    pid = cursor.fetchone()[0]
  elif cursor.rowcount > 1:
    pid = cursor.fetchone()[0]
  else:
    cursor.execute(QI_PERSON, (filer_naml, filer_namf))
    select_pid = "SELECT pid FROM Person ORDER BY Person.pid DESC limit 1;"
    cursor.execute(select_pid);
    pid = cursor.fetchone()[0];
  return pid

#inserts into lobbyist employer 
def insert_lobbyist_employer(cursor, filer_naml, filer_id, coalition, state):
  select_stmt = "SELECT filer_id FROM LobbyistEmployer WHERE filer_id = %(filer_id)s AND state = $(state)s"
  cursor.execute(select_stmt, {'filer_id':filer_id, 'state':state})
  if(cursor.rowcount == 0):
    filer_naml = refactored_Lobbying_Firm_Name_Fix.clean_name(filer_naml)
    cursor.execute(QI_LOBBYIST_EMPLOYER, (filer_naml, filer_id, coalition, state)) 

#inserts into lobbying firm
def insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr): 
  select_stmt = "SELECT filer_id FROM LobbyingFirm WHERE filer_id = %(filer_id)s"
  cursor.execute(select_stmt, {'filer_id':filer_id})
  if(cursor.rowcount == 0):
    filer_naml = refactored_Lobbying_Firm_Name_Fix.clean_name(filer_naml)
    cursor.execute(QI_LOBBYING_FIRM, (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr))
    
#inserts into lobbyist
def insert_lobbyist(cursor, pid, filer_id, state):
  select_stmt = "SELECT pid FROM Lobbyist WHERE pid = %(pid)s AND state = %(state)s"
  cursor.execute(select_stmt, {'pid':pid, 'state':state})
  if(cursor.rowcount > 0):
    return
  select_stmt = "SELECT filer_id FROM Lobbyist WHERE filer_id = %(filer_id)s ANDstate = %(state)s"
  cursor.execute(select_stmt, {'filer_id':filer_id, 'state':state})
  if(cursor.rowcount == 0):
    cursor.execute(QI_LOBBYIST, (pid, filer_id, state))

def insert_lobbyist_employment(cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state):
  select_stmt = "SELECT sender_id, rpt_date, ls_beg_yr, state FROM LobbyistEmployment WHERE sender_id = %(sender_id)s AND pid = %(pid)s AND ls_beg_yr = %(ls_beg_yr)s AND ls_end_yr = %(ls_end_yr)s AND state = %(state)s;"
  cursor.execute(select_stmt, {'sender_id':sender_id,'pid':pid,'ls_beg_yr':ls_beg_yr,'ls_end_yr':ls_end_yr,'state':state})
  if(cursor.rowcount == 0):
    cursor.execute(QI_LOBBYIST_EMPLOYMENT, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
    
def insert_lobbyist_direct_employment(cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state):
  select_stmt = "SELECT sender_id, rpt_date, ls_beg_yr, state FROM LobbyistDirectEmployment WHERE sender_id = %(sender_id)s AND pid = %(pid)s AND ls_beg_yr = %(ls_beg_yr)s AND ls_end_yr = %(ls_end_yr)s AND state = %(state)s;"
  cursor.execute(select_stmt, {'sender_id':sender_id,'pid':pid,'ls_beg_yr':ls_beg_yr,'ls_end_yr':ls_end_yr,'state':state})
  if(cursor.rowcount == 0):
    cursor.execute(QI_LOBBYIST_DIRECT_EMPLOYMENT, (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
    
def insert_lobbyist_contracts(cursor, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state):
  select_stmt = "SELECT filer_id, sender_id, rpt_date, state FROM LobbyingContracts WHERE filer_id = %(filer_id)s AND sender_id = %(sender_id)s AND rpt_date = %(rpt_date)s AND state = %(state)s"
  cursor.execute(select_stmt, {'filer_id':filer_id,'sender_id':sender_id,'rpt_date':rpt_date,'state':state})
  if(cursor.rowcount == 0):
    cursor.execute(QI_LOBBYING_CONTRACTS, (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state))
  
#For case 4
#Goes through the lobbyist list and determines if they work for
#Lobbyist Employment or LobbyistDirectEmployment  
def find_lobbyist_employment(cursor, index):
  select_stmt = "SELECT filer_id FROM LobbyingFirm WHERE filer_id = %(sender_id)s"
  cursor.execute(select_stmt, {'sender_id':Lobbyist[index][1]})
  if(cursor.rowcount > 0):
    select_stmt = "SELECT * FROM LobbyistEmployment WHERE pid = %(pid)s AND sender_id = %(sender_id)s AND rpt_date = %(rpt_date)s AND ls_end_yr = %(ls_end_yr)s AND state = %(state)s"
    cursor.execute(select_stmt, {'pid':Lobbyist[index][0], 'sender_id':Lobbyist[index][1], 'rpt_date':Lobbyist[index][2], 'ls_end_yr':Lobbyist[index][4]})
    if(cursor.rowcount == 0):
      cursor.execute(QI_LOBBYIST_EMPLOYMENT, (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2], Lobbyist[index][3], Lobbyist[index][4]))
  select_stmt = "SELECT filer_id FROM LobbyistEmployer WHERE filer_id = %(sender_id)s"
  cursor.execute(select_stmt, {'sender_id':Lobbyist[index][1]})
  if(cursor.rowcount > 0):
    select_stmt = "SELECT * FROM LobbyistDirectEmployment WHERE pid = %(pid)s AND sender_id = %(sender_id)s AND rpt_date = %(rpt_date)s AND ls_end_yr = %(ls_end_yr)s AND state = %(state)s"
    cursor.execute(select_stmt, {'pid':Lobbyist[index][0], 'sender_id':Lobbyist[index][1], 'rpt_date':Lobbyist[index][2], 'ls_end_yr':Lobbyist[index][4]})
    if(cursor.rowcount == 0):
      cursor.execute(QI_LOBBYIST_DIRECT_EMPLOYMENT, (Lobbyist[index][0], Lobbyist[index][1], Lobbyist[index][2], Lobbyist[index][3], Lobbyist[index][4]))
        
def main():
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='DDDB2015July',
                       user='awsDB',
                       passwd='digitaldemocracy789') as cursor:
    refOff = "SET foreign_key_checks = 0"
    cursor.execute(refOff)
    with open('/home/data_warehouse_common/scripts/CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
      tsvin = csv.reader(tsvin, delimiter='\t')
      
      val = 0
      index = 0

      for row in tsvin:
        form = row[3]
        sender_id = row[4]
        entity_cd = row[6]
        val = val + 1
        print val
        #case 1 - Lobbying Firm
        if form == "F601" and entity_cd == "FRM" and (sender_id[:1] == 'F' or sender_id[:1].isdigit()) and sender_id == row[5]: 
          filer_naml = row[7]
          filer_id = row[5]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          print "naml = {0}, id = {1}, date = {2}, beg = {3}, end = {4}\n".format(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
          insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
        #case 2 - Lobbyist and their employer
        elif form == "F604" and entity_cd == "LBY" and sender_id[:1] == 'F':
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          pid = getPerson(cursor, filer_id, filer_naml, filer_namf, val, state)
          print "filer_id = {0}\n".format(filer_id)
          print "sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n".format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
          insert_lobbyist(cursor, pid, filer_id, state)
          insert_lobbyist_employment(cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
        #case 3 - lobbyist and their direct employment under a lobbying firm
        elif form == "F604" and entity_cd == "LBY" and sender_id[:1] == 'E':
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          pid = getPerson(cursor, filer_id, filer_naml, filer_namf, val, state)
          print "filer_id = {0}\n".format(filer_id)
          print "sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n".format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
          insert_lobbyist(cursor, pid, filer_id, state)
          insert_lobbyist_direct_employment(cursor, pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
        #case 4 - found a lobbyist, but must determine later if they are under a firm or an employer
        elif form == "F604" and entity_cd == "LBY" and sender_id.isdigit():
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
          print "filer_id = {0}\n".format(filer_id)
          pid = getPerson(cursor, filer_id, filer_naml, filer_namf, val, state)
          insert_lobbyist(cursor, pid, filer_id, state)
          print "inserting Lobbyist into index {0}\n".format(index)
          #insert the lobbyist into the array for later
          Lobbyist[index][0] = pid
          Lobbyist[index][1] = sender_id
          Lobbyist[index][2] = rpt_date
          Lobbyist[index][3] = ls_beg_yr
          Lobbyist[index][4] = ls_end_yr
          index += 1
        #case 5 - Found an employer who is under a contract
        elif form == "F602" and entity_cd == "LEM":
          filer_naml = row[7]
          filer_namf = row[8]
          filer_id = row[5]
          sender_id = row[4]
          rpt_date = row[12]
          rpt_date = rpt_date.split(' ')[0]
          rpt_date = format_date(rpt_date)
          ls_beg_yr = row[13]
          ls_end_yr = row[14]
          coalition = (filer_id[:1] == 'C') * 1
          print "filer_naml = {0}, filer_id = {1}, coalition = {2}\n".format(filer_naml, filer_id, coalition)
          insert_lobbyist_employer(cursor, filer_naml, filer_id, coalition, state)
          insert_lobbyist_contracts(cursor, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
        #case 6 - Lobbyist EMployer
        elif form == "F603" and entity_cd == "LEM":
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
          coalition = (filer_id[:1] == 'C') * 1
          print "filer_naml = {0}, filer_id = {1}, coalition = {2}\n".format(filer_naml, filer_id, coalition)
          if(ind_cb == 'X'):
            insert_lobbyist_employer(cursor, filer_naml + filer_namf, filer_id, coalition, state)
          else:
            insert_lobbyist_employer(cursor, filer_naml, filer_id,  coalition, state)
        #case 7 - IGNORE
        elif form == "F606":
          print 'case 7'
        #case 8 - IGNORE
        elif form == "F607" and entity_cd == "LEM":
          print 'case 8'
        #just to catch those that dont fit at all
        else:
          print 'Does not match any case!'
          
      #Goes through the Lobbyist table and finds employment
      #Continuation of case 4
      while index:
        index -= 1
        print "checking lobbyist {0}\n".format(index)
        find_lobbyist_employment(cursor, index)
        
    refOn = "Set foreign_key_checks = 1"
    cursor.execute(refOn)   
      
if __name__ == "__main__":
  main()  
