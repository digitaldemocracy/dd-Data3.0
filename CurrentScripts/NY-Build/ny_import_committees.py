#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: ny_import_committees.py
Author: John Alkire
Date: 11/26/2015
Description:
- Imports NY committees using senate API
- Fills Committee and servesOn
- Currently configured to test DB
'''

import sys
from Database_Connection import mysql_connection
import traceback
import requests
import MySQLdb
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
C_INSERTED = 0
S_INSERTED = 0

select_committee_last = '''SELECT cid FROM Committee
                           ORDER BY cid DESC
                           LIMIT 1'''
                            
select_committee = '''SELECT cid 
                      FROM Committee
                      WHERE house = %(house)s 
                       AND name = %(name)s 
                       AND state = %(state)s'''   
                                                
select_person = '''SELECT p.pid 
                   FROM Person p, Legislator l
                   WHERE first = %(first)s 
                    AND last = %(last)s 
                    AND state = %(state)s
                    AND p.pid = l.pid'''
                                                                  
select_serveson = '''SELECT pid 
                     FROM servesOn
                     WHERE pid = %(pid)s 
                      AND year = %(year)s 
                      AND house = %(house)s 
                      AND cid = %(cid)s 
                      AND state = %(state)s'''

insert_committee = '''INSERT INTO Committee
                       (cid, house, name, state)
                      VALUES
                       (%(cid)s, %(house)s, %(name)s, %(state)s);'''                                                       

insert_serveson = '''INSERT INTO servesOn
                      (pid, year, house, cid, state, position)
                     VALUES
                      (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s,
                      %(position)s);'''
API_YEAR = 2016
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"

STATE = 'NY'

def create_payload(table, sqlstmt):
    return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'NY'
    }

#this function takes in a full name and outputs a tuple with a first and last name 
#names should be cleaned to maintain presence of Jr, III, etc, but remove middle names.
#many names for assembly members in the New York Senate API do not line up with the assembly     
#website. The API names are replaced with the website names.        
def clean_name(name):
    problem_names = {
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
    }
    
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
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
    last = name_arr[0]
    last = last.replace(' ' ,'') + suffix
    
    if (first + ' ' + last) in problem_names.keys():             
        return problem_names[(first + ' ' + last)]
        
    return (first, last)

#calls NY Senate API and returns the list of results                        
def call_senate_api(restCall, house, offset):
    if house != "":
        house = "/" + house
    url = API_URL.format(restCall, API_YEAR, house, offset)
    r = requests.get(url)
    out = r.json()
    return out["result"]["items"]

#function gets the largest CID in the DB because CID does not autoincrement
def get_last_cid_db(cur):
    cur.execute(select_committee_last)
    
    query = cur.fetchone();
    return query[0]

#checks if Committee is in database. 
#If it is, return its CID. Otherwise, return false
def is_comm_in_db(comm, cur):
                                                                     
    try:
        cur.execute(select_committee, comm)
        query = cur.fetchone()
             
        if query is None:                   
            return False       
    except:            
        return False
    return query

#checks if a servesOn item exists in the DB.
#returns true/false as expected    
def is_serveson_in_db(member, cur):
                                                                 
    try:
        cur.execute(select_serveson, member)
        query = cur.fetchone()
        
        if query is None:            
            return False       
    except:            
        return False    
    
    return True

#function to call senate API and process all necessary data into lists and     
#dictionaries. Returns list of committee dicts
def get_committees_api():
    committees = call_senate_api("committees", "senate", 0)
    ret_comms = list()
    
    for comm in committees:    
        committee = dict()
        committee['name'] = comm['name']        
        committee['house'] = "Senate"
        committee['state'] = STATE
        committee['members'] = list()
        members = comm['committeeMembers']['items']
        
        for member in members:
          try:
            sen = dict()                
            name = clean_name(member['fullName']) 
            sen['last'] = name[1]
            sen['first'] = name[0]
            sen['year'] = member['sessionYear']
            sen['house'] = "Senate"
            sen['state'] = STATE
            
            if member['title'] == "CHAIR_PERSON":           
                sen['position'] = "chair"
            else:
                sen['position'] = "member"
                
            committee['members'].append(sen)
          except IndexError:
            logger.warning('Person not found ' + member['fullName'],
                additional_fields={'_state':'NY'})

        ret_comms.append(committee)
        
    #print "Downloaded %d committees..." % len(ret_comms)
    return ret_comms

#function to add committees to DB. Calls API and then processes data
#only adds committees if they do not exist and only adds to servesOn if member
#is not already there.
def add_committees_db(cur):
    global C_INSERTED, S_INSERTED
    committees = get_committees_api()
    x = 0
    y = 0
    for committee in committees:
        cid = get_last_cid_db(cur) + 1      
        get_cid = is_comm_in_db(committee, cur)
        
        if  get_cid == False:
            x += 1
            committee['cid'] = str(cid)
            try:
              cur.execute(insert_committee, {'cid':committee['cid'], 
                'house':committee['house'], 'name':committee['name'], 'state':committee['state']})
              C_INSERTED += cur.rowcount
            except MySQLdb.Error:
              logger.warning('Insert Failed', full_msg=traceback.format_exc(),        
                    additional_fields=create_payload('Committee', (insert_committee % committee)))
        else:
            committee['cid'] = get_cid[0]
                   
        for member in committee['members']:
            member['pid'] = get_pid_db(member, cur)
            member['cid'] = committee['cid']
            
            if is_serveson_in_db(member, cur) == False:                
              try:
                cur.execute(insert_serveson, member)
                S_INSERTED += cur.rowcount
              except MySQLdb.Error:
                logger.warning('Insert Failed', full_msg=traceback.format_exc(),        
                      additional_fields=create_payload('servesOn', (insert_serveson % member)))
              y += 1
                
    #print "Added %d committees and %d members" % (x,y)                        

#function to get PID of person based on name.     
def get_pid_db(person, cur):    
    cur.execute(select_person, person)
    
    query = cur.fetchone();
    return query[0]    

def main():
    ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=ddinfo['host'],
                        user=ddinfo['user'],
                        db=ddinfo['db'],
                        port=ddinfo['port'],
                        passwd=ddinfo['passwd'],
                        charset='utf8') as dddb:
      add_committees_db(dddb)
      logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(C_INSERTED) + ' rows in Committee and inserted ' 
                    + str(S_INSERTED) + ' rows in servesOn',
          additional_fields={'_affected_rows':'Committee:'+str(C_INSERTED)+
                                         ', servesOn:'+str(S_INSERTED),
                             '_inserted':'Committee:'+str(C_INSERTED)+
                                         ', servesOn:'+str(S_INSERTED),
                             '_state':'NY'})

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()
    
    

