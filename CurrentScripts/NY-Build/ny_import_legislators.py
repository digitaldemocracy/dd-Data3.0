#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
'''
File: ny_import_legislators.py
Author: John Alkire
Modified: Eric Roh
Date: 6/21/2016
Description:
- Imports NY legislators using senate API
- Fills Person, Term, and Legislator
- Missing personal/social info for legislators (eg. bio, twitter, etc)
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
P_INSERT = 0
L_INSERT = 0
T_INSERT = 0
T_UPDATE = 0

insert_person = '''INSERT INTO Person
                (last, first, image, source)
                VALUES
                (%(last)s, %(first)s, %(image)s, "ny_import_legislators.py");'''

insert_legislator = '''INSERT INTO Legislator
                (pid, state)
                VALUES
                (%(pid)s, %(state)s);'''

insert_term = '''INSERT INTO Term
                (pid, year, house, state, district, current_term)
                VALUES
                (%(pid)s, %(year)s, %(house)s, %(state)s, %(district)s, %(current)s);'''
                
select_legislator = '''SELECT p.pid 
                       FROM Person p, Legislator l
                       WHERE first = %(first)s 
                        AND last = %(last)s 
                        AND state = %(state)s
                        AND p.pid = l.pid'''

select_term = '''SELECT district, current_term
                 FROM Term
                 WHERE pid = %(pid)s 
                  AND state = %(state)s
                  AND year = %(year)s
                  AND house = %(house)s'''

QU_TERM = '''UPDATE Term
             SET district = %(district)s, current_term = 1
             WHERE pid = %(pid)s
              AND state = %(state)s
              AND year = %(year)s
              AND house = %(house)s'''                        
                                           
API_YEAR = 2017
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
#API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"
API_URL += "key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"

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
        "WILLIAMS":("Jamie", "Williams"),
        "PEOPLES-STOKE":("Crystal", "Peoples-Stoke"),
        "KAMINSKY":("Todd", "Kaminsky"),
        "HYNDMAN":("Alicia", "Hyndman"),
        "HUNTER":("Pamela", "Hunter"),
        "HARRIS":("Pamela", "Harris"),
        "CASTORINA":("Ron", "Castorina", "Jr"),
        "CANCEL":("Alice", "Cancel"),
        "BARNWELL":("Brian", "Barnwell"),
        "BYRNE":("Kevin", "Byrne"),
        "CARROLL":("Robert", "CARROLL"),
        "ERRIGO":("Joseph", "Errigo"),
        "HEASTIE":("Carl", "Heastie"),
        "JONES":("Billy", "Jones"),
        "NORRIS":("Michael", "Norris"),
        "VANEL":("Clyde", "Vanel"),
        "WALSH":("Mary", "Walsh"),
        "MILLER":("Brian", "Miller"),
    }
    
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()      
    suffix = "";         

    if len(name_arr) == 1 and name_arr[0] in problem_names.keys():
      name_arr = list(problem_names[name_arr[0]])

    if len(name_arr) > 2:
      print(name_arr)
          
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
    #print "Hey Yall!!! I'm going to print the result of the API_URL format call!"
    r = requests.get(url)
    print url
    out = r.json()
    return out["result"]["items"]

#checks if Legislator + Person is in database. 
#If it is, return its PID. Otherwise, return false
def is_leg_in_db(senator, dddb):                                            
    try:
        dddb.execute(select_legislator, senator)
        query = dddb.fetchone()
        
        if query is None:            
            return False       
    except:            
        return False
    
    return query[0]

#checks if Term + Person is in database.
# UPDATES the Term if the district has changed. 
#returns true/false as expected
def is_term_in_db(senator, dddb):
    global T_UPDATE
    try:
        dddb.execute(select_term, senator)
        query = dddb.fetchone()

        if query[0] != senator['district'] or query[1] == 0:
            #print "Hella updated! Radical!~~~~~"
            #print 'updated', senator
            try:
              dddb.execute(QU_TERM, senator)
              T_UPDATE += dddb.rowcount
            except MySQLdb.Error:
              logger.warning('Update Failed', full_msg=traceback.format_exc(),
                  additional_fields=create_payload('Term', (QU_TERM % senator)))
            T_UPDATE += dddb.rowcount
            return True
        if query is None:
            return False
    except:
        return False
    
    return True

#function to call senate API and process all necessary data into lists and     
#dictionaries. Returns list of senator dicts        
def get_senators_api():
    senators = call_senate_api("members", "", 0)
    ret_sens = list()
    for senator in senators:
      try:
        sen = dict()
        name = clean_name(senator['fullName'])
        sen['house'] = senator['chamber'].title()
        sen['last'] = name[1]
        sen['state'] = "NY"
        sen['year'] = senator['sessionYear']            
        sen['first'] = name[0]    
        sen['district'] = senator['districtCode']
        sen['image'] = senator['imgName']
        if sen['image'] is None:
            sen['image'] = ''
        ret_sens.append(sen)
      except IndexError as error:
        logger.warning('Problem with name ' + senator['fullName'], 
            full_msg=traceback.format_exc(), additional_fields={'_state':'NY'})
    print "Downloaded %d legislators..." % len(ret_sens)
    return ret_sens        

#function to add legislator's data to Person, Legislator, and Term
#adds to Person and Legislator if they are not already filled
#and adds to Term if it is not already filled
def add_senator_db(senator, dddb):
    global P_INSERT, L_INSERT, T_INSERT
    pid = is_leg_in_db(senator, dddb)
    ret = False
    senator['pid'] = pid
    if pid == False:
      try:
        dddb.execute(insert_person, senator)
        P_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(), 
            additional_fields=create_payload('Person', (insert_person % senator)))
      pid = dddb.lastrowid        
      senator['pid'] = pid
      try:
        dddb.execute(insert_legislator, senator)
        L_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Legislator', (insert_legislator % senator)))
      ret = True
    
    if is_term_in_db(senator, dddb) == False:   
      #print insert_term % senator  
      try:
        senator['current'] = 1
        dddb.execute(insert_term, senator)
        T_INSERT += dddb.rowcount
      except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Term', (insert_term % senator)))
       
    return ret    

#function to add legislators to DB. Calls API and calls add_senator_db on 
#each legislator
def add_senators_db(dddb):
    senators = get_senators_api()
    x = 0
    for senator in senators:    
        if add_senator_db(senator, dddb):
            x += 1

    #print "Added %d legislators" % x 

def main():
    ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=ddinfo['host'],
                        user=ddinfo['user'],
                        db=ddinfo['db'],
                        port=ddinfo['port'],
                        passwd=ddinfo['passwd'],
                        charset='utf8') as dddb:
      add_senators_db(dddb)
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
                             '_state':'NY'})

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()
