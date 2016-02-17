#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: ny_import_legislators.py
Author: John Alkire
Date: 11/26/2015
Description:
- Imports NY legislators using senate API
- Fills Person, Term, and Legislator
- Missing personal/social info for legislators (eg. bio, twitter, etc)
- Currently configured to test DB
'''
import requests
import MySQLdb
import loggingdb

insert_person = '''INSERT INTO Person
                (last, first, image)
                VALUES
                (%(last)s, %(first)s, %(image)s);'''

insert_legislator = '''INSERT INTO Legislator
                (pid, state)
                VALUES
                (%(pid)s, %(state)s);'''

insert_term = '''INSERT INTO Term
                (pid, year, house, state, district)
                VALUES
                (%(pid)s, %(year)s, %(house)s, %(state)s, %(district)s);'''
                
select_legislator = '''SELECT p.pid 
                       FROM Person p, Legislator l
                       WHERE first = %(first)s 
                        AND last = %(last)s 
                        AND state = %(state)s
                        AND p.pid = l.pid'''

select_term = '''SELECT p.pid 
                 FROM Person p, Term t
                 WHERE first = %(first)s 
                  AND last = %(last)s 
                  AND state = %(state)s
                  AND year = %(year)s
                  AND house = %(house)s
                  AND district = %(district)s
                  AND p.pid = t.pid'''                        
                                           
API_YEAR = 2016
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"

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
#returns true/false as expected
def is_term_in_db(senator, dddb):                                            
    try:
        dddb.execute(select_term, senator)
        query = dddb.fetchone()
        
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
    print "Downloaded %d legislators..." % len(ret_sens)
    return ret_sens        

#function to add legislator's data to Person, Legislator, and Term
#adds to Person and Legislator if they are not already filled
#and adds to Term if it is not already filled
def add_senator_db(senator, dddb):
    pid = is_leg_in_db(senator, dddb)
    ret = False
    senator['pid'] = pid
    if pid == False:
        dddb.execute(insert_person, senator)
        pid = dddb.lastrowid        
        senator['pid'] = pid
        dddb.execute(insert_legislator, senator)  
        ret = True
    
    if is_term_in_db(senator, dddb) == False:   
        print insert_term % senator  
        dddb.execute(insert_term, senator)        
       
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
    dddb_conn =  loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8')
    dddb = dddb_conn.cursor()
    dddb_conn.autocommit(True)
    add_senators_db(dddb)
    dddb_conn.close()
    
main()
