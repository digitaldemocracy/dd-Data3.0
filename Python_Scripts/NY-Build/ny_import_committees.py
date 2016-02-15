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
import requests
import MySQLdb
import loggingdb

select_committee_last = '''SELECT cid FROM Committee
                           ORDER BY cid DESC
                           LIMIT 1'''
                            
select_committee = '''SELECT cid 
                      FROM Committee
                      WHERE house = %(house)s 
                       AND name = %(name)s 
                       AND state = %(state)s'''   
                                                
select_person = '''SELECT * FROM Person
                   WHERE first = %(first)s
                    AND last = %(last)s'''
                                                                  
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
                        
def call_senate_api(restCall, house, offset):
    if house != "":
        house = "/" + house
    url = API_URL.format(restCall, API_YEAR, house, offset)
    r = requests.get(url)
    out = r.json()
    return out["result"]["items"]

def get_last_cid_db(dddb):

    cur = dddb.cursor()                  
    cur.execute(select_committee_last)
    
    query = cur.fetchone();
    return query[0]

def is_comm_in_db(comm, dddb):
    cur = dddb.cursor()
                                                                     
    try:
        cur.execute(select_committee, comm)
        query = cur.fetchone()
             
        if query is None:                   
            return False       
    except:            
        return False
    return query
    
def is_serveson_in_db(member, dddb):
    cur = dddb.cursor()
                                                                 
    try:
        cur.execute(select_serveson, member)
        query = cur.fetchone()
        
        if query is None:            
            return False       
    except:            
        return False    
    
    return True
    
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
            
        ret_comms.append(committee)
        
    print "Downloaded %d committees..." % len(ret_comms)
    return ret_comms

def add_committees_db(dddb):
    committees = get_committees_api()
    cur = dddb.cursor()
    x = 0
    y = 0
    for committee in committees:
        cid = get_last_cid_db(dddb) + 1      
        get_cid = is_comm_in_db(committee, dddb)
        
        if  get_cid == False:
            x += 1
            committee['cid'] = str(cid)
            cur.execute(insert_committee, committee)
        else:
            committee['cid'] = get_cid[0]
                   
        for member in committee['members']:
            member['pid'] = get_pid_db(member['first'], member['last'], dddb)
            member['cid'] = committee['cid']
            
            if is_serveson_in_db(member, dddb) == False:                
                cur.execute(insert_serveson, member)                
                y += 1
                
    print "Added %d committees and %d members" % (x,y)                        
    
def get_pid_db(first, last, dddb):    
    cur = dddb.cursor()                  
    cur.execute(select_person, {'first':first,'last':last})
    
    query = cur.fetchone();
    return query[0]
    

def main():
    dddb =  loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8')
    dddb.autocommit(True)
      
    add_committees_db(dddb)
    
    dddb.close()
main()
    
    

