# -*- coding: utf8 -*-
'''
File: import_committees_ny.py
Author: John Alkire
Date: 11/26/2015
Description:
- Imports NY committees using senate API
- Fills Committee and servesOn
- Currently configured to test DB
'''
import requests
import MySQLdb

def call_senate_api(restCall, year, house, offset):
    if house != "":
        house = "/" + house
    url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "?full=true&limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=" + str(offset)
    r = requests.get(url)
    out = r.json()
    return out["result"]["items"]

def get_last_cid_db(dddb):
    select_comm = '''SELECT cid FROM Committee
                    ORDER BY cid DESC
                    LIMIT 1
                  '''
    cur = dddb.cursor()                  
    cur.execute(select_comm)
    
    query = cur.fetchone();
    return query[0]

def is_comm_in_db(comm, dddb):
    cur = dddb.cursor()
    select_comm = '''SELECT cid 
                    FROM Committee
                    WHERE house = %(house)s and name = %(name)s and
                     state = %(state)s'''                                                                    
    try:
        cur.execute(select_comm, comm)
        query = cur.fetchone()
             
        if query is None:                   
            return False       
    except:            
        return False
    return query
    
def is_serveson_in_db(member, dddb):
    cur = dddb.cursor()
    select_comm = '''SELECT pid 
                    FROM servesOn
                    WHERE pid = %(pid)s and year = %(year)s and 
                    house = %(house)s and cid = %(cid)s and state = %(state)s'''                                                                    
    try:
        cur.execute(select_comm, member)
        query = cur.fetchone()
        
        if query is None:
            #print select_comm % member            
            return False       
    except:            
        return False    
    
    return True

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
    
def get_committees_api(year):
    committees = call_senate_api("committees", 2015, "senate", 0)
    ret_comms = list()
    for comm in committees:
        committee = dict()
        committee['name'] = comm['name']
        
        committee['house'] = "Senate"
        committee['state'] = "NY"
        committee['members'] = list()
        members = comm['committeeMembers']['items']
        for member in members:
            sen = dict()                
            name = clean_name(member['fullName']) 
            sen['last'] = name[1]
            sen['first'] = name[0]
            sen['year'] = str(year)
            sen['house'] = "Senate"
            sen['state'] = "NY"
            if member['title'] == "CHAIR_PERSON":           
                sen['position'] = "chair"
            else:
                sen['position'] = "member"
            committee['members'].append(sen)
        ret_comms.append(committee)
    print "Downloaded %d committees..." % len(ret_comms)
    return ret_comms

def add_committees_db(year, dddb):
    committees = get_committees_api(year)
    cur = dddb.cursor()
    x = 0
    y = 0
    for committee in committees:
        cid = get_last_cid_db(dddb) + 1      
        get_cid = is_comm_in_db(committee, dddb)
        if  get_cid == False:
            x = x + 1
            committee['cid'] = str(cid)
            insert_stmt = '''INSERT INTO Committee
                            (cid, house, name, state)
                            VALUES
                            (%(cid)s, %(house)s, %(name)s, %(state)s);
                            '''
            #print (insert_stmt % committee)
            cur.execute(insert_stmt, committee)
        else:
            committee['cid'] = get_cid[0]
           
        
        for member in committee['members']:
            member['pid'] = get_pid_db(member['first'], member['last'], dddb)
            member['cid'] = committee['cid']
            
            if is_serveson_in_db(member, dddb) == False:                
                insert_stmt = '''INSERT INTO servesOn
                            (pid, year, house, cid, state, position)
                            VALUES
                            (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s, %(position)s);
                            '''
                #print (insert_stmt % member)
                cur.execute(insert_stmt, member)                
                y = y + 1
    print "Added %d committees and %d members" % (x,y)
                        
    
def get_pid_db(first, last, dddb):
    select_person = '''SELECT * FROM Person
                     WHERE first = %(first)s
                      AND last = %(last)s 
                  '''
    cur = dddb.cursor()                  
    cur.execute(select_person, {'first':first,'last':last})
    
    query = cur.fetchone();
    return query[0]
    

def main():
    dddb =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='JohnTest',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8')
    dddb.autocommit(True)
      

    add_committees_db(2015, dddb)
    
    dddb.close()
main()
    
    

