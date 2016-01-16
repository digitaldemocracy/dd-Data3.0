# -*- coding: utf8 -*-
'''
File: import_legislators_ny.py
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

def call_senate_api(restCall, year, house, offset):
    if house != "":
        house = "/" + house
    url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(year) + house + "?full=true&limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=" + str(offset)
    r = requests.get(url)
    out = r.json()
    return out["result"]["items"]

def is_leg_in_db(senator, dddb):
    select_leg = '''SELECT * 
                    FROM Person p, Legislator l
                    WHERE first = %(first)s AND last = %(last)s AND state = %(state)s
                    AND p.pid = l.pid'''                                               
    try:
        dddb.execute(select_leg, senator)
        query = dddb.fetchone()
        
        if query is None:            
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
    
def get_senators_api(year):
    senators = call_senate_api("members", year, "", 0)
    ret_sens = list()
    for senator in senators:
        sen = dict()
        name = clean_name(senator['fullName']) 
        sen['house'] = senator['chamber'].title()
        sen['last'] = name[1]
        sen['state'] = "NY"
        sen['year'] = str(year)            
        sen['first'] = name[0]    
        sen['district'] = senator['districtCode']
        sen['image'] = senator['imgName']
        if sen['image'] is None:
            sen['image'] = ''
        ret_sens.append(sen)
    print "Downloaded %d legislators..." % len(ret_sens)
    return ret_sens        

def add_senator_db(senator, dddb):
    if is_leg_in_db(senator, dddb) == False:
        insert_stmt = '''INSERT INTO Person
                        (last, first, image)
                        VALUES
                        (%(last)s, %(first)s, %(image)s);
                        '''
        dddb.execute(insert_stmt, senator)
        pid = dddb.lastrowid
        senator['pid'] = pid
        insert_stmt = '''INSERT INTO Legislator
                        (pid, state)
                        VALUES
                        (%(pid)s, %(state)s);
                        '''
        dddb.execute(insert_stmt, senator)
        insert_stmt = '''INSERT INTO Term
                        (pid, year, house, state, district)
                        VALUES
                        (%(pid)s, %(year)s, %(house)s, %(state)s, %(district)s);
                        '''
        dddb.execute(insert_stmt, senator)        
        #print "Added " + senator['last'] + ", " + senator['first']    
        return True
    return False
     
def add_senators_db(year, dddb):
    senators = get_senators_api(year)
    x = 0
    for senator in senators:
    #senator = senators[0]
        if add_senator_db(senator, dddb):
            x = x + 1

    print "Added %d legislators" % x 


def main():
    dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='JohnTest',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8')
    dddb = dddb_conn.cursor()
    dddb_conn.autocommit(True)
    add_senators_db(2015, dddb)
    dddb_conn.close()
    
main()

    
    

