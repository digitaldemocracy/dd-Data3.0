#!/usr/bin/env python
'''
File: ny_import_committees.py
Author: John Alkire
Date: 12/16/2015
Description:
- Imports NY committees by scraping assembly webpage
- Fills Committee and servesOn
- Currently configured to test DB
'''

import sys
from Database_Connection import mysql_connection
import traceback
from lxml import html
import requests
import MySQLdb
from graylogger.graylogger import GrayLogger                                    
import datetime as dt
from time import strftime
GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'                 
logger = None
C_INSERTED = 0
S_INSERTED = 0
S_UPDATED = 0

insert_committee = '''INSERT INTO Committee
                       (cid, house, name, state)
                      VALUES
                       (%(cid)s, %(house)s, %(name)s, %(state)s);'''
                    
insert_serveson = '''INSERT INTO servesOn
                      (pid, year, house, cid, state, position, current_flag, start_date)
                     VALUES
                      (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s, %(position)s, %(current_flag)s, %(start_date)s);'''

update_serveson = '''UPDATE servesOn
                     SET end_date = %(end_date)s,
                      current_flag = %(current_flag)s
                     WHERE pid = %(pid)s
                      AND cid = %(cid)s
                      AND house = %(house)s
                      AND state = %(state)s
                      AND year = %(year)s
                      '''

select_committee = '''SELECT cid 
                      FROM Committee
                      WHERE house = %(house)s 
                       AND name = %(name)s 
                       AND state = %(state)s'''        

select_last_committee = '''SELECT cid FROM Committee
                           ORDER BY cid DESC
                           LIMIT 1'''
                           
select_person = '''SELECT l.pid
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

select_current_members = '''SELECT pid
                            FROM servesOn
                            WHERE house = %(house)s
                             AND cid = %(cid)s
                             AND state = %(state)s
                             AND current_flag = true
                             AND year = %(year)s'''

YEAR = '2015'
STATE = 'NY'                                         
COMMITTEES_URL = 'http://assembly.state.ny.us/comm/'
CATEGORIES_XP = '//*[@id="sitelinks"]/span//text()'
COMMITTEES_XP = '//*[@id="sitelinks"]//ul[{0}]//li/strong/text()'
COMMITTEE_LINK_XP = '//*[@id="sitelinks"]//ul[{0}]//li[{1}]/a[contains(@href,"mem")]/@href'
MEMBERS_XP = '//*[@id="sitelinks"]/span//li/a//text()'

def create_payload(table, sqlstmt):                                             
      return {                                                                    
        '_table': table,                                                          
        '_sqlstmt': sqlstmt,                                                      
        '_state': 'NY'                                                            
      }

def get_last_cid_db(dddb):
	dddb.execute(select_last_committee)
	
	query = dddb.fetchone();
	return query[0]

def is_comm_in_db(comm, dddb):                                                            
    try:
        dddb.execute(select_committee, {'house':comm['house'], 'name':comm['name'],
         'state':comm['state']})
        query = dddb.fetchone()
             
        if query is None:                   
            return False       
    except:            
        logger.warning("Select query failed", full_msg = traceback.format_exc(),
         additional_fields = create_payload("Committee", (select_committee % comm)))
    return query
    
def is_serveson_in_db(member, dddb):                                                                   
    try:
        dddb.execute(select_serveson, member)
        query = dddb.fetchone()
        
        if query is None:            
            return False       
    except:            
        logger.warning("Select query failed", full_msg = traceback.format_exc(),
         additional_fields = create_payload("servesOn", (select_serveson % member)))
    
    return True

def clean_name(name):
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
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
                    
    last = name_arr[0] + suffix
    return (first, last)
    
def get_committees_html():
    page = requests.get(COMMITTEES_URL)
    tree = html.fromstring(page.content)
    categories_html = tree.xpath(CATEGORIES_XP)
    ret_comms = list()
    committees = dict()
    x = 1
    positions = ["chair", "member"]
    count = 0;
    
    for category in categories_html:
        committees_html = tree.xpath(COMMITTEES_XP.format(x))        
        y = 1
        #print category
        
        for comm in committees_html:                    
            link = tree.xpath(COMMITTEE_LINK_XP.format(x,y))
            committee = dict()
            committee['name'] = comm
            committee['type'] = category
            committee['house'] = "Assembly"
            committee['state'] = STATE
            committee['members'] = list()
            #print "    "+comm
            
            if len(link) > 0:
                strip_link = link[0][0:len(link[0]) - 1]
            
                link = COMMITTEES_URL + strip_link                                      
                
                member_page = requests.get(link)
                member_tree = html.fromstring(member_page.content)
                
                members_html = member_tree.xpath(MEMBERS_XP)
                position = 0
                
                for mem in members_html:                    
                    sen = dict()
                    name = clean_name(mem)                        
                    sen['position'] = positions[position] 
                    sen['last'] = name[1]        
                    sen['first'] = name[0]                                        
                    sen['year'] = YEAR
                    sen['house'] = "Assembly"
                    sen['state'] = STATE
                    committee['members'].append(sen)
                    position = 1
            
            count = count + 1
            ret_comms.append(committee)    
            y = y + 1
        x = x + 1   
    #print "Scraped %d committees..." % len(ret_comms)
    return ret_comms                 

def get_past_members(committee, dddb):
    update_members = list()
    try:
        dddb.execute(select_current_members, {'house':committee['house'], 'cid':committee['cid'],
            'state':committee['state'], 'year':YEAR})
        query = dddb.fetchall()
        for member in query:
            isCurrent = False
            for comMember in committee['members']:
                if member[0] == comMember['pid']:
                    isCurrent = True
            if isCurrent == False:
                mem = dict()
                mem['current_flag'] = 0
                mem['end_date'] = dt.datetime.today().strftime("%Y-%m-%d")
                mem['pid'] = member[0]
                mem['cid'] = committee['cid']
                mem['house'] = "Assembly"
                mem['year'] = YEAR
                mem['state'] = STATE
                update_members.append(mem)
    except:
        logger.warning("Select query failed", full_msg = traceback.format_exc(),
         additional_fields = create_payload("servesOn", (select_current_members % {'house':committee['house'],
         'cid':committee['cid'], 'state':committee['state'], 'year':YEAR})))

    return update_members

def add_committees_db(dddb):
    global C_INSERTED, S_INSERTED, S_UPDATED
    committees = get_committees_html()
    count = 0
    y = 0
    
    for committee in committees:       
        cid = get_last_cid_db(dddb) + 1      
        get_cid = is_comm_in_db(committee, dddb)
        
        if  get_cid == False:
            committee['cid'] = str(cid)
            count = count + 1
            try:
              dddb.execute(insert_committee, {'cid':committee['cid'], 
                'house':committee['house'], 'name':committee['name'], 'state':committee['state']})
              C_INSERTED += dddb.rowcount
            except MySQLdb.Error:
              logger.warning('Insert Failed', full_msg=traceback.format_exc(),        
                    additional_fields=create_payload('Committee', (insert_committee % committee)))
        else:
            committee['cid'] = get_cid[0]          

        if len(committee['members']) > 0:
        
            for member in committee['members']:
                member['pid'] = get_pid_db(member, dddb)
                member['cid'] = committee['cid']
                if is_serveson_in_db(member, dddb) == False:                
                    if member['pid'] != None:
                      member['current_flag'] = '1'
                      member['start_date'] = dt.datetime.today().strftime("%Y-%m-%d")
                      try:
                        dddb.execute(insert_serveson, member)
                        S_INSERTED += dddb.rowcount
                      except MySQLdb.Error:
                        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                            additional_fields=create_payload('servesOn', (insert_serveson % member)))
                      y = y + 1

            updatedMems = get_past_members(committee, dddb)
            
            if len(updatedMems) > 0:
                for member in updatedMems:
                    try:
                        dddb.execute(update_serveson, member)
                        S_UPDATED += dddb.rowcount
                    except MySQLdb.Error:
                        logger.warning('Update Failed', full_msg = traceback.format_exc())
    #print "Inserted %d committees and %d members" % (count, y)
                

def get_pid_db(person, dddb):
    try:
        dddb.execute(select_person, person)
        query = dddb.fetchone()
        return query[0]
    except:
        print "Person not found: ", (select_person %  person)
        return None

    
def main():
    ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=ddinfo['host'],
                    user=ddinfo['user'],
                    db=ddinfo['db'],
                    port=ddinfo['port'],
                    passwd=ddinfo['passwd']) as dddb:
      add_committees_db(dddb)
      logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(C_INSERTED) + ' rows in Committee and inserted ' 
                    + str(S_INSERTED) + ' rows in servesOn',
          additional_fields={'_affected_rows':'Committee:'+str(C_INSERTED)+
                                         ', servesOn:'+str(S_INSERTED + S_UPDATED),
                             '_inserted':'Committee:'+str(C_INSERTED)+
                                         ', servesOn:'+str(S_INSERTED),
                             '_updated':'servesOn:'+str(S_UPDATED),
                             '_state':'NY'})

if __name__ == '__main__':                                                      
    with GrayLogger(GRAY_URL) as _logger:                                         
          logger = _logger
          main()
