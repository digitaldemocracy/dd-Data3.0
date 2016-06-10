# -*- coding: utf8 -*-
'''
File: import_lobbyists_ny.py
Author: John Alkire
Modified: 5/20/2016
Description:
- Imports NY lobbyist data using NY API (https://data.ny.gov/Transparency/Registered-Lobbyist-Disclosures-Beginning-2007/djsm-9cw7)
- Fills Lobbyist, LobbyingFirm
- Note that there is not filer ID in the NY data, which means LobbyingFirmState, LobbyistEmployment, LobbyistEmployer
  cannot be filled. We need to decide on a method to either create filer IDs or alter the schema. 
'''
import requests
import MySQLdb

insert_person = '''INSERT INTO Person
                (last, first)
                VALUES
                (%(last)s, %(first)s);'''

insert_lobbyist = '''INSERT INTO Lobbyist
                (pid, state)
                VALUES
                (%(pid)s, %(state)s);'''
                
insert_lobbyingfirmstate = '''INSERT INTO LobbyingFirmState
                        (filer_id, filer_naml, state)
                        VALUES
                        (%(filer_id)s, %(filer_naml)s, %(state)s);'''

insert_lobbyingfirm = '''INSERT INTO LobbyingFirm
                    (filer_naml)
                    VALUES
                    (%(filer_naml)s);'''    
                    
select_lobbyist = '''SELECT p.pid 
                     FROM Person p, Lobbyist l
                     WHERE p.first = %(first)s AND p.last = %(last)s
                      AND p.pid = l.pid'''

select_lobbyingfirm = '''SELECT filer_naml
                         FROM LobbyingFirm
                         WHERE filer_naml = %(filer_naml)s'''
                                               
dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                            user='awsDB',
                            db='JohnTest',
                            port=3306,
                            passwd='digitaldemocracy789',
                            charset='utf8')
dddb = dddb_conn.cursor()
dddb_conn.autocommit(True)                                                
name_checks = ['(', '\\' ,'/', 'OFFICE', 'LLC']


def clean_name(name):
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV', 'SR':', Sr.', 'JR':', Jr.'}
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
        
def get_names(names):
    names = names.replace(', III', ' III')
    names = names.replace(', MD', '')
    names = names.replace(', DR.', '')
    names = names.replace(", JR", " JR")
    names = names.replace(", SR", " SR")
    names = names.replace("NULL", "")
    names = names.replace(', ESQ.', '')
    ret_names = list()
    for per in names.split(','):
        if len(per) > 4:            
            ret_names.append(per)
    return ret_names

def call_lobbyist_api():
    url = 'https://data.ny.gov/resource/mbmr-kxth.json?$limit=50000'
    r = requests.get(url)
    lobbyists_api = r.json()
    return lobbyists_api
    
def get_lobbyists_api(lobbyists_api):
    lobbyists = dict()
    for lbyst in lobbyists_api:    
         
        if lbyst['lobbyist_name'] in lobbyists:
            if not lobbyist['person']: 
                if 'additional_lobbyists_lr' in lbyst:
                    lobbyist['lobbyists'] += get_names(lbyst['additional_lobbyists_lr'])
                if 'additional_lobbyists_lbr' in lbyst:
                    lobbyist['lobbyists'] += get_names(lbyst['additional_lobbyists_lbr'])
                lobbyist['lobbyists'] = list(set(lobbyist['lobbyists']))
        else:
            lobbyist = dict()
            lobbyist['person'] = False
            try:
                if lbyst['additional_lobbyists_lr'] == 'NULL' and lbyst['additional_lobbyists_lbr'] ==  'NULL' and \
                lbyst['lr_responsible_party_first_name'] in lbyst['lobbyist_name'] and lbyst['lr_responsible_party_last_name'] in lbyst['lobbyist_name']:
                    cont = True
                    for name in name_checks:
                        if name in lbyst['lobbyist_name']:
                            cont = False         
                    if cont:
                        lobbyist['person'] = True                        
                        lobbyist['first'] = lbyst['lr_responsible_party_first_name']
                        lobbyist['last'] = lbyst['lr_responsible_party_last_name']                      
            except:
                pass                                
            
            lobbyist['filer_naml'] = lbyst['lobbyist_name']
            lobbyist['state'] = 'NY'
            
            if not lobbyist['person']:
                lobbyist['lobbyists'] = list()
                if 'additional_lobbyists_lr' in lbyst:
                    lobbyist['lobbyists'] += get_names(lbyst['additional_lobbyists_lr'])
                if 'additional_lobbyists_lbr' in lbyst:
                    lobbyist['lobbyists'] += get_names(lbyst['additional_lobbyists_lbr'])
                lobbyist['lobbyists'] = list(set(lobbyist['lobbyists']))
                        
            lobbyists[lbyst['lobbyist_name']] = lobbyist            
    
    return lobbyists
    
def is_lobbyist_in_db(lobbyist):
    dddb.execute(select_lobbyist, lobbyist)
    query = dddb.fetchone()
    
    if query is None:            
        return False       

    return True
    
def is_lobbyingfirm_in_db(lobbyist):
    dddb.execute(select_lobbyingfirm, lobbyist)
    query = dddb.fetchone()
    
    if query is None:            
        return False       

    return True 
       
def insert_lobbyist_db(lobbyist):
    if not is_lobbyist_in_db(lobbyist):
        
        dddb.execute(insert_person, lobbyist)
        pid = dddb.lastrowid   
        lobbyist['pid'] = pid
        dddb.execute(insert_lobbyist, lobbyist)  

def insert_lobbyingfirm_db(lobbyist):
    if not is_lobbyingfirm_in_db(lobbyist):
        dddb.execute(insert_lobbyingfirm, lobbyist)
        for person in lobbyist['lobbyists']:
            per = dict()
            try:
                name = clean_name(person)
                per['state'] = 'NY'
                per['first'] = name[0]
                per['last'] = name[1]
                insert_lobbyist_db(per)
            except:
                print name
    
def insert_lobbyists_db(lobbyists):
    for lobbyist in lobbyists.values():
        if lobbyist['person']:            
            insert_lobbyist_db(lobbyist)
        else:
            insert_lobbyingfirm_db(lobbyist)
            
def main():    
    lobbyists_api = call_lobbyist_api()
    lobbyists = get_lobbyists_api(lobbyists_api)
    insert_lobbyists_db(lobbyists)
    
main()