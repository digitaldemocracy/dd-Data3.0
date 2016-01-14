'''
File: import_committees_ny.py
Author: John Alkire
Date: 12/16/2015
Description:
- Imports NY committees by scraping assembly webpage
- Fills Committee and servesOn
- Currently configured to test DB
'''
from lxml import html
import requests
import MySQLdb

def get_last_cid_db(dddb):
	select_comm = '''SELECT cid FROM Committee
					ORDER BY cid DESC
					LIMIT 1
                  '''
	dddb.execute(select_comm)
	
	query = dddb.fetchone();
	return query[0]

def is_comm_in_db(comm, dddb):

    select_comm = '''SELECT cid 
                    FROM Committee
                    WHERE house = %(house)s and name = %(name)s and
                     state = %(state)s'''                                                                    
    try:
        dddb.execute(select_comm, comm)
        query = dddb.fetchone()
             
        if query is None:                   
            return False       
    except:            
        return False
    return query
    
def is_serveson_in_db(member, dddb):

    select_comm = '''SELECT pid 
                    FROM servesOn
                    WHERE pid = %(pid)s and year = %(year)s and 
                    house = %(house)s and cid = %(cid)s and state = %(state)s'''                                                                    
    try:
        dddb.execute(select_comm, member)
        query = dddb.fetchone()
        
        if query is None:
            #print select_comm % member            
            return False       
    except:            
        return False    
    
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
    
def get_committees_html(year):
    page = requests.get('http://assembly.state.ny.us/comm/')
    tree = html.fromstring(page.content)
    categories_html = tree.xpath('//*[@id="sitelinks"]/span//text()')
    ret_comms = list()
    committees = dict()
    x = 1
    positions = ["chair", "member"]
    count = 0;
    for category in categories_html:
        committees_html = tree.xpath('//*[@id="sitelinks"]//ul['+str(x)+']//li/strong/text()')        
        y = 1
        print category
        for comm in committees_html:                    
            link = tree.xpath('//*[@id="sitelinks"]//ul['+str(x)+']//li['+str(y)+']/a[contains(@href,"mem")]/@href')
            committee = dict()
            committee['name'] = comm
            committee['type'] = category
            committee['house'] = "Assembly"
            committee['state'] = "NY"
            committee['members'] = list()
            print "    "+comm
            
            if len(link) > 0:
                strip_link = link[0][0:len(link[0]) - 1]
            
                link = 'http://assembly.state.ny.us/comm/' + strip_link                                      
                
                member_page = requests.get(link)
                member_tree = html.fromstring(member_page.content)
                
                members_html = member_tree.xpath('//*[@id="sitelinks"]/span//li/a//text()')
                position = 0
                for mem in members_html:                    
                    sen = dict()
                    name = clean_name(mem)                        
                    sen['position'] = positions[position] 
                    sen['last'] = name[1]        
                    sen['first'] = name[0]                                        
                    sen['year'] = str(year)
                    sen['house'] = "Assembly"
                    sen['state'] = "NY"
                    committee['members'].append(sen)
                    position = 1
            
            count = count + 1
            ret_comms.append(committee)    
            y = y + 1
        x = x + 1   

    return ret_comms                 

def add_committees_db(year, dddb):
    committees = get_committees_html(year)

    count = 0
    for committee in committees:       
        cid = get_last_cid_db(dddb) + 1      
        get_cid = is_comm_in_db(committee, dddb)
        if  get_cid == False:

            committee['cid'] = str(cid)
            insert_stmt = '''INSERT INTO Committee
                            (cid, house, name, state)
                            VALUES
                            (%(cid)s, %(house)s, %(name)s, %(state)s);
                            '''
            count = count + 1
            print committee['name']
            #print (insert_stmt % committee)
            dddb.execute(insert_stmt, committee)
        else:
            committee['cid'] = get_cid[0]          

        if len(committee['members']) > 0:
            for member in committee['members']:
                member['pid'] = get_pid_db(member, dddb)
                member['cid'] = committee['cid']
                if is_serveson_in_db(member, dddb) == False:                
                    insert_stmt = '''INSERT INTO servesOn
                                (pid, year, house, cid, state, position)
                                VALUES
                                (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s, %(position)s);
                                '''                               
                    if member['pid'] != "bad":
                        dddb.execute(insert_stmt, member)
                        
    print "Final: " + str(count)
                

def get_pid_db(person, dddb):
    select_person = '''SELECT * 
                       FROM Person p, Legislator l
                       WHERE first = %(first)s AND last = %(last)s AND state = %(state)s
                       AND p.pid = l.pid'''                                           
    
    try:
        dddb.execute(select_person, person)
        query = dddb.fetchone()
        return query[0]
    except:
        print "Error", (select_person %  person)
        return "bad"

    
def main():
    dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                    user='awsDB',
                    db='JohnTest',
                    port=3306,
                    passwd='digitaldemocracy789')
    dddb = dddb_conn.cursor()
    dddb_conn.autocommit(True)

    add_committees_db(2015, dddb)

    dddb_conn.close()

main()