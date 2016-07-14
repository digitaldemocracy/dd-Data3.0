#!/usr/bin/env python2.6
# -*- coding: utf8 -*-

'''
File: ny_import_billvotes.py
Author: John Alkire
Maintained: Miguel Aguilar
Date: 1/22/2016
Last Updated: 07/13/2016
Description:
- Imports NY bill vote data using the senate API and by scraping the NY assembly page
- Fills BillVoteDetail and BillVoteSummary
- Currently configured to test DB
'''

import requests
import MySQLdb
import sys
import traceback
from datetime import datetime
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None

insert_billvotedetail = '''INSERT INTO BillVoteDetail
                            (pid,voteId,result,state)
                           VALUES
                            (%(pid)s,%(voteId)s,%(result)s,%(state)s);'''
                        
insert_billvotesummary = '''INSERT INTO BillVoteSummary
                             (bid,cid,VoteDate,VoteDateSeq,ayes,naes,abstain, result)
                            VALUES
                             (%(bid)s,%(cid)s,%(VoteDate)s,%(VoteDateSeq)s,%(ayes)s,%(naes)s,%(abstain)s, %(result)s);'''
                                                
select_person = '''SELECT * 
                   FROM Person p, Legislator l, Term t
                   WHERE last like %(last)s
                    AND l.state = %(state)s
                    AND p.pid = l.pid
                    AND t.pid = l.pid 
                    AND t.house = %(house)s'''      

select_committee = '''SELECT cid 
                      FROM Committee
                      WHERE house = %(house)s 
                       AND name = %(name)s 
                       AND state = %(state)s'''

select_committee_2 = '''SELECT name 
                      FROM Committee
                      WHERE house = %s  
                       AND state = %s
                       AND name like %s'''        

select_billvotesummary = '''SELECT voteId 
                            FROM BillVoteSummary
                            WHERE bid = %(bid)s 
                             AND VoteDate = %(VoteDate)s'''
                    
select_billvotedetail = '''SELECT voteId 
                           FROM BillVoteDetail
                           WHERE voteId = %(voteId)s 
                            AND pid = %(pid)s'''                        
                                                                
API_YEAR = 2016
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"
ASSEMBLY_URL = 'http://assembly.state.ny.us/leg/?default_fld=&leg_video=&bn={0}&term={1}&Committee%26nbspVotes=Y&Floor%26nbspVotes=Y'
BILL_API_INCREMENT = 1000
STATE = 'NY'
voteToResult = {'N': 'NOE', 'Y':'AYE', 'E':'ABS',  'A':'AYE'}


def create_payload(table, sqlstmt):                                             
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'NY'
    }


def call_senate_api(restCall, house, offset):
    if house != "":
        house = "/" + house
    url = API_URL.format(restCall, API_YEAR, house, offset)
    r = requests.get(url)

    out = r.json()
    return (out["result"]["items"], out['total'])


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
    }
    
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()      
    suffix = "";         

    if len(name_arr) == 1 and name_arr[0] in problem_names.keys():
      name_arr = list(problem_names[name_arr[0]])

          
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
    
def get_comm_cid(dddb, comm):
    try:
        dddb.execute(select_committee, comm)
    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('Committee',(select_committee%comm)))

    query = dddb.fetchone()
           
    if query is None:              
        raise Exception('No CID found')   
    
    return query[0]
    
def get_bills_api(dddb):
    total = BILL_API_INCREMENT
    cur_offset = 0
    ret_bills = list()
    
    while cur_offset < total:
        print "Current Bill Offset: %d" % cur_offset
        call = call_senate_api("bills", "", cur_offset)
        bills = call[0]
        total = call[1]
        
        for bill in bills:             
            b = dict()
            b['number'] = bill['basePrintNo'][1:]
            b['house'] = bill['billType']['chamber'].title()
            b['bid'] = STATE + "_" + str(bill['session']) + str(int(bill['session'])+1) + '0' + bill['basePrintNo']
            
            if b['house'] == 'Assembly':
                b['votes'] = get_vote_sums_assem(dddb, b['bid'], bill['basePrintNo'])
            elif len(bill['votes']['items']) > 0:
                b['votes'] = get_vote_sums_senate(dddb, b, bill['votes']['items'])
            else:
                b['votes'] = list()
           
            if len(b['votes']) > 0:
                ret_bills.append(b)
        
        cur_offset += BILL_API_INCREMENT
    
    print "Downloaded %d bills with votes..." % len(ret_bills)    
    return ret_bills

    
def get_vote_sums_senate(dddb, bill, vote_items):
    ret_votes = list()
    
    for billvote in vote_items:
        bv = dict()
        bv['bid'] = bill['bid']
        bv['state'] = 'NY'
        bv['house'] = bill['house']
    
        if billvote['voteType'] == "COMMITTEE":
            bv['name'] = billvote['committee']['name']
        else:
            bv['name'] =  bv['house'] + ' Floor'
    
        bv['VoteDate'] = billvote['voteDate']
        bv['VoteDateSeq'] = 0
        bv['votes'] = get_vote_details_sen(dddb, billvote['memberVotes']['items'], bv)
    
        try:
            ayes = billvote['memberVotes']['items']['AYE']['size']                                                
        except:
            ayes = 0
    
        try:
            ayewrs = billvote['memberVotes']['items']['AYEWR']['size'] 
        except:
            ayewrs = 0
            
        bv['ayes'] = ayes + ayewrs
    
        try:
            bv['naes'] = billvote['memberVotes']['items']['NAY']['size']
        except:
            bv['naes'] = 0
    
        try:
            bv['abstain'] = billvote['memberVotes']['items']['EXC']['size']
        except:
            bv['abstain'] = 0
      
        bv['cid'] = get_comm_cid(dddb, bv)
          
        if int(bv['naes']) < int(bv['ayes']):
            bv['result'] = '(PASS)'
        else:
            bv['result'] = '(FAIL)'
        
        ret_votes.append(bv)
        
    return ret_votes
    
def get_vote_details_sen(dddb, vote_items, bv):
    ret_votes = list()
    
    for key, value in vote_items.items():      
      
        for person in value['items']:
            bvd = dict()
            name = clean_name(person['fullName'])
            bvd['last'] = name[1]
            bvd['state'] = STATE
            bvd['bid'] = bv['bid']
            bvd['house'] = bv['house']           
            bvd['result'] = voteToResult[str(key[0:1])]         
            bvd['pid'] = get_pid_db(dddb, bvd)
            ret_votes.append(bvd)
    
    return ret_votes
           

def get_db_name(dddb, name):
    comm = {'name':name, 'state':'NY', 'house':'Assembly'}
    dddb.execute(select_committee_2, ('Assembly', 'NY', '%'+'%'.join(name.split())+'%'))
    query = dddb.fetchone()

    if query is None:              
        raise Exception('No Name found')

    return query[0]

def get_vote_sums_assem(dddb, bid, bill):
    ret_arr = list()
    api_year = 2015
    url = ASSEMBLY_URL.format(bill, api_year)
    page = requests.get(url)

    soup = BeautifulSoup(page.content, 'html5lib')
    
    for table in soup.find_all('table'):         
        bv = dict()
        bv['bid'] = bid;
        if str(table.find('caption').find_all('span')[0].string) == 'DATE:':
            bv['VoteDate'] = datetime.strptime(str(table.find('caption').find_all('span')[1].string), '%m/%d/%Y')
            bv['name'] = 'Assembly Floor'
        else:
            bv['VoteDate'] = datetime.strptime(str(table.find('caption').find_all('span')[3].string), '%m/%d/%Y')
            comm_name = str(table.find('caption').find_all('span')[1].string).split('   ')[0].lower()
            comm_name = ' '.join([word[0].upper() + word[1:] if word not in ['and', 'on', 'in', 'to', '&', 'of', 'with', 'the'] \
                            else word for word in comm_name.split()])
            if '-' in comm_name:
                ndx = comm_name.index('-')+1
                comm_name = ''.join([comm_name[:ndx], comm_name[ndx].upper(), comm_name[ndx + 1:]])
            if '/' in comm_name:
                ndx = comm_name.index('/')+1
                comm_name = ''.join([comm_name[:ndx], comm_name[ndx].upper(), comm_name[ndx + 1:]])

            c_name = get_db_name(dddb,comm_name)
            bv['name'] = c_name

        bv['VoteDateSeq'] = 0
        bv['state'] = STATE
        bv['house'] = 'Assembly'

        names = table.find_all('td')
        votes = dict() 
      
        for x in range(0, len(names), 2):
            if names[x].string != None:
                if names[x].string == 'Mr Spkr':
                    votes[speaker] = str(names[x+1].string)
                else:
                    votes[str(names[x].string)] = str(names[x+1].string)
                
        vote_details = get_vote_details_assem(dddb,bid,votes)
        bv['votes'] = vote_details[0]
        bv['ayes'] = vote_details[1]
        bv['naes'] = vote_details[2]
        bv['abstain'] = vote_details[3]
        bv['cid'] = get_comm_cid(dddb, bv)
        
        if int(bv['naes']) < int(bv['ayes']):            
            bv['result'] = '(PASS)'
        else:
            bv['result'] = '(FAIL)'
        
        ret_arr.append(bv)
        
    return ret_arr
                        
def get_vote_details_assem(dddb, bid, votes):    
    ret_list = list()
    y = n = a = 0
    
    for k, v in votes.items():
        bvd = dict()
        bvd['last'] = k + '%'
        bvd['state'] = STATE
        bvd['bid'] = bid
        bvd['house'] = 'Assembly'
        bvd['result'] = voteToResult[v[0:1]]
        bvd['pid'] = get_pid_db(dddb, bvd)
        
        if v[0:1] == 'Y' or v == 'Aye':
            y = y + 1
        elif v[0:1] == 'N' or v == 'Nay':
            n = n + 1
        else:
            a = a + 1
            
        ret_list.append(bvd)
        
    return ret_list, y, n, a

def get_pid_db(dddb, person):
    try:
        dddb.execute(select_person, person)
        query = dddb.fetchone()
        return query[0]
    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('Person',(select_person%person)))
        print "Person not found: ", (select_person %  person)
        return None

def get_speaker_name():
    page = requests.get('http://assembly.state.ny.us/mem/leadership/')
    soup = BeautifulSoup(page.content, 'html.parser')

    for s in soup.find_all('strong'):
        if s.string == 'Speaker':
            speaker = s.parent.find(target='blank').string

    sl =  speaker.split()        
    return sl[len(sl) - 1]
    
def is_billvotesum_in_db(dddb, bv):                                                    
    try:
        dddb.execute(select_billvotesummary, bv)
        query = dddb.fetchone()
        if query is None:                   
            return False 
        return query
    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('BillVoteSummary',(select_billvotesummary, bv)))
        return False
    
def is_bvd_in_db(dddb, bvd):
    try:
        dddb.execute(select_billvotedetail, bvd)
        query = dddb.fetchone()
        if query is None:                   
            return False 
        return True
    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('BillVoteDetail',(select_billvotedetail%bvd)))
        return False

def insert_bvd_db(dddb, votes, voteId, none_count):
    for bvd in votes:
            bvd['voteId'] = voteId            
           
            if not is_bvd_in_db(dddb, bvd) and bvd['pid'] is not None:
                try:
                    dddb.execute(insert_billvotedetail, bvd)
                except MySQLdb.Error:
                    logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload('BillVoteDetail',(insert_billvotedetail%bvd)))

            if bvd['pid'] is None:
                none_count = none_count + 1
    return none_count

def insert_billvotesums_db(dddb, bills):
    none_count = 0
    sum_count = 0

    for bill in bills:
        for bv in bill['votes']:
            voteId = is_billvotesum_in_db(dddb, bv)
        
            if not voteId:                
                try:
                    dddb.execute(insert_billvotesummary, bv)
                    sum_count = sum_count + 1
                except MySQLdb.Error:
                    logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload('BillVoteSummary',(insert_billvotesummary%bv)))

                voteId = dddb.lastrowid
                none_count = insert_bvd_db(dddb, bv['votes'], voteId, none_count)

    #print('Number of invalid inserts: ', none_count)
    print "Number of billvote summary and details inserted: %d" % sum_count


speaker = get_speaker_name()
def main():
    with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='DDDB2015Dec',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
        insert_billvotesums_db(dddb, get_bills_api(dddb))   

if __name__ == '__main__':
    with GrayLogger(GRAY_URL) as _logger:
        logger = _logger
        main()
