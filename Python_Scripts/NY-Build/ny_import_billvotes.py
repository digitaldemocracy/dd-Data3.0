'''
File: import_billvotes_ny.py
Author: John Alkire
Date: 1/22/2016
Description:
- Imports NY bill vote using senate API
- Fills BillVoteDetail and BillVoteSummary
- Currently configured to test DB
'''
import requests
import MySQLdb
import sys
from bs4 import BeautifulSoup


insert_billvotedetail = '''INSERT INTO BillVoteDetail
                        (pid,voteId,result,state)
                        VALUES
                        (%(pid)s,%(voteId)s,%(result)s,%(state)s);
                        '''
                        
insert_billvotesummary = '''INSERT INTO BillVoteSummary
                        (bid,cid,VoteDate,ayes,naes,abstain, result)
                        VALUES
                        (%(bid)s,%(cid)s,%(VoteDate)s,%(ayes)s,%(naes)s,%(abstain)s, %(result)s);
                        '''
                                                
select_person = '''SELECT * 
                    FROM Person p, Legislator l, Term t
                    WHERE last like %(last)s AND l.state = %(state)s
                    AND p.pid = l.pid AND t.pid = l.pid AND t.house = %(house)s 
                '''      

select_committee = '''SELECT cid 
                    FROM Committee
                    WHERE house = %(house)s and name = %(name)s and
                    state = %(state)s'''       

select_billvotesummary = '''SELECT voteId 
                        FROM BillVoteSummary
                        WHERE bid = %(bid)s and VoteDate = %(VoteDate)s 
                        '''
                    
select_billvotedetail = '''SELECT voteId 
                            FROM BillVoteDetail
                            WHERE voteId = %(voteId)s and pid = %(pid)s 
                        '''                                                            
API_YEAR = 2016
                        
dddb_conn =  MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                user='awsDB',
                db='JohnTest',
                port=3306,
                passwd='digitaldemocracy789')
dddb = dddb_conn.cursor()
dddb_conn.autocommit(True)

voteToResult = {'N': 'NOE', 'Y':'AYE', 'E':'ABS',  'A':'AYE'}

def call_senate_api(restCall, house, offset):
    if house != "":
        house = "/" + house
    url = "http://legislation.nysenate.gov/api/3/" + restCall + "/" + str(API_YEAR) + house + "?full=true&limit=100&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset=" + str(offset)
    r = requests.get(url)

    out = r.json()
    return (out["result"]["items"], out['total'])

def clean_name(name):

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
        
    return (first, last)
    
def get_comm_cid(comm):   
    dddb.execute(select_committee, comm)
    query = dddb.fetchone()
            
    if query is None:                   
        raise Exception('No CID found')   
    
    return query[0]
    
def get_bills_api(year):
    total = 1001
    cur_offset = 1000
    ret_bills = list()
    
    while cur_offset < total:

        call = call_senate_api("bills", "", cur_offset)
        bills = call[0]
        total = call[1]
        for bill in bills:             
            b = dict()
            sys.stdout.write('\nGot bill...')
            b['number'] = bill['basePrintNo'][1:]
            b['house'] = bill['billType']['chamber'].title()
            b['bid'] = "NY_" + str(year) + str(year+1) + '0' + bill['basePrintNo']
            if b['house'] == 'Assembly':
                sys.stdout.write('checking assembly...')
                b['votes'] = get_vote_sums_assem(b['bid'], bill['basePrintNo'], year)
            elif len(bill['votes']['items']) > 0:
                sys.stdout.write('has senate vote...')
                b['votes'] = get_vote_sums_senate(b, bill['votes']['items'])
            else:
                b['votes'] = list()
            if len(b['votes']) > 0:
                ret_bills.append(b)        
                sys.stdout.write('found %d votes...' % len(b['votes']))      
        cur_offset += total #1000
    print "Downloaded %d bills..." % len(ret_bills)    
    return ret_bills
    
def get_vote_sums_senate(bill, vote_items):
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
        bv['votes'] = get_vote_details_sen(billvote['memberVotes']['items'], bv)
        try:
            ayes = billvote['memberVotes']['items']['AYE']['size']                                                
        except:
            ayes = 0
        try:
            ayewrs= billvote['memberVotes']['items']['AYEWR']['size'] 
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
        bv['cid'] = get_comm_cid(bv)
          
        if int(bv['naes']) < int(bv['ayes']):
            bv['result'] = '(PASS)'
        else:
            bv['result'] = '(FAIL)'
        
        ret_votes.append(bv)
    return ret_votes
    
def get_vote_details_sen(vote_items, bv):
    ret_votes = list()
    
    for key, value in vote_items.items():      
        for person in value['items']:
            bvd = dict()
            name = clean_name(person['fullName'])
            bvd['last'] = name[1]
            bvd['state'] = 'NY'
            bvd['bid'] = bv['bid']
            bvd['house'] = bv['house']           
            bvd['result'] = voteToResult[str(key[0:1])]         
            bvd['pid'] = get_pid_db(bvd)
            ret_votes.append(bvd)
    
    return ret_votes
           
def get_vote_sums_assem(bid, bill, year):
    ret_arr = list()
    url = 'http://assembly.state.ny.us/leg/?default_fld=&bn={0}&term={0}&Votes=Y'.format(bill, year)
    page = requests.get(url)

    soup = BeautifulSoup(page.content, 'html.parser')
    
    for table in soup.find_all('table'):
         
        bv = dict()
        bv['bid'] = bid;
        bv['VoteDate'] = str(table.find('caption').find_all('span')[1].string) 
        tally = table.find('caption').find(style="float-right").find('span').string.split('/')
        bv['state'] = 'NY'
        bv['name'] = 'Assembly Floor'
        bv['house'] = 'Assembly'
        names = table.find_all('td')
        votes = dict() 
        for x in range(0, len(names), 2):
            if names[x].string != None:
                if names[x].string == 'Mr Spkr':
                    votes[speaker] = str(names[x+1].string)
                else:
                    votes[str(names[x].string)] = str(names[x+1].string)
                
        vote_details = get_vote_details_assem(bid,votes)
        bv['votes'] = vote_details[0]
        bv['ayes'] = vote_details[1]
        bv['naes'] = vote_details[2]
        bv['abstain'] = vote_details[3]
        bv['cid'] = get_comm_cid(bv)
        
        if int(bv['naes']) < int(bv['ayes']):
            
            bv['result'] = '(PASS)'
        else:
            bv['result'] = '(FAIL)'
        ret_arr.append(bv)
        
    return ret_arr
                        
def get_vote_details_assem(bid, votes):    
    ret_list = list()
    y = n = a = 0
    for k, v in votes.items():
        bvd = dict()
        bvd['last'] = k + '%'
        bvd['state'] = 'NY'
        bvd['bid'] = bid
        bvd['house'] = 'Assembly'
        bvd['result'] = voteToResult[v[0:1]]
        bvd['pid'] = get_pid_db(bvd)
        if v[0:1] == 'Y':
            y = y + 1
        elif v[0:1] == 'N':
            n = n + 1
        else:
            a = a + 1
        ret_list.append(bvd)
        
    return ret_list, y, n, a

def get_pid_db(person):              
    try:
        dddb.execute(select_person, person)
        query = dddb.fetchone()
        return query[0]
    except:
        print "Person not found: ", (select_person %  person)
        return "bad"

def get_speaker_name():
    page = requests.get('http://assembly.state.ny.us/mem/leadership/')
    soup = BeautifulSoup(page.content, 'html.parser')

    for s in soup.find_all('strong'):
        if s.string == 'Speaker':
            speaker = s.parent.find(target='blank').string

    sl =  speaker.split()        
    return sl[len(sl) - 1]
    
def is_billvotesum_in_db(bv):
                                                    
    try:
        dddb.execute(select_billvotesummary, bv)
        query = dddb.fetchone()
             
        if query is None:                   
            return False       
    except:            
        return False
    return query
    
def is_bvd_in_db(bvd):
            
    try:
        dddb.execute(select_billvotedetail, bvd)
        query = dddb.fetchone()
             
        if query is None:                   
            return False       
    except:            
        return False
    return True
    
def insert_bvd_db(votes, voteId):
    for bvd in votes:
            bvd['voteId'] = voteId            
            if not is_bvd_in_db(bvd):
                print insert_billvotedetail % bvd
                dddb.execute(insert_billvotedetail, bvd)


def insert_billvotesums_db(bills):
    for bill in bills:
        for bv in bill['votes']:
            voteId = is_billvotesum_in_db(bv)
            if not voteId:
                
                print insert_billvotesummary % bv
                dddb.execute(insert_billvotesummary, bv)                
                voteId = dddb.lastrowid
                insert_bvd_db(bv['votes'], voteId)

speaker = get_speaker_name()
insert_billvotesums_db(get_bills_api())
