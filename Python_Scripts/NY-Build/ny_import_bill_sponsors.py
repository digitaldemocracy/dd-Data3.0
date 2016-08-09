#!/usr/bin/env python
# -*- coding: utf8 -*-
'''
File: import_authors_ny.py
Author: Min Eric Roh
Date: 12/26/2015
Description:
- Imports NY authors using senate API
- Fills authors
- Needs Bill, BillVersion, Person tables to be filled first
- Currently configured to test DB
'''

import traceback
import requests
import MySQLdb
from graylogger.graylogger import GrayLogger                                    
GRAY_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None
logged_list = list()
INSERTED = 0

counter = 0

US_STATE = 'NY'

# URL
URL = ('http://legislation.nysenate.gov/api/3/%(restCall)s/%(year)s%(house)s' +
    '?full=true&limit=1000&key=IhV5AXQ1rhUS8ePXkfwsO4AvjQSodd4Q&offset=%(offset)s')

# INSERTS
QI_BILLSPONSORS = '''INSERT INTO BillSponsors (pid, bid, vid, contribution)
                   VALUES (%s, %s, %s, %s)'''
QI_BILLSPONSORROLLS = '''INSERT INTO BillSponsorRolls (roll)
                     VALUES (%s)'''
# SELECTS
QS_AUTHORS_CHECK = '''  SELECT pid
            FROM authors
            WHERE bid = %(bid)s
             AND vid = %(vid)s
             AND contribution = %(contribution)s'''
QS_PERSON = ''' SELECT pid
              FROM Person
              WHERE last = %s
               AND first = %s
              ORDER BY Person.pid'''
QS_BILL = ''' SELECT * FROM Bill
        WHERE bid = %s'''
QS_BILLVERSION = '''  SELECT * FROM BillVersion
            WHERE vid = %s'''
QS_BILLSPONSORS_CHECK = '''SELECT *
                         FROM BillSponsors
                         WHERE bid = %s
                          AND pid = %s
                          AND vid = %s
                          AND contribution = %s'''
QS_BILLSPONSORROLL_CHECK = '''SELECT *
                              FROM BillSponsorRolls
                              WHERE roll = %s'''
# UPDATE
QU_AUTHORS = '''  UPDATE authors
          SET pid = %(pid)s
          WHERE bid = %(bid)s
           AND vid = %(vid)s
           AND contribution = %(contribution)s'''

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'NY'
  }

def call_senate_api(restCall, year, house, offset):
  if house != "":
    house = "/" + house
  url = URL % {'restCall':restCall, 'year':str(year), 'house':house, 'offset':str(offset)}
  r = requests.get(url)
  print url
  out = r.json()
  return (out["result"]["items"], out['total'])

def get_author_api(year):
  total = 1000
  cur_offset = 1
  ret_bills = list()

  while cur_offset < total:
    call = call_senate_api("bills", year, "", cur_offset)
    bills = call[0]
    total = call[1]
    for bill in bills:
      if bill['sponsor']['member'] is not None:
        b = dict()
        b['type'] = bill['basePrintNo']
#       print b['type']
        b['session'] = '0'
        b['versions'] = bill['amendments']['items']
        b['bid'] = "NY_" + str(year) + str(year + 1) + b['session'] + b['type']
#       for versions in bill['amendments']['items'].values():
#         if versions['coSponsors']['size'] > 0:
#           for sponsors in versions['coSponsors']['items']:
#             print sponsors['fullName'].encode('utf8')
#             fullName = sponsors['fullName'].encode('utf8')
#             name = clean_name(fullName)
#             b['last'] = name[1]
#             b['first'] = name[0]
        ret_bills.append(b)
    cur_offset += 1000
  print len(ret_bills)
  return ret_bills


'''
If the BillSponsor for this bill is not in the DDDB, add BillSponsor.
If contribution is not in the DDDB then add.
|dd_cursor|: DDDB database cursor
|pid|: Person id
|bid|: Bill id
|vid|: Bill Version id
|contribution|: the person's contribution to the bill (ex: Lead Author)
'''
def add_sponsor(dd_cursor, pid, bid, vid, contribution):
  global counter
  dd_cursor.execute(QS_BILLSPONSORS_CHECK, (bid, pid, vid, contribution))

  if dd_cursor.rowcount == 0:
#    print pid, vid, contribution
    try:
      dd_cursor.execute(QI_BILLSPONSORS, (pid, bid, vid, contribution))
      INSERTED += dd_cursor.rowcount
      counter += 1
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('BillSponsors', 
            (QI_BILLSPONSORS % (pid, bid, vid, contribution))))

def insert_sponsors_db(bill, dddb, contribution):
# print type(bill['versions'])
# print type(bill['versions'].values()), bill['versions'].values()
  for version in bill['versions'].values():
    a = dict()
#   print version['version']
    a['vid'] = bill['bid'] + version['version']
    vid_check = check_vid_db(a['vid'], dddb)
    if version[contribution]['size'] > 0  and check_bid_db(bill['bid'], dddb) and vid_check:
      for sponsors in version[contribution]['items']:
        try:
          fullName = sponsors['fullName'].encode('utf8')
          name = clean_name(fullName)
          a['last'] = name[1]
          a['first'] = name[0]
#         print a['vid']
          pid = get_pid_db(a['first'], a['last'], dddb)
          if pid is not None:
            a['pid'] = pid
            a['bid'] = bill['bid']
    #       print a['vid']
            if counter % 1000 == 0 and counter != 0:
              print counter
            add_sponsor(dddb, a['pid'], a['bid'], a['vid'], contribution)
        except IndexError:
          full_name = sponsors['fullName'].encode('utf8')
          if full_name not in logged_list:
            logged_list.append(full_name)
            logger.warning('Problem with name ' + fullName,
                additional_fields={'_state':'NY'})
#     else:
#       print a['bid'], "already existing"
#   else:
#     print "fill Person, Bill table first"

def check_vid_db(vid, dddb):
  dddb.execute(QS_BILLVERSION, (vid,))
  if dddb.rowcount == 1:
    return True
  else:
    if vid not in logged_list:
      logged_list.append(vid)
      logger.warning('BillVersion not found ' + vid, 
          additional_fields={'_state':'NY'})
    return False 

def check_bid_db(bid, dddb):
  dddb.execute(QS_BILL, (bid,))
  if dddb.rowcount == 1:
    return True
  else:
    if bid not in logged_list:
      logged_list.append(bid)
      logger.warning('Bill not found ' + bid, additional_fields={'_state':'NY'})
    return False

def clean_name(name):
    problem_names = {
        "Inez Barron":("Charles", "Barron"), 
        "Philip Ramos":("Phil", "Ramos"), 
        "Thomas McKevitt":("Tom", "McKevitt"), 
        "Albert Stirpe":("Al","Stirpe"), 
        "Peter Abbate":("Peter","Abbate, Jr."),
#        "Sam Roberts":("Pamela","Hunter"),
        "Herman Farrell":("Herman", "Farrell, Jr."),
        "Fred Thiele":("Fred", "Thiele, Jr."),
#       "William Scarborough":("Alicia", "Hyndman"),
        "Robert Oaks":("Bob", "Oaks"),
        "Andrew Goodell":("Andy", "Goodell"),
        "Peter Rivera":("José", "Rivera"),
        "Addie Jenne Russell":("Addie","Russell"),
        "Kenneth Blankenbush":("Ken","Blankenbush"),
#        "Alec Brook-Krasny":("Pamela","Harris"),
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
#     print "word", word
        if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
            name_arr.remove(word)
            if word in ending.keys():
                suffix = ending[word]            
#    print name_arr        
    first = name_arr.pop(0)
#    print "first", first
    while len(name_arr) > 1:
        first = first + ' ' + name_arr.pop(0)            
    last = name_arr[0]
#    print "last", last
    last = last.replace(' ' ,'') + suffix
    
    if (first + ' ' + last) in problem_names.keys():             
        return problem_names[(first + ' ' + last)]
#    print "return"
    return (first, last)

def get_pid_db(first, last, dddb):
  dddb.execute(QS_PERSON, (last, first))
  if dddb.rowcount >= 1:
    ret = dddb.fetchone()[0]
    return ret
  else:
    if [first, last]not in logged_list:
      logged_list.append([first, last])
      logger.warning('Person not found ' + last + ', ' + first,
          additional_fields={'_state':'NY'})
    return None

def add_authors_db(year, dddb):

  bills = get_author_api(year)

  for bill in bills:
    for contribution in ('coSponsors', 'multiSponsors'):
      dddb.execute(QS_BILLSPONSORROLL_CHECK, (contribution,))

      if dddb.rowcount == 0:
        try:
          dddb.execute(QI_BILLSPONSORROLLS, (contribution,))
        except MySQLdb.Error:
          logger.warning('Insert Failed', full_msg=traceback.format_exc(),
              additional_fields=create_payload('BillSponsorRoll', 
                (QI_BILLSPONSORROLLS % (contribution,))))
      insert_sponsors_db(bill, dddb, contribution)

def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
            user='awsDB',
            db='DDDB2015Dec',
            port=3306,
            passwd='digitaldemocracy789',
            charset='utf8') as dddb:
#   dddb = dddb_conn.cursor()
#   dddb_conn.autocommit(True)
    add_authors_db(2015, dddb)
    logger.info(__file__ + ' terminated', 
        full_msg='inserted ' + str(INSERTED) + ' in BillSponsors',
        additional_fields={'_table':'BillSponsors',
                           '_affected_rows':'BillSponsors:'+str(INSERTED),
                           '_inserted':'BillSponsors:'+str(INSERTED),
                           '_state':'NY'})
    print counter
'''
#   call = call_senate_api("bills", 2015, "", 1)
#   bills = call[0]
#   for bill in bills:
#     print type(bill)
      for versions in bill['amendments']['items'].values():
#       print type(versions['coSponsors']), versions['coSponsors']
        if versions['coSponsors']['size'] > 0:
          for sponsors in versions['coSponsors']['items']:
            print sponsors['fullName'].encode('utf8')
        if versions['multiSponsors']['size'] > 0:
          for sponsors in versions['multiSponsors']['items']:
            print sponsors['fullName'].encode('utf8')'''
    
if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger
    main()
