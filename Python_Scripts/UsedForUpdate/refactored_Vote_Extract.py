#!/usr/bin/env python
'''
File: Vote_Extract.py
Authored By: Daniel Mangin
Modified By: Matt Versaggi
Date: 6/11/2015
Last Modified: 5/18/2016

Description:
- Gets vote data from capublic.bill_summary_vote into DDDB.BillVoteSummary
  and capublic.bill_detail_vote into DDDB.BillVoteDetail
- Used in daily update of DDDB

Sources:
  - Leginfo (capublic)
    - Pubinfo_2015.zip
    - Pubinfo_Mon.zip
    - Pubinfo_Tue.zip
    - Pubinfo_Wed.zip
    - Pubinfo_Thu.zip
    - Pubinfo_Fri.zip
    - Pubinfo_Sat.zip

  -capublic
    - bill_summary_vote_tbl
    - bill_detail_vote_tbl

Populates:
  - BillVoteSummary (bid, mid, cid, VoteDate, ayes, naes, abstain, result)
  - BillVoteDetail (pid, voteId, result, state)

'''

import traceback
import MySQLdb
import sys
from graylogger.graylogger import GrayLogger
API_URL = 'http://development.digitaldemocracy.org:12202/gelf'
logger = None
logged_list = list()

# U.S. State
STATE = 'CA'

# Names pulled from CAPublic database that are missing comma before 'and'
# (this is compared to the names that are scraped from the committee websites
# and placed in our DB by the refactored_Get_Committees_Web script).
#
# NOTE: If any of these names has > 1 'and' in them, there will be problems.
NAMES_MISSING_COMMA = ['Water, Parks and Wildlife', 
                       'Public Employees, Retirement and Social Security']
# Extraordinary Committees 
# 
# NOTE: I had planned on using these lists of extraordinary committees
# to identify votes that were for these committees specifically. However,
# there was a lot of overlap for both Rules and Appropriations (there exists
# standing committees for those in both houses as well), and we needed to
# get this script working quickly due to an immenent (temporary) shutdown
# of Digital Democracy
A_EXTRAORDINARY_I_COMMITTEES = ['Finance',
                                'Transportation and Infrastructure Development',
                                'Rules'
                               ]

A_EXTRAORDINARY_II_COMMITTEES = ['Finance',
                                 'Public Health and Developmental Services',
                                 'Rules'
                                ]

S_EXTRAORDINARY_I_COMMITTEES = ['Rules',
                               'Appropriations',
                               'Transportation and Infrastructure Development'
                               ]
S_EXTRAORDINARY_II_COMMITTEES = ['Rules',
                                'Appropriations',
                                'Public Health and Developmental Services'
                                ]

# Queries
QS_BILL_DETAIL = '''SELECT bill_id, location_code, legislator_name, 
                     vote_code, motion_id, trans_update
                    FROM bill_detail_vote_tbl'''
QS_BILL_SUMMARY = '''SELECT bill_id, location_code, motion_id, ayes, noes, 
                      abstain, vote_result, trans_update
                     FROM bill_summary_vote_tbl'''
QS_VOTE_DETAIL = '''SELECT pid, voteId
                    FROM BillVoteDetail 
                    WHERE pid = %(pid)s 
                     AND voteId = %(voteId)s
                     AND state = %(state)s'''
QS_VOTE_SUMMARY = '''SELECT bid, mid, VoteDate 
                     FROM BillVoteSummary 
                     WHERE bid = %(bid)s 
                      AND mid = %(mid)s 
                      AND VoteDate = %(vote_date)s'''   
QS_VOTE_ID = '''SELECT voteId 
                FROM BillVoteSummary 
                WHERE bid = %(bid)s 
                 AND mid = %(mid)s'''
QS_PERSON_FL = '''SELECT pid, last, first
                  FROM Person
                  WHERE last = %(filer_naml)s
                   AND first = %(filer_namf)s
                   AND pid < 130
                  ORDER BY Person.pid'''
QS_PERSON_L = '''SELECT pid, last, first
                  FROM Person
                  WHERE last = %(filer_naml)s
                   AND pid < 130
                  ORDER BY Person.pid'''
QS_PERSON_LIKE_L = '''SELECT pid, last, first
                      FROM Person
                      WHERE last LIKE %(filer_naml)s
                      ORDER BY Person.pid'''    
QS_TERM = '''SELECT pid, house
             FROM Term
             WHERE pid = %(pid)s
              AND house = %(house)s
              AND state = %(state)s'''
QS_LOCATION_CODE = '''SELECT description, long_description 
                      FROM location_code_tbl 
                      WHERE location_code = %(location_code)s'''
QS_COMMITTEE = '''SELECT cid 
                  FROM Committee 
                  WHERE name = %(name)s 
                   AND house = %(house)s
                   AND state = %(state)s'''
QI_SUMMARY = '''INSERT INTO BillVoteSummary
                 (bid, mid, cid, VoteDate, ayes, naes, abstain, result)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

QI_DETAIL =  '''INSERT INTO BillVoteDetail (pid, voteId, result, state) 
                VALUES (%s, %s, %s, %s)'''

def create_payload(table, sqlstmt):
  return {
    '_table': table,
    '_sqlstmt': sqlstmt,
    '_state': 'CA'
  }

'''
If committee is found, return cid. Otherwise, return None.
'''
def find_committee(cursor, name, house):
  cursor.execute(QS_COMMITTEE, {'name':name, 'house':house, 'state':STATE})
  if(cursor.rowcount == 1):
    return cursor.fetchone()[0]
  sys.stderr.write("WARNING: Unable to find committee {0}\n".format(name))
  return None

'''
Parses the committee to find name and house. If committee is found, return cid.
Otherwise, return None.
'''
def get_committee(ca_cursor, dd_cursor, location_code):
  ca_cursor.execute(QS_LOCATION_CODE, {'location_code':location_code})
  if(ca_cursor.rowcount > 0):
    loc_result = ca_cursor.fetchone()
    temp_name = loc_result[0]
    committee_name = loc_result[1]

    committee_name = clean_name(committee_name)

    if 'Asm' in temp_name or 'Assembly' in temp_name:
      house = 'Assembly'
    else:
      house = 'Senate'

    if 'Floor' in committee_name:
      name = '{0} Floor'.format(house)
    elif 'Transportation and Infrastructure Development' in committee_name:
      name = '{0} 1st Extraordinary Session on {1}'.format(house, committee_name)
    elif 'Public Health and Developmental Services' in committee_name:
      name = '{0} 2nd Extraordinary Session on {1}'.format(house, committee_name)
    elif 'Finance' in committee_name and house == 'Assembly':
      name = 'Assembly 1st Extraordinary Session on Finance'
    else:
      name = '{0} Standing Committee on {1}'.format(house, committee_name)

  return find_committee(dd_cursor, name, house)
    
'''
Handles all instances of reformatting and cleaning specific legislator and
committee names.
'''
def clean_name(name):
  # For de Leon
  temp = name.split('\xc3\xb3')
  if (len(temp) > 1):
    name = temp[0] + 'o' + temp[1];

  # For Travis Allen
  if (name == 'Allen Travis'):
    name = 'Travis Allen'

  if (name == 'Aging and Long Term Care'):
    name = 'Aging and Long-Term Care'

  # For any names missing a final comma before 'and' (see top of script)
  if (name in NAMES_MISSING_COMMA):
    name_pieces = name.split(' and ')
    name = '{0}, and {1}'.format(name_pieces[0], name_pieces[1])

  return name

'''
'''
def get_person(cursor, filer_naml, floor, state):
  pid = None 
  filer_naml = clean_name(filer_naml)
  temp = filer_naml.split(' ')

  if(floor == 'AFLOOR'):
    floor = "Assembly"
  else:
    floor = "Senate"
  filer_namf = ''
  if(len(temp) > 1):
    filer_naml = temp[len(temp)-1]
    filer_namf = temp[0]
#    print 'They had a first name!!!'
    cursor.execute(QS_PERSON_FL, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
  else:
#    print 'Only a last name...'
    cursor.execute(QS_PERSON_L, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
  if cursor.rowcount == 1:
    pid = cursor.fetchone()[0]
  elif cursor.rowcount > 1:
    a = []
    for j in range(0, cursor.rowcount):
      temp = cursor.fetchone()
      a.append(temp[0])
    for j in range(0, cursor.rowcount):
      cursor.execute(QS_TERM, {'pid':a[j],'house':floor,'state':state})
      if(cursor.rowcount == 1):
        pid = cursor.fetchone()[0]
  else:
    filer_naml = '%' + filer_naml + '%'
    cursor.execute(QS_PERSON_LIKE_L, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
    if(cursor.rowcount > 0):
      pid = cursor.fetchone()[0]
    else:
      sys.stderr.write("WARNING: Unable to find person {0}".format(filer_naml))
  return pid

'''
If Bill Vote Summary is found, return vote id. Otherwise, return None.
'''
def get_vote_id(cursor, bid, mid):
  cursor.execute(QS_VOTE_ID, {'bid':bid, 'mid':mid})
  if cursor.rowcount == 1:
    return cursor.fetchone()[0]  
  return None;

'''
If Bill Vote Summary is not in DDDB, add. Otherwise, skip
'''
def insert_bill_vote_summary(cursor, bid, mid, cid, vote_date, ayes, naes, abstain, result):
  cursor.execute(QS_VOTE_SUMMARY, {'bid':bid, 'mid':mid, 'vote_date':vote_date})
  if cursor.rowcount == 0:
    try:
      cursor.execute(QI_SUMMARY, (bid, mid, cid, vote_date, ayes, naes, abstain, result))
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('BillVoteSummary', 
            (QI_SUMMARY % (bid, mid, cid, vote_date, ayes, naes, abstain, result))))

'''
If Bill Vote Detail is not in DDDB, add. Otherwise, skip
'''
def insert_bill_vote_detail(cursor, pid, voteId, result):
  cursor.execute(QS_VOTE_DETAIL, {'pid':pid, 'voteId':voteId, 'state':STATE})
  if cursor.rowcount == 0:
    try:
      cursor.execute(QI_DETAIL, (pid, voteId, result, STATE))
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('BillVoteDetail',
            (QI_DETAIL % (pid, voteId, result, STATE))))

'''
Get Bill Vote Summaries. If bill vote summary isn't found in DDDB, add. 
Otherwise, skip.
'''
def get_summary_votes(ca_cursor, dd_cursor):
  print('Getting Summaries')
  ca_cursor.execute(QS_BILL_SUMMARY)
  rows = ca_cursor.fetchall()
  for bid, loc_code, mid, ayes, noes, abstain, result, vote_date in rows:
    cid = get_committee(ca_cursor, dd_cursor, loc_code)
    bid = '%s_%s' % (STATE, bid)

    if cid is not None:
#      print str(cid) + ' ' + str(bid)
      insert_bill_vote_summary(
          dd_cursor, bid, mid, cid, vote_date, ayes, noes, abstain, result)
    elif cid is None and loc_code not in logged_list:
      logged_list.append(loc_code)
      logger.warning('Committee not found ' + str(loc_code), 
          additional_fields={'_state':'CA'})

'''
Get Bill Vote Details. If bill vote detail isn't found in DDDB, add.
Otherwise, skip.
'''
def get_detail_votes(ca_cursor, dd_cursor):
  print('Getting Details')
  ca_cursor.execute(QS_BILL_DETAIL)
  rows = ca_cursor.fetchall()

  for bid, loc_code, legislator, vote_code, mid, trans_update in rows:
    bid = '%s_%s' % (STATE, bid)
    date = trans_update.strftime('%Y-%m-%d')
    pid = get_person(dd_cursor, legislator, loc_code, STATE)
    vote_id = get_vote_id(dd_cursor, bid, mid)
    result = vote_code

#    if bid == '201520160AB350':
#      print('vote_id: %s, pid: %s' % (vote_id, pid))
#      raise Exception()
    if vote_id is not None and pid is not None:
      insert_bill_vote_detail(dd_cursor, pid, vote_id, result)
#    elif vote_id is None and (bid, mid) not in logged_list:
#      logged_list.append((bid, mid))
#      logger.warning('Vote ID not found', full_msg='vote_id for bid: ' + bid +
#          ' mid: ' + str(mid) + ' not found', additional_fields={'_state':'CA'})
    elif pid is None and legislator not in logged_list:
      logged_list.append(legislator)
      logger.warning('Person not found ' + legislator, 
          additional_fields={'_state':'CA'})

def main():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python') as ca_cursor:
    with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                           port=3306,
                           db='EricTest',
                           user='awsDB',
                           passwd='digitaldemocracy789') as dd_cursor:
      get_summary_votes(ca_cursor, dd_cursor)
      get_detail_votes(ca_cursor, dd_cursor)

if __name__ == "__main__":
  with GrayLogger(API_URL) as _logger:
    logger = _logger
    main()
