#!/usr/bin/env python
'''
File: Vote_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

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
  - BillVoteDetail (pid, voteId, result)

'''

import re
import sys
import loggingdb
import MySQLdb
from pprint import pprint
from urllib import urlopen

# Queries
insert_summary = '''INSERT INTO BillVoteSummary 
                    (bid, mid, cid, VoteDate, ayes, naes, abstain, result) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                 '''

insert_detail =  '''INSERT INTO BillVoteDetail (pid, voteId, result) 
                    VALUES (%s, %s, %s);
                 '''

'''
If committee is found, return cid. Otherwise, return -1.
'''
def find_committee(cursor, name, house):
  select_stmt = '''SELECT cid 
                   FROM Committee 
                   WHERE name = %(name)s 
                    AND house = %(house)s;
                '''
  cursor.execute(select_stmt, {'name':name, 'house':house})
  if(cursor.rowcount == 1):
    return cursor.fetchone()[0]
  return -1

'''
Parses the committee to find name and house. If committee is found, return cid.
Otherwise, return -1.
'''
def get_committee(ca_cursor, dd_cursor, location_code):
  select_stmt = '''SELECT description, long_description 
                   FROM location_code_tbl 
                   WHERE location_code = %(location_code)s;
                '''
  ca_cursor.execute(select_stmt, {'location_code':location_code})
  if(ca_cursor.rowcount > 0):
    temp = ca_cursor.fetchone()
    name = temp[0]
    long_name = temp[1]
    if 'Water, Parks' in long_name:
      long_name = 'Water, Parks, and Wildlife'
    if 'Asm' in name or 'Assembly' in name:
      house = 'Assembly'
    elif 'Sen' in name:
      house = 'Senate'
    else:
      house = 'Joint'
  return find_committee(dd_cursor, long_name, house)
    
'''
Cleans bad names
'''
def clean_name(name):
  # For de Leon
  temp = name.split('\xc3\xb3')
  if(len(temp) > 1):
    name = temp[0] + 'o' + temp[1];

  # For Travis Allen
  if(name == 'Allen Travis'):
    name = 'Travis Allen'

  return name

def get_person(cursor, filer_naml, floor):
	pid = -1
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
		select_pid = "SELECT pid, last, first FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf)s AND pid < 130 ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	else:
		select_pid = "SELECT pid, last, first FROM Person WHERE last = %(filer_naml)s AND pid < 130 ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount == 1:
		pid = cursor.fetchone()[0]
	elif cursor.rowcount > 1:
		a = []
		for j in range(0, cursor.rowcount):
			temp = cursor.fetchone()
			a.append(temp[0])
		for j in range(0, cursor.rowcount):
			select_term = "SELECT pid, house FROM Term WHERE pid = %(pid)s AND house = %(house)s;"
			cursor.execute(select_term, {'pid':a[j],'house':floor})
			if(cursor.rowcount == 1):
				pid = cursor.fetchone()[0]
	else:
		filer_naml = '%' + filer_naml + '%'
		select_pid = "SELECT pid, last, first FROM Person WHERE last LIKE %(filer_naml)s ORDER BY Person.pid;"
		cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
		if(cursor.rowcount > 0):
			pid = cursor.fetchone()[0]
		else:
			print "could not find {0}".format(filer_naml)
	return pid

'''
If Bill Vote Summary is found, return vote id. Otherwise, return -1.
'''
def get_vote_id(cursor, bid, mid):
  select_pid = '''SELECT voteId 
                  FROM BillVoteSummary 
                  WHERE bid = %(bid)s 
                   AND mid = %(mid)s;
               '''
  cursor.execute(select_pid, {'bid':bid, 'mid':mid})
  if cursor.rowcount == 1:
    return cursor.fetchone()[0]
  return -1;

'''
If Bill Vote Summary is not in DDDB, add. Otherwise, skip
'''
def insert_bill_vote_summary(cursor, bid, mid, cid, vote_date, ayes, naes, abstain, result):
  select_pid = '''SELECT bid, mid, VoteDate 
                  FROM BillVoteSummary 
                  WHERE bid = %(bid)s 
                   AND mid = %(mid)s 
                   AND VoteDate = %(vote_date)s;
               '''
  cursor.execute(select_pid, {'bid':bid, 'mid':mid, 'vote_date':vote_date})
  if cursor.rowcount == 0:
    cursor.execute(insert_summary, (bid, mid, cid, vote_date, ayes, naes, abstain, result))

'''
If Bill Vote Detail is not in DDDB, add. Otherwise, skip
'''
def insert_bill_vote_detail(cursor, pid, voteId, result):
  select_pid = '''SELECT pid, voteId
                  FROM BillVoteDetail 
                  WHERE pid = %(pid)s 
                   AND voteId = %(voteId)s;
               '''
  cursor.execute(select_pid, {'pid':pid, 'voteId':voteId})
  if cursor.rowcount == 0:
    cursor.execute(insert_detail, (pid, voteId, result))

'''
Get Bill Vote Summaries. If bill vote summary isn't found in DDDB, add. 
Otherwise, skip.
'''
def get_summary_votes(ca_cursor, dd_cursor):
  print('Getting Summaries')
  ca_cursor.execute('''SELECT bill_id, location_code, motion_id, ayes, noes, 
                        abstain, vote_result, trans_update
                       FROM bill_summary_vote_tbl;
                    ''')
  rows = ca_cursor.fetchall()
  for (bid, loc_code, mid, ayes, noes, abstain, result, vote_date) in rows:
    cid = get_committee(ca_cursor, dd_cursor, loc_code)

    if(cid != -1):
      insert_bill_vote_summary(
          dd_cursor, bid, mid, cid, vote_date, ayes, noes, abstain, result)

'''
Get Bill Vote Details. If bill vote detail isn't found in DDDB, add.
Otherwise, skip.
'''
def get_detail_votes(ca_cursor, dd_cursor):
  print('Getting Details')
  ca_cursor.execute('''SELECT bill_id, location_code, legislator_name, 
                        vote_code, motion_id, trans_update
                       FROM bill_detail_vote_tbl;
                    ''')
  rows = ca_cursor.fetchall()

  for (bid, loc_code, legislator, vote_code, mid, trans_update) in rows:
    date = trans_update.strftime('%Y-%m-%d')
    pid = get_person(dd_cursor, legislator, loc_code)
    vote_id = get_vote_id(dd_cursor, bid, mid)
    result = vote_code

    if vote_id != -1 and pid != -1:
      insert_bill_vote_detail(dd_cursor, pid, vote_id, result)

def main():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python') as ca_cursor:
    with loggingdb.connect(host='transcription.digitaldemocracy.org',
                         db='DDDB2015JulyTest',
                         user='monty',
                         passwd='python') as dd_cursor:
      get_summary_votes(ca_cursor, dd_cursor)
      get_detail_votes(ca_cursor, dd_cursor)

if __name__ == "__main__":
  main()
