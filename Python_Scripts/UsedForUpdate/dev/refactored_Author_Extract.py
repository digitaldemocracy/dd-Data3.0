#!/usr/bin/env python2.6
'''
File: Author_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan, Steven Thon
Date: 6/11/2015
Last Modified: 11/3/2015

Description:
- Inserts the authors from capublic.bill_version_authors_tbl into the 
  DDDB2015Apr.authors or DDDB2015Apr.committeeAuthors
- This script runs under the update script

Sources:
  - Leginfo (capublic)
    - Pubinfo_2015.zip
    - Pubinfo_Mon.zip
    - Pubinfo_Tue.zip
    - Pubinfo_Wed.zip
    - Pubinfo_Thu.zip
    - Pubinfo_Fri.zip
    - Pubinfo_Sat.zip

  - capublic
    - bill_version_author_tbl

Populates:
  - authors (pid, bid, vid, contribution)
  - CommitteeAuthors (cid, bid, vid, state)

'''

import re
import sys
import loggingdb
import MySQLdb
from pprint import pprint
from urllib import urlopen

# U.S. State
state = 'CA'

# Queries
query_insert_author = '''INSERT INTO authors (pid, bid, vid, contribution)
                         VALUES (%s, %s, %s, %s);''' 

query_insert_committee_author = '''INSERT INTO CommitteeAuthors (cid, bid, vid, state) 
                                   VALUES (%s, %s, %s, %s);'''

'''
If the committee author for this bill is not in DDDB, add. Otherwise, skip.
'''
def add_committee_author(cursor, cid, bid, vid, state):
  select_stmt = '''SELECT * 
                   FROM CommitteeAuthors 
                   WHERE cid = %(cid)s
                    AND bid = %(bid)s 
                    AND vid = %(vid)s
                    AND state = %(state)s;
                '''
  cursor.execute(select_stmt, {'cid':cid, 'bid':bid, 'vid':vid, 'state':state})
  if cursor.rowcount == 0:
    cursor.execute(query_insert_committee_author, (cid, bid, vid))

'''
Clean committee names. Returns cleaned name.
'''
def clean_committee_name(name):
  # Removes the 'Committee on' string inside the capublic name
  if 'Committee on' in name:
    return ' '.join((name.split(' '))[2:])

'''
Attempt to find the committee. If found, return their 'cid'. Otherwise,
return -1.
'''
def find_committee(cursor, name, house, state):
  house = house.title()                   # Titlecased for DDDB enum
  name = clean_committee_name(name)       # Clean name for checking

  # Find committee
  select_stmt = '''SELECT cid 
                   FROM Committee 
                   WHERE name = %(name)s 
                    AND house = %(house)s
                    AND state = %(state)s;
                '''
  cursor.execute(select_stmt, {'name':name, 'house':house, 'state':state})

  # Found
  if cursor.rowcount == 1:
    return cursor.fetchone()[0]

  # Not Found
  return -1

'''
Clean the name of the person and remove/replace weird characters
'''
def clean_name(name):
  # For de Leon
  temp = name.split('\xc3\xb3')
  if(len(temp) > 1):
    name = temp[0] + 'o' + temp[1]

  # For Travis Allen
  if(name == 'Allen Travis'):
    name = 'Travis Allen'

  # For O'Donnell
  if 'Donnell' in name:
    name = "O'Donnell"

  return name

'''
Find the Person using a combined name
'''
def get_person(cursor, filer_naml, house):
  pid = -1                              # Default NULL value
  filer_naml = clean_name(filer_naml)   # Clean name
  temp = filer_naml.split(' ')
  filer_namf = ''
  house = house.title()

  # Checks if there is a first and last name or just a first
  if(len(temp) > 1):
    filer_naml = temp[len(temp)-1]
    filer_namf = temp[0]
    select_pid = '''SELECT Person.pid, last, first
                    FROM Person, Legislator 
                    WHERE Legislator.pid = Person.pid 
                     AND last = %(last_name)s 
                     AND first = %(first_name)s
                    ORDER BY Person.pid;
                 '''
    cursor.execute(select_pid, {'last_name':filer_naml, 'first_name':filer_namf})
  else:
    select_pid = '''SELECT Person.pid, last, first 
                    FROM Person, Legislator 
                    WHERE Legislator.pid = Person.pid 
                     AND last = %(last_name)s 
                    ORDER BY Person.pid;
                 '''
    cursor.execute(select_pid, {'last_name':filer_naml})

  # If it finds a match of the exact name, use that
  if cursor.rowcount == 1:
    pid = cursor.fetchone()[0]
  # If there is more than one, have to use the house
  elif cursor.rowcount > 1:
    a = [t[0] for t in cursor.fetchall()]
    end = cursor.rowcount
    # Find which person it is using their term
    for j in range(0, end):
      select_term = '''SELECT pid, house 
                       FROM Term 
                       WHERE pid = %(pid)s 
                        AND house = %(house)s 
                       ORDER BY Term.pid;
                    '''
      cursor.execute(select_term, {'pid':a[j],'house':house})
      if(cursor.rowcount == 1):
        pid = cursor.fetchone()[0]

  # If none were found, loosen the search up a bit and just look for last name
  else:
    filer_naml = '%' + filer_naml + '%'
    select_pid = '''SELECT Person.pid, last, first
                    FROM Person, Legislator
                    WHERE Legislator.pid = Person.pid
                     AND last LIKE %(last_name)s
                    ORDER BY Person.pid;
                 '''
    cursor.execute(select_pid, {'last_name':filer_naml})
    if(cursor.rowcount == 1):
      pid = cursor.fetchone()[0]

  return pid

'''
Finds the bill id associated with the bill version. If bill is found, return the 
row. Otherwise, return None.
'''
def find_bill(cursor, vid):
  select_pid = '''SELECT bid 
                  FROM BillVersion 
                  WHERE vid = %(vid)s;
               '''
  cursor.execute(select_pid, {'vid':vid})
  if cursor.rowcount > 0:
	  return cursor.fetchone()[0]
  return None

'''
If the author for this bill is not in DDDB, add author. Otherwise, skip.
'''
def add_author(cursor, pid, bid, vid, contribution):
  select_stmt = '''SELECT bid, pid, vid 
                   FROM authors 
                   WHERE bid = %(bid)s 
                    AND pid = %(pid)s 
                    AND vid = %(vid)s;
                '''
  cursor.execute(select_stmt, {'bid':bid, 'pid':pid, 'vid':vid})
  if(cursor.rowcount == 0):
    cursor.execute(query_insert_author, (pid, bid, vid, contribution))

'''
Grabs capublic's information and selectively adds bill authors into DDDB.

Authors are only added if they are the primary lead author of the bill. Also, 
bill authors can be either Legislators or Committees.
'''
def get_authors(ca_cursor, dd_cursor):
  select_stmt = '''SELECT bill_version_id, type, house, name, 
                    contribution, primary_author_flg
                   FROM bill_version_authors_tbl
                '''
  ca_cursor.execute(select_stmt)
  rows = ca_cursor.fetchall()

  # Iterate over each bill author row in capublic
  for (vid, author_type, house, name, contrib, prim_author_flg) in rows:
    bid = find_bill(dd_cursor, vid)

    # Check if the bill is in DDDB. Otherwise, skip
    if bid is not None and prim_author_flg == 'Y':

      # Legislator Authors
      if author_type == 'Legislator':
        pid = get_person(dd_cursor, name, house)
        if pid != -1:
          add_author(dd_cursor, pid, bid, vid, contrib.title())
        else:
          print('Could not find Legislator Author "%s" for bill version %s, skipping' %
              (name, vid))

      # Committee Authors
      elif author_type == 'Committee':
        cid = find_committee(dd_cursor, name, house)
        if cid != -1:
          add_committee_author(dd_cursor, cid, bid, vid)
        else:
          print('Could not find Committee Author "%s" for bill version %s, skipping' % 
              (name, vid))

def main():
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='DDDB2015July',
                       user='awsDB',
                       passwd='digitaldemocracy789') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='capublic',
                       passwd='python') as ca_cursor:
      get_authors(ca_cursor, dd_cursor)

if __name__ == "__main__":
  main()  
