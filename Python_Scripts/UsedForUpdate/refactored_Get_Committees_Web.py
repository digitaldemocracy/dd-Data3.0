#!/usr/bin/env python27
# -*- coding: utf-8 -*- 
'''
File: Get_Committees_Web.py
Author: Daniel Mangin
Modified By: Mandy Chan and Freddy Hernandez
Date: 6/11/2015
Last Changed: 11/22/2015

Description:
- Scrapes the Assembly and Senate websites to gather committees and memberships
- Used for daily update

Sources:
  - California Assembly Website
  - California Senate Website

Dependencies:
  - Person
  - Term

Populates:
  - Committee (cid, house, name, state)
  - servesOn (pid, year, house, cid, state)

'''

import datetime
import json
import MySQLdb
import re
import sys
import urllib2

import loggingdb

# U.S. State
STATE = 'CA'

# Database Queries
# INSERTS
QI_COMMITTEE = '''INSERT INTO Committee (cid, house, name, state)
                      VALUES (%s, %s, %s, %s)'''
QI_SERVESON = '''INSERT INTO servesOn (pid, year, house, cid, state) 
                     VALUES (%s, %s, %s, %s, %s)'''

# SELECTS
QS_TERM = '''SELECT pid
             FROM Term
             WHERE house = %s
              AND year = %s
              AND state = %s'''
QS_COMMITTEE = '''SELECT cid
                  FROM Committee
                  WHERE house = %s
                   AND name = %s
                   AND state = %s'''
QS_COMMITTEE_MAX_CID = '''SELECT cid
                          FROM Committee
                          ORDER BY cid DESC
                          LIMIT 1'''
QS_LEGISLATOR = '''SELECT p.pid
                   FROM Person p
                   JOIN Legislator l ON p.pid = l.pid
                   WHERE last LIKE %s
                    AND first LIKE %s
                   ORDER BY p.pid'''
QS_SERVESON = '''SELECT * FROM servesOn
                 WHERE pid = %s
                  AND house = %s
                  AND year = %s 
                  AND cid = %s'''
'''
Gets a committee id given its house and name. If the committee does
not exist in the database, it is first inserted and its new committee id
obtained.

|cursor|: database cursor
|house|: political house (assembly/senate)
|name|: name of the committee

Returns the committee id.
'''
def get_committee_id(cursor, house, name):
  cursor.execute(QS_COMMITTEE, (house, name, STATE))
  com = cursor.fetchone()
  return insert_committee(cursor, house, name) if com is None else com[0]

'''
Inserts committee 

|cursor|: DDDB database cursor
|house|: House (Assembly/Senate) for adding
|name|: Legislator name for adding

Returns the new cid.
'''
def insert_committee(cursor, house, name):
  # Get the next available cid.
  cursor.execute(QS_COMMITTEE_MAX_CID)
  cid = cursor.fetchone()[0] + 1
  cursor.execute(QI_COMMITTEE, (cid, house, name, STATE))
  return cid

'''
Checks if the legislator is already in database, otherwise input them in servesOn
'''
def insert_serveson(cursor, pid, year, house, cid):
  cursor.execute(QS_SERVESON, (pid, house, year, cid))
  if(cursor.rowcount == 0):
    cursor.execute(QI_SERVESON, (pid, year, house, cid, STATE))

'''
Finds the id of a person.

|cursor|: database cursor
|name|: name of person to look for

Returns the id, or None if the person is not in the database.
'''
def get_person_id(cursor, name):
  names = name.split(' ')
  first = '%%%s%%' % names[0]
  last = '%%%s%%' % names[-1]
  cursor.execute(QS_LEGISLATOR, (last, first))
  if cursor.rowcount > 0:
    return cursor.fetchone()[0]
  return None

'''
Cleans committee names

|name|: Committee name to clean

Returns the cleaned name
'''
def clean_name(name):
  if 'acute;' in name:
    print('getting rid of acute character')
    name = ''.join(''.join(name.split('&')).split('acute;'))
  if '&#39;' in name:
    name = "'".join(name.split('&#39;'))
  if '&#039;' in name:
    name = "'".join(name.split('&#039;'))
  if '&nbsp;' in name:
    name = name.split('&nbsp;')[0]
  if 'nbsp;' in name:
    name = name.split('nbsp;')[0]
  if '&rsquo;' in name:
    name = name.split('&rsquo;')[0]
  if '–' in name:
    name = '-'.join(name.split('–'))
  return name.strip()

'''
Scrapes a committee members page website and returns the
names of the members.

|url|: The url of the committee members page
|house|: political house of committee (Assembly or Senate)

Generates member names.
'''
def get_committee_members(url, house):
  try:
    html = urllib2.urlopen(url).read()
    if house == 'Assembly':
      member_pat = '<td>\s*<a.*?>(.*?)</a>.*?</td>'
    else:
      member_pat = '<a.*?>Senator\s*(.*?)</a>'

    for match in re.finditer(member_pat, html):
      # Some names look like "John Doe (Chair)". Remove the (Chair) part.
      yield clean_name(match.group(1).split('(')[0])
  except urllib2.HTTPError as e:
    print('%s: %s' % (url, e))

'''
A generator that returns committees for a given house.

|house|: political house (Assembly or Senate)

Generates tuples of <committee members page url>, <committee name>
'''
def get_committees(house):
  host = 'http://%s.ca.gov' % house.lower()
  html = urllib2.urlopen('%s/committees' % host).read()
  committee_pat = '<span class="field-content">\s*<a href="(.*?)">(.*?)</a>'

  for match in re.finditer(committee_pat, html):
    url = match.group(1)
    name = clean_name(match.group(2))

    if url.startswith('/'):
      # |url| is a relative link; make it an absolute link.
      url = '%s%s' % (host, url)

    if house == 'Assembly':
      if len(url.split('/')) == 3:
        # No resource requested in |url|. Add the default one.
        url += '/membersstaff'
    yield url, name

'''
Scrapes committee web pages for committee information and adds it
to DDDB if it does not already exist.

|cursor|: database cursor
|house|: political house (Assembly or Senate)
'''
def update_committees(cursor, house, year):
  cursor.execute(QS_TERM, (house, year, STATE))
  term_pids = [row[0] for row in cursor.fetchall()]

  # Special case for floor committee.
  floor_cid = get_committee_id(cursor, house, '%s Floor' % house)
  for pid in term_pids:
    insert_serveson(cursor, pid, year, house, floor_cid)
  
  for url, name in get_committees(house):
    # Joint committees are recorded with a house of 'Joint'.
    cid = get_committee_id(cursor, 'Joint' if 'Joint' in name else house, name)
    for member in get_committee_members(url, house):
      pid = get_person_id(cursor, member)
      if pid is not None and pid in term_pids:
        insert_serveson(cursor, pid, year, house, cid)

def main():
  with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='DDDB2015Dec',
                         user='awsDB',
                         passwd='digitaldemocracy789',
                         charset='utf8') as dd:
    year = datetime.datetime.now().year
    for house in ['Assembly', 'Senate']:
      update_committees(dd, house, year)
    
if __name__ == '__main__':
  main()
