#!/usr/bin/env python
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
  - servesOn (pid, year, district, house, cid)

'''

import json
import MySQLdb
import re
import sys
import urllib2

import loggingdb

from pprint import pprint
from urllib import urlopen

# U.S. State
state = 'CA'

# Database Queries
# INSERTS
qi_committee = '''INSERT INTO Committee (cid, house, name, state)
                      VALUES (%s, %s, %s, %s)'''
qi_serveson = '''INSERT INTO servesOn (pid, year, district, house, cid) 
                     VALUES (%s, %s, %s, %s, %s)'''

# SELECTS
qs_term = '''SELECT pid, district
             FROM Term
             WHERE house = %(house)s
              AND state = %(state)s'''
qs_committee = '''SELECT cid
                  FROM Committee
                  WHERE house = %s
                   AND name = %s
                   AND state = %s'''
qs_committee_max_cid = '''SELECT cid
                          FROM Committee
                          ORDER BY cid DESC
                          LIMIT 1'''

'''
Gets a committee.

|cursor|: database cursor
|house|: political house (assembly/senate)
|name|: name of the committee

Returns the row found in the database, or None if not found.
'''
def get_committee(cursor, house, name):
  return dd_cursor.execute(qs_committee, (house, name, state)).fetchone()

'''
Inserts the floor members after finding their terms

|dd_cursor|: DDDB database cursor
|cid|: committee id
|house|: political house (assembly/senate)
'''
def insert_floor_members(dd_cursor, cid, house):
  dd_cursor.execute(qs_term, {'house':house, 'state':state})

  for pid, district in dd_cursor.fetchall():
    year = 2015
    insert_serveson(dd_cursor, pid, year, district, house, cid)

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
  com = get_committee(cursor, house, name)
  return insert_committee(cursor, house, name) if com is None else com[0]

'''
Inserts a committee.

|cursor|: database cursor
|house|: political house (assembly/senate)
|name|: name of the committee
'''
def insert_floor_committee(cursor, house, name):
  cid = get_committee_id(cursor, house, name)
  insert_floor_members(cursor, cid, house)

'''
Finds the district that a legislator serves using Term

|dd_cursor|: DDDB database cursor
|pid|: Person id to find
|year|: Year to find
|house|: House (Assembly/Senate) to find
'''
def find_district(cursor, pid, year, house):
  select_stmt = '''SELECT district 
                   FROM Term 
                   WHERE pid = %(pid)s 
                    AND house = %(house)s
                    AND year = %(year)s
                    AND state = %(state)s
                '''
  cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year, 'state':state})
  if(cursor.rowcount > 0):
    temp = cursor.fetchone()
    return temp[0]
  return 999

'''
Checks if the legislator is already in database, otherwise input them in servesOn
'''
def insert_serveson(cursor, pid, year, district, house, cid):
  select_stmt = '''SELECT * FROM servesOn WHERE pid = %(pid)s
                    AND house = %(house)s AND year = %(year)s 
                    AND cid = %(cid)s AND district = %(district)s
                '''
  cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year, 'cid':cid, 'district':district})
  if(cursor.rowcount == 0):
    print 'inserting {0}'.format(pid)
    cursor.execute(qi_serveson, (pid, year, district, house, cid))

'''
Finds the id of a person.

|cursor|: database cursor
|name|: name of person to look for

Returns the id, or None if the person is not in the database.
'''
def get_person_id(cursor, name):
  names = name.split(' ')
  first = ''.join(name[0].split(' '))
  last = ''.join(name[len(name) - 1].split(' '))
  select_pid = '''SELECT pid
                  FROM Person
                  WHERE last LIKE %%%s%%
                   AND first LIKE %%%s%%
                  ORDER BY Person.pid'''
  cursor.execute(select_pid, (last, first))
  if cursor.rowcount > 0:
    return cursor.fetchone()[0]
  return None

'''
Updates the database to show that a house member is a member of
a given committee.

|cursor|: database cursor
|cid|: The committee id
|member_name|: name of the house member
|house|: political house (Assembly or Senate)
'''
def update_committee_membership(cursor, cid, member_name, house):
  year = 2015
  pid = get_person_id(cursor, member_name)
  if pid is not None:
    district = find_district(cursor, pid, year, house)
    if district is not None:
      insert_serveson(cursor, pid, year, district, house, cid)
    else:
      print('District not found')
  else:
    print('%s not found' % member_name)

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
Inserts committee 

|dd_cursor|: DDDB database cursor
|house|: House (Assembly/Senate) for adding
|name|: Legislator name for adding

Returns the new cid.
'''
def insert_committee(cursor, house, name):
  # Get the next available cid.
  cursor.execute(qs_committee_max_cid)
  cid = dd_cursor.fetchone()[0] + 1
  cursor.execute(qi_committee, (cid, house, name))
  return cid

'''
Scrapes a committee members page website and returns the
names of the members.

|url|: The url of the committee members page
|house|: political house of committee (Assembly or Senate)

Generates member names.
'''
def get_committee_members(url, house):
  html = urllib2.urlopen(url).read()
  if house == 'Assembly':
    member_pat = '<td>\s*<a.*?>(.*?)</a>.*?</td>'
  else:
    member_pat = '<a.*?>Senator\s*(.*?)</a>'

  for match in re.finditer(member_pat, html):
    # Some names look like "John Doe (Chair)". Remove the (Chair) part.
    yield clean_name(match.group(1).split('('))

'''
A generator that returns committees for a given house.

|house|: political house (Assembly or Senate)

Generates tuples of <committee members page url>, <committee name>
'''
def get_committees(house):
  host = 'http://%s.ca.gov' % house.lower()
  html = urllib2.urlopen('http://%s/committees' % host).read()
  committee_pat = '<span class="field-content">\s*<a href="(.*?)">(.*?)</a>'

  for match in re.finditer(committee_pat, html):
    url = match.group(1)
    name = clean_name(match.group(2))

    if house == 'Assembly':
      if(url.count('/') == 1):
        # |url| is a relative link. Add it to the host.
        url = '%s/%s' % (host, url)
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
def update_committees(cursor, house):
  insert_floor_committee(cursor, house, '%s Floor' % house)
  
  for url, name in get_committees(house):
    # Joint committees are recorded with a house of 'Joint'.
    cid = get_committee_id(cursor, 'Joint' if 'Joint' in name else house, name)
    for member in get_committee_members(url, house):
      update_committee_membership(cursor, cid, member, house)

def main():
  # Database Connections
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='DDDB2015July',
                       user='awsDB',
                       passwd='digitaldemocracy789',
                       charset='utf8') as dd:
    for house in ['Assembly', 'Senate']:
      update_committees(dd, house)
    
if __name__ == '__main__':
  main()
