#!/usr/bin/env python
# -*- coding: utf-8 -*- 
'''
File: Get_Committees_Web.py
Author: Daniel Mangin
Modified By: Mandy Chan
Date: 6/11/2015
Last Changed: 11/17/2015

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
Inserts the Committee Senate Floor

|dd_cursor|: DDDB database cursor
'''
def insert_senate_floor(dd_cursor):
  house = 'Senate'
  name = 'Senate Floor'

  com = get_committee(dd_cursor, house, name)
  cid = insert_committee(dd_cursor, house, name) if com is None else com[0]
  insert_floor_members(dd_cursor, cid, house)
  
'''
Inserts the Committee Assembly Floor

|dd_cursor|: DDDB database cursor
'''
def insert_assembly_floor(dd_cursor):
  name = 'Assembly Floor'
  house = 'Assembly'

  com = get_committee(dd_cursor, house, name)
  cid = insert_committee(dd_cursor, house, name) if com is None else com[0]
  insert_floor_members(dd_cursor, cid, house)
      
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
Finds the person
'''
def get_person(cursor, filer_naml, filer_namf):
  pid = -1
  filer_naml = '%' + filer_naml + '%'
  filer_namf = '%' + filer_namf + '%'
  select_pid = 'SELECT pid FROM Person WHERE last LIKE %(filer_naml)s AND first LIKE %(filer_namf)s ORDER BY Person.pid;'
  cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
  if cursor.rowcount > 0:
    pid = cursor.fetchone()[0]
  else:
    print "couldn't find {0} {1}".format(filer_namf, filer_naml)
  return pid

'''
Creates all the data needed for the servesOn insertion
'''
def create_servesOn(cursor, name, house, cid):
  year = 2015
  name = name.split(' ')
  first = ''.join(name[0].split(' '))
  last = ''.join(name[len(name)-1].split(' '))
  pid = -1;
  if len(first) > 0 and len(last) > 0:
    pid = get_person(cursor, last, first)
  else:
    print 'Missing first or last name';
  if pid != -1:
    district = find_district(cursor, pid, year, house)
    if district is not None:
      insert_serveson(cursor, pid, year, district, house, cid)
    else:
      print 'District not found'

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

def find_committee(cursor, house, name):
  name = clean_name(name)
  select_stmt = 'SELECT * from Committee where house = %(house)s AND name = %(name)s;'
  cursor.execute(select_stmt, {'house':house,'name':name})
  if cursor.rowcount == 0:
    return insert_committee(cursor, house, name)
  else:
    return cursor.fetchone()[0]

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
Get each member of the given assembly committee. If the member is not recorded
in DDDB, add.

|dd|: database cursor
|imp|: link to committee members page
|cid|: committee id
|house|: house (assembly or senate)
'''
def get_members_assembly(dd, imp, cid, house):
  link = imp.split('"')[1]
  if(imp.count('/') == 1):
    link = 'http://assembly.ca.gov' + link;
  if len(link.split('/')) == 3:
    link = link + '/membersstaff'
  page = urllib2.urlopen(link)
  html = page.read()
  matches = re.findall('<td>\n.+<.+</td>',html)

  for match in matches:
    name = match.split('>')[2].split('<')[0].split('(')[0]
    name = clean_name(name)
    create_servesOn(dd, name, house, cid)

'''
Get each member of the given senate committee. If the member is not recorded
in DDDB, add.

|dd|: database cursor
|imp|: link to committee members page
|cid|: committee id
|house|: house (assembly or senate)
'''
def get_members_senate(dd, imp, cid, house):
  link = imp.split('"')[1]
  page = urllib2.urlopen(link)
  html = page.read()
  matches = re.findall('<a href=.+>Senator.+',html)
  ''.join(matches)

  for match in matches:
    parts = match.split('>')
    for part in parts:
      if 'Senator' in part:
        name = part.split('>')[0].split('(')[0].split('<')[0]
        name = ' '.join(name.split(' ')[1:])
        name = clean_name(name)
        create_servesOn(dd, name, house, cid)

'''
Opens up the assembly committe page and scrapes information about its 
committees and committee members. If the committee or member is not recorded
in DDDB, add.

|dd|: database cursor
'''
def get_assembly_information(dd):
  response = urllib2.urlopen('http://assembly.ca.gov/committees')
  html = response.read()
  matches = re.findall('<span class="field-content">.+',html)
  for match in matches:
    parts = match.split('<')
    imp = parts[2].split('>')
    house = 'Assembly'
    if 'Joint' in imp[1]:
      house = 'Joint'
    print 'Committee: {0}'.format(imp[1])
    cid = find_committee(dd, house, imp[1])
    house = 'Assembly'
    get_members_assembly(dd, imp[0], cid, house)
  insert_assembly_floor(dd)

'''
Opens up the senate committee page and scrapes information about its committees
and committee members. If the committee or member is not recorded in DDDB, add.

|dd|: database cursor
'''
def get_senate_information(dd):
  joint = 'Joint'
  senate = 'Senate'

  response = urllib2.urlopen('http://senate.ca.gov/committees')
  html = response.read()
  matches = re.findall('<div class="views-field views-field-title">.+\n.+',html)
  insert_floor_committee(dd, house, 'Senate Floor')

  for match in matches:
    match = match.split('\n')[1]
    parts = match.split('<')
    imp = parts[1].split('>')

    name = clean_name(imp[1])
    cid = get_committee_id(dd, joint if joint in name else senate, name)
    get_members_senate(dd, imp[0], cid, senate)

def main():
  # Database Connections
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='DDDB2015July',
                       user='awsDB',
                       passwd='digitaldemocracy789',
                       charset='utf8') as dd:
    get_assembly_information(dd)
    get_senate_information(dd)

if __name__ == '__main__':
  main()
