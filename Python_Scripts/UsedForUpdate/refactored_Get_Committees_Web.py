#!/usr/bin/env python27
# -*- coding: utf-8 -*- 
'''
File: Get_Committees_Web.py
Author: Daniel Mangin
Modified By: Mandy Chan, Freddy Hernandez, Matt Versaggi
Date: 6/11/2015
Last Modified: 1/26/2016

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

Notes:
  - Some committees cause a 404 (Not Found) when the committee membership url 
      is requested; this happens when the link is clicked on through the browser
      as well, so there's not much that can be done about it. At this point, the
      committee membership pages known to cause this are:
        * Assembly Select Committee on Emerging Technology
        * Assembly Select Committee on Expanding Access to CA Natural Resources

'''

import datetime
import json
import MySQLdb
import re
import sys
import urllib2
from bs4 import BeautifulSoup

#import loggingdb

# U.S. State
STATE = 'CA'

# Database Queries
# INSERTS
QI_COMMITTEE = '''INSERT INTO Committee (cid, house, name, type, state)
                      VALUES (%s, %s, %s, %s, %s)'''
QI_SERVESON = '''INSERT INTO servesOn (pid, year, house, cid, position, state) 
                     VALUES (%s, %s, %s, %s, %s, %s)'''

# SELECTS
QS_TERM = '''SELECT pid
             FROM Term
             WHERE house = %s
              AND year <= %s
              AND state = %s'''
QS_TERM_2 = '''SELECT year
               FROM Term
               WHERE pid = %s
                AND house = %s
                AND year <= %s
                AND state = %s'''
QS_COMMITTEE = '''SELECT cid
                  FROM Committee
                  WHERE house = %s
                   AND name = %s
                   AND type = %s
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
                  AND cid = %s
                  AND state = %s'''

'''
Cleans committee names

|name|: Committee name to clean

Returns the cleaned name
'''
def clean_name(name):

  if 'NavigableString' in str(type(name)):
    name = str(name.encode('utf-8'))

  if 'acute;' in name:
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
  if '-' in name:
    name = '-'.join(name.split('-'))
  if name.endswith('.') and name.count > 2:
    name = name.replace('.', '')

  # Cleans out html found in scraped name; occurs when acute character used
  while '<' in name:
    start = name.find('<')
    end = name.find('>')
    name = name.replace(name[start:end + 1], '')

  name = name.strip('\xc2\xa0')
  name = name.replace('&nbsp;', '')
  name = name.replace('\xe2\x80\x99', '\'')

  return name.strip()

'''
Cleans committee urls

|url|: url to clean
|house|: political house of committee (Assembly or Senate)
|host|: host url

Returns clean url
'''
def clean_url(url, house, host):
  if url.startswith('/'):
    # |url| is a relative link; make it an absolute link.
    url = '%s%s' % (host, url)
  if house == 'Assembly':
    if len(url.split('/')) == 3:
      # Special case for Assembly Standing Committee on Water, Parks, and Wildlife
      if 'awpw.assembly' in url:
        url += '/content/members-staff'
      else:
      # No resource requested in |url|. Add the default one.
        url += '/membersstaff'
    if len(url.split('/')) == 4 and url.endswith('/'):
      # Same as above except url ends with another forward slash
      url += 'membersstaff'
  return url

'''
Inserts committee 

|cursor|: DDDB database cursor
|house|: House (Assembly/Senate) for adding
|name|: Legislator name for adding

Returns the new cid.
'''
def insert_committee(cursor, house, name, commType):
  # Get the next available cid.
  cursor.execute(QS_COMMITTEE_MAX_CID)
  cid = cursor.fetchone()[0] + 1
  cursor.execute(QI_COMMITTEE, (cid, house, name, commType, STATE))
  return cid

'''
Checks if the legislator is already in database, otherwise input them in servesOn

!!!WARNING!!!: If multiple Term years exist for same (pid, house, state) this
               function may have problems

|cursor|: DB cursor
|pid|: id in DB of person-to-be-inserted
|year|: year began serving
|house|: political house (assebly/senate)
|cid|: id of committee serving on
|position|: position in the committee
'''
def insert_serveson(cursor, pid, year, house, cid, position):
  # First get year of Term served by Person represented by pid
  cursor.execute(QS_TERM_2, (pid, house, year, STATE))
  termYear = cursor.fetchone()[0]
  cursor.execute(QS_SERVESON, (pid, house, termYear, cid, STATE))
  if (cursor.rowcount == 0):
    print 'About to insert pid:{0} year:{1} house:{2} cid:{3} position:{4} \
           state:{5}'.format(pid, termYear, house, cid, position, STATE)
    cursor.execute(QI_SERVESON, (pid, termYear, house, cid, position, STATE))

'''
Gets a committee id given its house and name. If the committee does
not exist in the database, it is first inserted and its new committee id
obtained.

|cursor|: database cursor
|house|: political house (assembly/senate)
|name|: name of the committee

Returns the committee id.
'''
def get_committee_id(cursor, house, name, commType):
  cursor.execute(QS_COMMITTEE, (house, name, commType, STATE))
  com = cursor.fetchone()
  # Tweak committee type slightly for Subcommittees/Budget Subcommittees
  if "Sub" in commType:
    commType += "committee"
    if "Budget" in name:
      commType = "Budget " + commType
  return insert_committee(cursor, house, name, commType) if com is None else com[0]

'''
Finds the id of a person.

|cursor|: database cursor
|name|: name of person to look for

Returns the id, or None if the person is not in the database.
'''
def get_person_id(cursor, name):
  
  if not ' ' in name:
    name = name.replace('\xc2\xa0', ' ')

  names = name.strip().split(' ')
  # Special case for Katcho...This is why we don't like him.
  if 'Katcho' in names[0]:
    first = 'K.H. \"Katcho\"'
  # Special case for Patricia Bates; She's Pat in the DB
  elif 'Patricia' in names[0] and 'Bates' in names[-1]:
    first = 'Pat'
  # Special case for when we scrape Ben Allen; he's Benjamin in DB
  elif 'Ben' in names[0] and 'Allen' in names[-1]:
    first = 'Benjamin'
  else:
    first = '{0}'.format(names[0].strip())
  # Name ends with Jr., Sr., (D), (R), or III, 
  # so strip it and get actual last name (w/o comma if existant)
  if 'Jr' in names[-1]        \
    or 'Sr' in names[-1]      \
    or ('(D)' in names[-1]    \
      or '(R)' in names[-1])  \
    or 'III' in names[-1]:  
    last = names[-2].replace(',', '')
  # Check if it's a name like Kevin De Leon; use De Leon as last if so
  elif names[-2].lower() == 'de':
    last = names[-2] + ' ' + names[-1]
  else:
    last = names[-1]
  if '.' in last:
    last = last.split('.')[1]
  
  first = clean_name(first)
  last = clean_name(last)
  
  cursor.execute(QS_LEGISLATOR, (last, first))
  if cursor.rowcount > 0:
    res = cursor.fetchone()[0]
  else:
    res = None
  return res

'''
Scans a given html page for a given regex pattern and returns a list of matches.

|url|: The url of the html page to scan
|pattern|: The pattern to search for (given to re.finditer())

Returns a list of matches (if found), returns an empty list otherwise.
'''
def scan_page(url, pattern):
  try:
    html = urllib2.urlopen(url).read()
    print "Reading from url {0}".format(url)
    return re.finditer(pattern, html)
  except urllib2.HTTPError as error:
    # Multiple committee membership pages cause exception when opened:
    #   First time requesting link returns 403 (Forbidden),
    #   Second time and thereafter returns 200 (Success).
    if error.getcode() == 403:
    # TO DO: Implement logic to retry page retrieval (using wget?)
      sys.stderr.write("{0}: {1}\n".format(url, error))
    else:
      sys.stderr.write("{0}: {1}\n".format(url, error))
    return []
 

'''
Scrapes a committee members page website and returns the
names of the members.

|url|: The url of the committee members page
|house|: political house of committee (Assembly or Senate)

Generates member names.
'''
def get_committee_members(url, house):
  if house == 'Assembly':
    member_pat = '<td>\s*<a.*?>(.*?)</a>(<a.*?>.*?</a>)*.*?</td>'
  else:
    member_pat = '<a href=.*?>Senator\s+(.*?)</a>'
  for match in scan_page(url, member_pat):
    # Check to see if scraped name has HTML within the name itself,
    #   i.e. Kans<a href="http://yada.yada">en Chu
    if (match.lastindex > 1):
        name = clean_name(match.group(1)) + clean_name(match.group(2))
    else:
        name = clean_name(match.group(1))
    yield name

'''
Checks the name to see if it includes the members position on the Committee.
If it does, use that position. Otherwise, default to "Member"

|member|: The member name to check

Returns the committee position of the member
'''
def get_member_position(member):
  if "Chair" in member or "chair" in member:
    temp = member.split('(')[1]
    if "Vice" in temp:
      position = "Vice-Chair"
    elif "Co-" in temp:
      position = "Co-Chair"
    else:
      position = "Chair"
    member = member.split('(')[0]
  else:
    position = "Member"
    if "Dem. Alternat" in member:
      tempMember = member.split(', Dem. Alternat')
      member = tempMember[0] if len(tempMember) > 1 \
                             else member.split('(Dem. Alternat')[0]
    elif "Rep. Alternat" in member:
      tempMember = member.split(', Rep. Alternat')
      member = tempMember[0] if len(tempMember) > 1 \
                             else member.split('(Rep. Alternat')[0]

  return member.strip(), position

'''
Creates and returns a list of committees for a given subcommittee type on
the senate committee site (html structure is different than assembly site).

|currSubComm|: The html block containing the current subcommittee type

Returns a list of committees
'''
def get_senate_subcomms(currSubComm):
  subCommList = []

  while (currSubComm.next_sibling.next_sibling and
         currSubComm.next_sibling.next_sibling.name != "h3"):
    subCommList.append(currSubComm.next_sibling.next_sibling.find("a"))
    currSubComm = currSubComm.next_sibling.next_sibling

  return subCommList

'''
A generator that returns committees for a given house.

|house|: political house (Assembly or Senate)

Generates tuples of <committee members page url>, <committee name>
'''
def get_committees(house):
  host = 'http://%s.ca.gov' % house.lower()
  htmlSoup = BeautifulSoup(urllib2.urlopen('%s/committees' % host).read())

  # Iterates through each of the html blocks that represent a committee type
  # and its respective list of committees
  for currBlock in htmlSoup.find_all("div", "block-views"):
    commType = clean_name(currBlock.find("h2").string)

    if "Joint" in commType and house == "Assembly":
      continue

    sub = True if "Sub" in commType else False
    extra = True if "Extraordinary" in commType else False
    
    # Check if currently in Sub or Extraordinary Committee block;
    #    need an extra loop if so
    if sub or extra:
      for currSubComm in currBlock("h3"):
        commNamePrefix = house + " " + currSubComm.string + " "
        subCommList = currSubComm.next_sibling.next_sibling("a") if house == "Assembly" else get_senate_subcomms(currSubComm)
        for committee in subCommList:
          commName = commNamePrefix
          if extra: 
            commName += "on "
          commName += clean_name(committee.string)
          url = clean_url(committee["href"], house, host)
          yield url, commName, commType.split(" ")[0]
    else:
      for committee in currBlock.find(class_="content").find_all("a"):
        url = clean_url(committee["href"], house, host)
        if "Joint" in committee.string:
          commName = clean_name(committee.string)
          if "Legislative Budget" in commName:
            commName += " Committee"
        elif "Other" in commType:
          commName = house + " Committee on " + clean_name(committee.string)
        else:
          commName = house + " " + commType.split(" ")[0] + " Committee on " + clean_name(committee.string)
        yield url, commName, commType.split(" ")[0]

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
  floor_cid = get_committee_id(cursor, house, '%s Floor' % house, "Floor")
  for pid in term_pids:
    insert_serveson(cursor, pid, year, house, floor_cid, 'Member')

  for url, name, commType in get_committees(house):
    # Joint committees are recorded with a house of 'Joint'.
    cid = get_committee_id(cursor, 'Joint' if 'Joint' in name else house,
                           name, commType)
    for member in get_committee_members(url, house):
      cleanMember, position = get_member_position(member)
      pid = get_person_id(cursor, cleanMember)
      if pid is not None and pid in term_pids:
        insert_serveson(cursor, pid, year, house, cid, position)
      else:
        print "WARNING: Could not find {0} in DB".format(clean_name(member.split('(')[0]))

def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='MattTest',
                       user='awsDB',
                       passwd='digitaldemocracy789',
                       charset='utf8') as dd:
    year = datetime.datetime.now().year
    for house in ['Assembly', 'Senate']:
      update_committees(dd, house, year)
    
if __name__ == '__main__':
  main()
