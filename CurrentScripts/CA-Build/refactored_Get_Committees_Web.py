#!/usr/bin/env python
# -*- coding: utf-8 -*- 
'''
File: Get_Committees_Web.py
Author: Daniel Mangin
Modified By: Mandy Chan, Freddy Hernandez, Matt Versaggi, Miguel Aguilar, James Ly
Date: 06/11/2015
Last Modified: 01/03/2017

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
  - The 403 HTTP Request error is still an issue for two pages.
    You are able to open the pages through the browser, but not 
    through python urllib2. The pages with this issue:
      - http://apro.assembly.ca.gov/membersstaff
      - http://smup.senate.ca.gov/
'''
from Database_Connection import mysql_connection
import Find_Person
import time
import datetime
import json
import MySQLdb
import re
import sys
import traceback
import urllib2
import sys
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger                                    
API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'                  
logger = None

# global counters
C_INSERT = 0
S_INSERT = 0
S_DELETE = 0

CR_UPDATE = 0   # committee room updates
CP_UPDATE = 0   # committee phone updates
CF_UPDATE = 0   # committee fax updates

# U.S. State
STATE = 'CA'

# Database Queries
# INSERTS
QI_COMMITTEE = '''INSERT INTO Committee (cid, house, name, type, state, room, phone, fax)
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
QI_SERVESON = '''INSERT INTO servesOn (pid, year, house, cid, position, state, start_date) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s)'''

# SELECTS
QS_TERM = '''SELECT pid
             FROM Term
             WHERE house = %s
              AND year BETWEEN %s AND %s
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
QS_SERVESON_2 = '''SELECT pid
                   FROM servesOn
                   WHERE start_date IS NOT NULL
                   AND end_date IS NULL'''

# DELETES
# Maybe change year from '<=' to '<'? 
# Not sure if we want to keep previous years 
QD_SERVESON = '''DELETE FROM servesOn
                WHERE start_date IS NULL 
                AND cid = %s
                AND house = %s
                AND year <= %s
                AND state = %s'''

#UPDATES
QU_COMMITTEE_ROOM = '''UPDATE Committee
                       SET room = %s
                       WHERE cid = %s'''
QU_COMMITTEE_PHONE = '''UPDATE Committee
                        SET phone = %s
                        WHERE cid = %s'''
QU_COMMITTEE_FAX = ''' UPDATE Committee
                       SET fax = %s
                       WHERE cid = %s'''
QU_SERVESON_ENDDATE = '''UPDATE servesOn
                         SET end_date = %s, current_flag = 0
                         WHERE start_date IS NOT NULL
                         AND end_date IS NULL
                         AND pid = %s'''

def create_payload(table, sqlstmt):
  return {
    '_table': table,
    '_sqlstmt': sqlstmt,
    '_state': 'CA',
    '_log_type':'Database'
      }

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
    #print 'HERE URL: %s   LEN: %d'%(url, len(url.split('/')))
    if len(url.split('/')) == 3:
      # Special case for Assembly Standing Committee on Water, Parks, and Wildlife
      if 'awpw.assembly' in url:
        url += '/content/members-staff'
      else:
      # No resource requested in |url|. Add the default one.
        url += '/membersstaff'
    if len(url.split('/')) == 4 and url.endswith('/'):
      # Same as above except url ends with another forward slash
      if 'expandingaccesstocanaturalresources' in url:
        url += 'content/members-staff'
      elif 'emergingtech.assembly' in url:
        url += 'content/members'
      else:
        url += 'membersstaff'
  return url

'''
Inserts committee 

|cursor|: DDDB database cursor
|house|: House (Assembly/Senate) for adding
|name|: Legislator name for adding

Returns the new cid.
'''
def insert_committee(cursor, house, name, commType, room, phone, fax):
  global C_INSERT
  try:
    # Get the next available cid
    cursor.execute(QS_COMMITTEE_MAX_CID)
    cid = cursor.fetchone()[0] + 1
    cursor.execute(QI_COMMITTEE, (cid, house, name, commType, STATE, room, phone, fax))
    C_INSERT += cursor.rowcount
    return cid
  except MySQLdb.Error:
    logger.warning('Insert Failed', full_msg=traceback.format_exc(),
      additional_fields=create_payload('Committee',(QI_COMMITTEE%(cid, house, name, commType, STATE, room, phone, fax))))
    return -1

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
|serve_count|: insert servesOn count

Returns insert servesOn count
'''
def insert_serveson(cursor, pid, year, house, cid, position, serve_count):
  global S_INSERT
  try:
    # First get year of Term served by Person represented by pid
    cursor.execute(QS_TERM_2, (pid, house, year, STATE))
    termYear = cursor.fetchone()[0]
    cursor.execute(QS_SERVESON, (pid, house, termYear, cid, STATE))
    if (cursor.rowcount == 0):
      #print 'About to insert pid:{0} year:{1} house:{2} cid:{3} position:{4} \
             #state:{5}'.format(pid, termYear, house, cid, position, STATE)
      today = time.strftime("%Y-%m-%d")
      cursor.execute(QI_SERVESON, (pid, termYear, house, cid, position, STATE, today))
      S_INSERT += cursor.rowcount
      serve_count = serve_count + 1
  except MySQLdb.Error:
    logger.warning('Insert Failed', full_msg=traceback.format_exc(),
    additional_fields=create_payload('servesOn',(QI_SERVESON%(pid, termYear, house, cid, position, STATE, today))))

  return serve_count

'''
Gets a committee id given its house and name. If the committee does
not exist in the database, it is first inserted and its new committee id
obtained.

|cursor|: database cursor
|house|: political house (assembly/senate)
|name|: name of the committee
|comm_count|: insert Committee count

Returns the committee id and insert Committee count.
'''
def get_committee_id(cursor, house, name, commType, comm_count, room, phone, fax):
  # Tweak committee type slightly for Subcommittees/Budget Subcommittees
  if "Sub" in commType:
    commType += "committee"
    if "Budget" in name:
      commType = "Budget " + commType

  try:
    cursor.execute(QS_COMMITTEE, (house, name, commType, STATE))
    com = cursor.fetchone()
  except MySQLdb.Error:
    logger.warning('Select Failed', full_msg=traceback.format_exc(),
    additional_fields=create_payload('Committee',(QS_COMMITTEE%(house, name, commType, STATE))))

  if com is None:
    comm_count = comm_count + 1

  return insert_committee(cursor, house, name, commType, room, phone, fax) if com is None else com[0], comm_count

'''
Finds the id of a person.

|cursor|: database cursor
|name|: name of person to look for

Returns the id, or None if the person is not in the database.
'''
def get_person_id(cursor, name, house, year, pfinder):
  
  if not ' ' in name:
    name = name.replace('\xc2\xa0', ' ')

  names = name.strip().split(' ')
  # Special case for Katcho...This is why we don't like him.
  if 'Katcho' in names[0]:
    first = 'K.H. \"Katcho\"'
  # Other special cases
  elif 'Christina' in names[0] and 'Garcia' in names[-1]:
    first = 'Cristina'
  elif 'Mark' in names[0] and 'Levine' in names[-1]:
    first = 'Marc'
  elif 'Steven' in names[0] and 'Glazer' in names[-1]:
    first = 'Steve'
  elif 'Lorena' in names[0] and 'Gonzales' in names[-1]:
    first = 'Lorena'
    names[-1] = 'Gonzalez'
  elif 'Fran' in names[0] and 'Pavely' in names[-1]:
    first = 'Fran'
    names[-1] = 'Pavley'
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
  elif len(names) >= 2 and names[-2].lower() == 'de':
    last = names[-2] + ' ' + names[-1]
  else:
    last = names[-1]
  if '.' in last:
    last = last.split('.')[1]
  
  first = clean_name(first)
  last = clean_name(last)
  
  #try:
  #  cursor.execute(QS_LEGISLATOR, (last, first))
  #except MySQLdb.Error:
  #  logger.warning('Select Failed', full_msg=traceback.format_exc(),
  #  additional_fields=create_payload('Legislator',(QS_LEGISLATOR%(last, first))))

  #if cursor.rowcount > 0:
  #  res = cursor.fetchone()[0]
  #else:
  #  res = None

  res = pfinder.findLegislator(first, last, house, year)
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
      try:
        comm_name = url.split('//')[1].split('.')[0]
        html = open('html_pages/'+comm_name, 'r').read()
        return re.finditer(pattern, html)
      except IOError:
        sys.stderr.write("Error: File %s does not appear to exist."%comm_name)
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
    member_pat = '<td>\s*(<h3>|)<a.*?>(.*?)</a>(<a.*?>.*?</a>)*.*?(</h3>\s*|)</td>'
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
Cleans (deletes) the members of a committee (cid) in the servesOn table.
That way they can insert the latest data in servesOn.

|cursor|: database cursor
|cid|: committee ID
|house|: political house (Assembly or Senate)
|year|: year of term
'''
def clean_servesOn(cursor, cid, house, year):
  global S_DELETE
  try:
    # Delete previous entries in order to insert the latest ones
    cursor.execute(QD_SERVESON, (cid, house, year, STATE))
    S_DELETE += cursor.rowcount
  except MySQLdb.Error:
    logger.warning('Delete Failed', full_msg=traceback.format_exc(),
    additional_fields=create_payload('servesOn',(QD_SERVESON%(cid, house, year, STATE))))


'''
Given a url in this format: http://committee.house.ca.gov/something_after
clean it so we can get to committee homepage -> http://committee.house.ca.gov

|url|: url to clean
'''
def get_committee_url(url):
  commUrl = ""
  temp = url.split('/')
  if temp[2] == "senate.ca.gov" or temp[2] == "assembly.ca.gov":
    commUrl = url
  else:
    commUrl = temp[0] + "//" + temp[2]
  
  return commUrl

'''
Given a committee url scrape the page for room, phone, and fax number
returns room, phone, and fax number as strings. They will be empty if none.

|commUrl|: committee url
'''
def get_committee_contact(commUrl, commType):
  room = ""
  phone = ""
  fax = ""

  specialSites = ["http://altc.senate.ca.gov", "http://srul.senate.ca.gov", "http://shea.senate.ca.gov",
                  "http://privacycp.assembly.ca.gov", "http://apia.senate.ca.gov", "http://defenseaero.senate.ca.gov"]

  htmlSoup = BeautifulSoup(urllib2.urlopen(commUrl).read())
  # assembly sites and senate sites have different formats
  # assembly site case
  if "assembly" in commUrl:
    content = htmlSoup.find_all("p")

    for c in content:
      text = c.get_text()
      #find respective contact info in text
      for line in text.split('\n'):
        if "Room" in line:
          start = line.find("Room")
          room = line[start + 4:]
          if "," in room:
            room = room.split(",")[0]
        if "phone" in line:
          start = line.find("phone")
          # case where phone in (xxx) xxx-xxxx format
          if "(" in line:
            phone = line[start - 16 : start - 1]
          #everyone else in xxx.xxx.xxxx format
          else:
            phone = line[start - 13 : start - 1]
        if "fax" in line:
          start = line.find("fax")
          fax = line[start - 13 : start - 1]
        #phone number for this site is after the word phone
        if commType == "Special":
          if "phone" in line.lower():
            start = line.lower().find("phone")
            phone = line[start + 7:]

  # senate or legislature site case
  if "senate" in commUrl or "legislature" in commUrl:
    # joint and select committees seem to have contact info in different spot
    content = htmlSoup.find_all("div", class_="content")
    for c in content:
      text = c.get_text()
      for line in text.split("\n"):
        if "Room" in line:
          start = line.find("Room")
          room = line[start + 4:]
        if "phone" in line.lower():
          start = line.lower().find("phone")
          phone = line[start + 7:]
        # case where the word phone is not in text
        if "(" in line and ")" in line and "-" in line:
          start = line.find("(")
          phone = line[start:]
        #case where phone and fax on the same line
        if "phone" in line.lower() and "fax" in line.lower():
          phone = line.lower().split("fax")
          phone = phone[0]
          start = phone.lower().find("phone")
          phone = phone[start+7:]
        if "fax" in line.lower():
          start = line.lower().find("fax")
          fax = line[start + 4:]
        # cause where no room number but suite address
        if "Suite" in line:
          start = line.find("Suite")
          room = line[start:]

  room = room.strip(".,")
  room = room.strip()
  phone = phone.strip(".,")
  phone = phone.strip()
  fax = fax.strip()
  return room, phone, fax


'''
update committee contact info if given

|cid|: committee id
|room|: string for room number
|phone|: string for phone
|fax|: string for fax

'''
def update_committee_contact(cursor, cid, room, phone, fax):
  if room != "":
    update_committee_room(cursor, cid, room)
  if phone != "":
    update_committee_phone(cursor, cid, phone)
  if fax != "":
    update_committee_fax(cursor, cid, fax)


'''
updates committee room
|cid|: string for committee id
|room|: string for room number
'''
def update_committee_room(cursor, cid, room):
  global CR_UPDATE
  try:
    cursor.execute(QU_COMMITTEE_ROOM, (room, cid))
    CR_UPDATE += cursor.rowcount
  except MySQLdb.Error:
    logger.warning('Update Failed', full_msg=traceback.format_exc(),
    additional_fields=create_payload('Committee',(QU_COMMITTEE_ROOM%(room, cid))))
    
'''
updates committee phone
|cid|: string for committee id
|phone|: string for phone
'''
def update_committee_phone(cursor, cid, phone):
  global CP_UPDATE
  try:
    cursor.execute(QU_COMMITTEE_PHONE, (phone, cid))
    CP_UPDATE += cursor.rowcount
  except MySQLdb.Error:
    logger.warning('Update Failed', full_msg=traceback.format_exc(),
    additional_fields=create_payload('Committee',(QU_COMMITTEE_PHONE%(phone, cid))))


'''
updates committee fax
|cid|: string for committee id
|fax|: string for fax
'''
def update_committee_fax(cursor, cid, fax):
  global CF_UPDATE
  try:
    cursor.execute(QU_COMMITTEE_FAX, (fax, cid))
    CF_UPDATE += cursor.rowcount
  except MySQLdb.Error:
    logger.warning('Update Failed', full_msg=traceback.format_exc(),
    additional_fields=create_payload('Committee',(QU_COMMITTEE_FAX%(fax, cid))))


'''
Scrapes committee web pages for committee information and adds it
to DDDB if it does not already exist.

|cursor|: database cursor
|house|: political house (Assembly or Senate)
|year|: year of term
|comm_count|: insert Committee count
|serve_count|: insert servesOn count

Returns insert counts
'''
def update_committees(cursor, house, year, comm_count, serve_count, pfinder, current_pid):
  cursor.execute(QS_TERM, (house, year-1, year, STATE))
  term_pids = [row[0] for row in cursor.fetchall()]
  # Special case for floor committee.
  room = ""
  phone = ""
  fax = ""
  floor_cid, comm_count = get_committee_id(cursor, house, '%s Floor' % house, "Floor", comm_count, room, phone, fax)
  #clean_servesOn(cursor, floor_cid, house, year)
  for pid in term_pids:
    serve_count = insert_serveson(cursor, pid, year, house, floor_cid, 'Member', serve_count)

  for url, name, commType in get_committees(house):
    #get committee root url
    commUrl = get_committee_url(url)
    # find their contact info
    room, phone, fax = get_committee_contact(commUrl, commType)

    # Joint committees are recorded with a house of 'Joint'.
    cid, comm_count = get_committee_id(cursor, 'Joint' if 'Joint' in name else house,
                           name, commType, comm_count, room, phone, fax)
    #clean_servesOn(cursor, cid, house, year)

    #update contact info in Committee table
    update_committee_contact(cursor, cid, room, phone, fax)

    for member in get_committee_members(url, house):
      cleanMember, position = get_member_position(member)
      pid = get_person_id(cursor, cleanMember, house, year,  pfinder)
      if pid is not None and pid in term_pids:
        serve_count = insert_serveson(cursor, pid, year, house, cid, position, serve_count)
        current_pid.append(pid)
      else:
        print "WARNING: Could not find {0} in DB".format(clean_name(member.split('(')[0]))

  return comm_count, serve_count, current_pid


'''
updates the end_date on servesOn for members that have a start_date, no end date, and not found in current_pid

|cursor|: database cursor
|current_pid|: list of current members found on the assembly, senate, and legislator sites
'''
def update_serveson(cursor, current_pid):
  count = 0
  cursor.execute(QS_SERVESON_2)
  if cursor.rowcount > 0:
    for row in cursor.fetchall():
      pid = row[0]
      if pid not in current_pid:
        try:
          today = time.strftime("%Y-%m-%d")
          cursor.execute(QU_SERVESON_ENDDATE, (today, pid))
          count = count + 1
        except MySQLdb.Error:
          logger.warning('Update Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('servesOn',(QU_SERVESON_ENDDATE%(today, pid))))

  return count
         

def main():
  dbinfo = mysql_connection(sys.argv) 
  with MySQLdb.connect(host=dbinfo['host'],
                       port=dbinfo['port'],
                       db=dbinfo['db'],
                       user=dbinfo['user'],
                       passwd=dbinfo['passwd'],
                       charset='utf8') as dd:
    comm_count = serve_count = 0
    year = datetime.datetime.now().year
    pfinder = Find_Person.FindPerson(dd, 'CA')
    current_pid = []
    for house in ['Assembly', 'Senate']:
      comm_count, serve_count, current_pid = update_committees(dd, house, year, comm_count, serve_count, pfinder, current_pid)
    logger.info(__file__ + ' terminated successfully.', 
        full_msg='Inserted ' + str(C_INSERT) + ' rows in Committee and inserted ' 
                  + str(S_INSERT) + ' and deleted ' + str(S_DELETE) + ' rows in servesOn',
        additional_fields={'_affected_rows':'Committee:'+str(C_INSERT)+
                                       ', servesOn:'+str(S_INSERT),
                           '_inserted':'Committee:'+str(C_INSERT)+
                                       ', servesOn:'+str(S_INSERT),
                           '_deleted':'servesOn:'+str(S_DELETE),
                           '_state':'CA',
                           '_log_type':'Database'})
    print 'Inserted %d entries to Committee'%comm_count
    print 'Inserted %d entries to servesOn'%serve_count

    # updates end dates for servesOn
    enddate_count = update_serveson(dd, current_pid)
    print 'Updated end_date for %d entries in servesOn'%enddate_count
    
if __name__ == '__main__':
  with GrayLogger(API_URL) as _logger:                                          
    logger = _logger
    main()
