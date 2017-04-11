#!/usr/bin/env python27
'''
File: Author_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan, Steven Thon, Eric Roh, Nick Russo
Date: 6/11/2015
Last Modified: 6/20/2016

Description:
- Inserts the authors from capublic.bill_version_authors_tbl into the 
  DDDB.authors or DDDB.committeeAuthors
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

from Database_Connection import mysql_connection
import traceback
import MySQLdb
from graylogger.graylogger import GrayLogger
import json
API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
logged_list = list()
LOG = {'authors' : {'inserted' : 0},
       'CommitteeAuthors' : {'inserted' : 0},
       'BillSponsors' : {'inserted' : 0}}
AU_INSERT = 0
CA_INSERT = 0
BS_INSERT = 0

# U.S. State
STATE = 'CA'
YEAR = 2017

# INSERTS
QI_AUTHORS = '''INSERT INTO authors (pid, bid, vid, contribution)
                VALUES (%s, %s, %s, %s)''' 
QI_COMMITTEEAUTHORS = '''INSERT INTO CommitteeAuthors (cid, bid, vid, state)
                         VALUES (%s, %s, %s, %s)'''
QI_BILLSPONSORS = '''INSERT INTO BillSponsors (pid, bid, vid, contribution)
                VALUES (%s, %s, %s, %s)''' 
QI_BILLSPONSORROLLS = '''INSERT INTO BillSponsorRolls (roll)
                VALUES (%s)''' 

# SELECTS
QS_COMMITTEEAUTHORS_CHECK = '''SELECT *
                               FROM CommitteeAuthors
                               WHERE cid = %s
                                AND bid = %s
                                AND vid = %s
                                AND state = %s'''
QS_COMMITTEE_GET = '''SELECT cid
                      FROM Committee
                      WHERE name = %s
                       AND house = %s
                       AND state = %s
                       AND current_flag = 1'''
QS_COMMITTEE_SHORT_GET = '''SELECT cid
                            FROM Committee
                            WHERE short_name = %s
                             AND house = %s
                             AND state = %s
                             AND current_flag = 1
                            '''

QS_BILLVERSION_BID = '''SELECT bid
                        FROM BillVersion
                        WHERE vid = %s
                         AND state = %s'''
QS_AUTHORS_CHECK = '''SELECT *
                      FROM authors
                      WHERE bid = %s
                       AND pid = %s
                       AND vid = %s'''
QS_BILLSPONSORS_CHECK = '''SELECT *
                           FROM BillSponsors
                           WHERE bid = %s
                            AND pid = %s
                            AND vid = %s
                            AND contribution = %s'''
QS_BILLSPONSORROLL_CHECK = '''SELECT *
                              FROM BillSponsorRolls
                              WHERE roll = %s'''
QS_BILL_VERSION_AUTHORS_TBL = '''SELECT DISTINCT bill_version_id, type, house, name,
                                  contribution, primary_author_flg
                                 FROM bill_version_authors_tbl'''
QS_LEGISLATOR_FL = '''SELECT p.pid, p.last, p.first
                      FROM Person p, Legislator l, Term t
                      WHERE p.pid = l.pid 
                      AND p.pid = t.pid 
                      AND p.last = %s
                      AND p.first = %s 
                      AND t.year = %s 
                      AND t.state = %s
                      AND t.house = %s
                      ORDER BY p.pid'''


QS_LEGISLATOR_L = '''SELECT p.pid, p.last, p.first
                     FROM Person p, Legislator l, Term t
                     WHERE p.pid = l.pid 
                     AND p.pid = t.pid 
                     AND p.last = %s 
                     AND t.year = %s 
                     AND t.state = %s
                     AND t.house = %s
                     ORDER BY p.pid'''
QS_TERM = '''SELECT pid, house 
             FROM Term 
             WHERE pid = %s 
              AND house = %s
              AND state = %s 
              AND year = %s
             ORDER BY Term.pid'''
QS_LEGISLATOR_LIKE_L = '''SELECT Person.pid, last, first
                          FROM Person, Legislator
                          WHERE Legislator.pid = Person.pid
                           AND last LIKE %s
                           AND state = %s
                          ORDER BY Person.pid'''

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA',
      '_log_type':'Database'
  }

'''
If the committee author for this bill is not in DDDB, add. Otherwise, skip.

|dd_cursor|: DDDB database cursor
|cid|: Committee id
|bid|: Bill id
|vid|: Bill Version id
'''
def add_committee_author(dd_cursor, cid, bid, vid):
  global CA_INSERT, LOG
  dd_cursor.execute(QS_COMMITTEEAUTHORS_CHECK, (cid, bid, vid, STATE))

  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_COMMITTEEAUTHORS, (cid, bid, vid, STATE))
      CA_INSERT += dd_cursor.rowcount
      LOG['CommitteeAuthors']['inserted'] += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('CommitteeAuthors', 
            (QI_COMMITTEEAUTHORS % (cid, bid, vid, STATE))))

'''
Cleans up the committee name if extraneous information is included.

|name|: Committee name

Returns the cleaned name.
'''
def clean_committee_name(name):
  # Removes the 'Committee on' string inside the capublic name
  if 'Committee on' in name:
    return ' '.join((name.split(' '))[2:])
  return name
'''
Attempts to get the committee.

|dd_cursor|: DDDB database cursor
|name|: Committee name
|house|: House (Assembly/Senate)

Returns the cid of the committee if found. Otherwise, return None.
'''
def get_committee(dd_cursor, name, house):
  house = house.title()                   # Titlecased for DDDB enum
  name = clean_committee_name(name)       # Clean name for checking
  
  dd_cursor.execute(QS_COMMITTEE_GET, (name, house, STATE))

  if dd_cursor.rowcount == 1:
    return dd_cursor.fetchone()[0]
  
  dd_cursor.execute(QS_COMMITTEE_SHORT_GET, (name, house, STATE))
  if dd_cursor.rowcount == 1:
    return dd_cursor.fetchone()[0]

  if name not in logged_list:
    logged_list.append(name)
    logger.warning('Committee not found ' + name, 
        full_msg=(QS_COMMITTEE_GET, (name, house, STATE)),
        additional_fields={'_state':'CA',
                           '_log_type':'Database'})
  return None

'''
Clean the name of the person and remove/replace weird characters.

|name|: Person's name to be cleaned

Returns the cleaned name.
'''
def clean_name(name):
  # Replaces all accented o's and a's
  if "\xc3\xb3" in name:
      name = name.replace("\xc3\xb3", "o")
  if "\xc3\xa1" in name:
      name = name.replace("\xc3\xa1", "a")
  
  if(name == 'Allen Travis'):
    name = 'Travis Allen'

  # For O'Donnell
  if 'Donnell' in name:
    name = "O'Donnell"
  
  # Removes positions and random unicode ? on Mark Stone's name
  name = name.replace("Vice Chair", "")
  name = name.replace("Chair", "")
  name = name.replace(chr(194), "")
  return name

'''
Find the Person using a combined name

|dd_cursor|: DDDB database cursor
|filer_naml|: Name of person
|house|: House (Senate/Assembly)
'''
def get_person(dd_cursor, filer_naml, house):
  pid = None
  filer_naml = clean_name(filer_naml)
  house = house.title()
  error_message = "Multiple matches for the same person: " 
  # First try last name.
  dd_cursor.execute(QS_LEGISLATOR_L, (filer_naml, YEAR, STATE, house))

  if dd_cursor.rowcount == 1:
    pid = dd_cursor.fetchone()[0]
  elif dd_cursor.rowcount == 0:
    parts = filer_naml.split(' ')
    if len(parts) > 1:
        dd_cursor.execute(QS_LEGISLATOR_FL, (parts[1:], parts[0], YEAR, STATE, house))
        if dd_cursor.rowcount == 1:
            pid = dd_cursor.fetchone()[0]
    else:
        filer_naml = '%' + filer_naml + '%'
        dd_cursor.execute(QS_LEGISLATOR_LIKE_L, (filer_naml, STATE))
        if(dd_cursor.rowcount == 1):
            pid = dd_cursor.fetchone()[0]
  else:
       error_message = "Person not found "
  if pid is None and filer_naml not in logged_list:
    logged_list.append(filer_naml)
    logger.warning(error_message + filer_naml,
        additional_fields={'_state':'CA'})
  return pid

'''
Finds the bid associated with the bill version. 

|dd_cursor|: DDDB database cursor
|vid|: Bill Version id

If bill is found, return the bid. Otherwise, return None.
'''
def get_bid(dd_cursor, vid):
  dd_cursor.execute(QS_BILLVERSION_BID, (vid, STATE))

  if dd_cursor.rowcount > 0:
    return dd_cursor.fetchone()[0]
  if vid not in logged_list:
    logged_list.append(vid)
    logger.warning('BillVersion not found '+vid, 
        full_msg=(QS_BILLVERSION_BID, (vid, STATE)),
        additional_fields={'_state':'CA'})
  return None

'''
If the author for this bill is not in DDDB, add author. Otherwise, skip.

|dd_cursor|: DDDB database cursor
|pid|: Person id
|bid|: Bill id
|vid|: Bill Version id
|contribution|: How the person contributed to the bill (ex: Lead Author)
'''
def add_author(dd_cursor, pid, bid, vid, contribution):
  global AU_INSERT, LOG
  dd_cursor.execute(QS_AUTHORS_CHECK, (bid, pid, vid))

  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_AUTHORS, (pid, bid, vid, contribution))
      AU_INSERT += dd_cursor.rowcount
      LOG['authors']['inserted'] += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Authors', 
            (QI_AUTHORS % (pid, bid, vid, contribution))))

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
  global BS_INSERT, LOG
  dd_cursor.execute(QS_BILLSPONSORROLL_CHECK, (contribution,))

  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_BILLSPONSORROLLS, (contribution,))
    except MySQLdb.Error as error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('BillSponsorRolls', 
          (QI_BILLSPONSORROLLS % (contribution,))))

  dd_cursor.execute(QS_BILLSPONSORS_CHECK, (bid, pid, vid, contribution))

  if dd_cursor.rowcount == 0:
    try:
      dd_cursor.execute(QI_BILLSPONSORS, (pid, bid, vid, contribution))
      BS_INSERT += dd_cursor.rowcount
      LOG['BillSponsors']['inserted'] += dd_cursor.rowcount
    except MySQLdb.Error as error:                                              
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('BillSponsors', 
          (QI_BILLSPONSORS % (pid, bid, vid, contribution))))

'''
Grabs capublic's information and selectively adds bill authors into DDDB.
Authors are only added if they are the primary lead author of the bill. 
Also, bill authors can be either Legislators or Committees.

|ca_cursor|: capublic database cursor
|dd_cursor|: DDDB database cursor
'''
def get_authors(ca_cursor, dd_cursor):
  ca_cursor.execute(QS_BILL_VERSION_AUTHORS_TBL)
  rows = ca_cursor.fetchall()

  # Iterate over each bill author row in capublic
  for vid, author_type, house, name, contrib, prim_author_flg in rows:
    vid = '%s_%s' % (STATE, vid)
    bid = get_bid(dd_cursor, vid)
    contribution = contrib.title().replace('_', ' ')

    # IF bid in database and is a Legislator add to BillSponsors
    if bid is not None and author_type == 'Legislator':
      pid = get_person(dd_cursor, name, house)
      if pid is not None:
        add_sponsor(dd_cursor, pid, bid, vid, contribution)

    # Check if the bill is in DDDB. Otherwise, skip
    if bid is not None and prim_author_flg == 'Y':
      # Legislator Authors
      if author_type == 'Legislator':
        pid = get_person(dd_cursor, name, house)
        if pid is not None:
          add_author(dd_cursor, pid, bid, vid, contribution)

      # Committee Authors
      elif author_type == 'Committee':
        cid = get_committee(dd_cursor, name, house)
        if cid is not None:
          add_committee_author(dd_cursor, cid, bid, vid)

def main():
  import sys
  dbinfo = mysql_connection(sys.argv)
  with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd']) as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='capublic',
                       passwd='python') as ca_cursor:
      get_authors(ca_cursor, dd_cursor)
      logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(AU_INSERT) + ' rows in authors, ' 
                    + str(BS_INSERT) + ' rows in BillSponsors and ' 
                    + str(CA_INSERT) + ' rows in CommitteeAuthors.',
          additional_fields={'_affected_rows':'authos:'+str(AU_INSERT)+
                                         ', BillSponsors:'+str(BS_INSERT)+
                                         ', CommitteeAuthors:'+str(CA_INSERT),
                             '_inserted':'authors:'+str(AU_INSERT)+
                                         ', BillSponsors:'+str(BS_INSERT)+
                                         ', CommitteeAuthors:'+str(CA_INSERT),
                             '_state':'CA',
                             '_log_type':'Database'})  
  LOG = {'tables': [{'state': 'CA', 'name': 'authors', 'inserted':AU_INSERT, 'updated': 0, 'deleted': 0},
    {'state': 'CA', 'name': 'BillSponsors', 'inserted':BS_INSERT, 'updated': 0, 'deleted': 0},
    {'state': 'CA', 'name': 'CommitteeAuthors', 'inserted':CA_INSERT, 'updated': 0, 'deleted': 0}]}
  sys.stderr.write(json.dumps(LOG))

if __name__ == '__main__':
  with GrayLogger(API_URL) as _logger:
    logger = _logger
    main()

