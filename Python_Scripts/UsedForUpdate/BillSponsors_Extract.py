#!/usr/bin/env python2.6
'''
File: BillSponsors_Extract.py
Author: Eric Roh
Date: 3/4/2016

Description:
- Inserts the BillSponsors from capublic.bill_version_authors_tbl into the 
  DDDB.BillSponsors or DDDB.BillSponsorRolls
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
  - BillSponsors (pid, bid, vid, contribution)
  - BillSponsorRolls (contribution)

'''

import MySQLdb

#import loggingdb

# U.S. State
STATE = 'CA'

# INSERTS
QI_BILLSPONSORS = '''INSERT INTO BillSponsors (pid, bid, vid, contribution)
                VALUES (%s, %s, %s, %s)''' 
QI_BILLSPONSORROLLS = '''INSERT INTO BillSponsorRolls (roll)
                VALUES (%s)'''

# SELECTS
QS_BILL_VERSION_AUTHORS_TBL = '''SELECT DISTINCT bill_version_id, type, house, name,
                                  contribution, primary_author_flg
                                 FROM bill_version_authors_tbl
                                 WHERE type = "Legislator"'''
QS_BILL_ROLLS = '''SELECT DISTINCT contribution
                   FROM bill_version_authors_tbl'''
QS_BILLVERSION_BID = '''SELECT bid
                        FROM BillVersion
                        WHERE vid = %s
                         AND state = %s'''
QS_BILLSPONSORS_CHECK = '''SELECT *
                           FROM BillSponsors
                           WHERE bid = %s
                            AND pid = %s
                            AND vid = %s'''
QS_BILLSPONSORROLL_CHECK = '''SELECT *
                              FROM BillSponsorRolls
                              WHERE roll = %s'''
QS_LEGISLATOR_FL = '''SELECT Person.pid, last, first
               FROM Person, Legislator 
               WHERE Legislator.pid = Person.pid 
                AND last = %s 
                AND first = %s
               ORDER BY Person.pid''' 
QS_LEGISLATOR_L = '''SELECT Person.pid, last, first 
                     FROM Person, Legislator 
                     WHERE Legislator.pid = Person.pid 
                      AND last = %s 
                     ORDER BY Person.pid'''
QS_TERM = '''SELECT pid, house 
             FROM Term 
             WHERE pid = %s 
              AND house = %s
              AND state = %s 
             ORDER BY Term.pid'''
QS_LEGISLATOR_LIKE_L = '''SELECT Person.pid, last, first
                          FROM Person, Legislator
                          WHERE Legislator.pid = Person.pid
                           AND last LIKE %s
                          ORDER BY Person.pid'''


'''
Clean the name of the person and remove/replace weird characters.

|name|: Person's name to be cleaned

Returns the cleaned name.
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

|dd_cursor|: DDDB database cursor
|filer_naml|: Name of person
|house|: House (Senate/Assembly)
'''
def get_person(dd_cursor, filer_naml, house):
  pid = None
  filer_naml = clean_name(filer_naml)
  temp = filer_naml.split(' ')
  filer_namf = ''
  house = house.title()

  # Checks if there is a first and last name or just a first
  if(len(temp) > 1):
    filer_naml = temp[len(temp)-1]
    filer_namf = temp[0]
    dd_cursor.execute(QS_LEGISLATOR_FL, (filer_naml, filer_namf))
  else:
    dd_cursor.execute(QS_LEGISLATOR_L, (filer_naml,))

  # If it finds a match of the exact name, use that
  if dd_cursor.rowcount == 1:
    pid = dd_cursor.fetchone()[0]
  # If there is more than one, have to use the house
  elif dd_cursor.rowcount > 1:
    a = [t[0] for t in dd_cursor.fetchall()]
    end = dd_cursor.rowcount
    # Find which person it is using their term
    for j in range(0, end):
      dd_cursor.execute(QS_TERM, (a[j], house, STATE))
      if(dd_cursor.rowcount == 1):
        pid = dd_cursor.fetchone()[0]

  # If none were found, loosen the search up a bit and just look for last name
  else:
    filer_naml = '%' + filer_naml + '%'
    dd_cursor.execute(QS_LEGISLATOR_LIKE_L, (filer_naml,))
    if(dd_cursor.rowcount == 1):
      pid = dd_cursor.fetchone()[0]

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
  return None

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
  dd_cursor.execute(QS_BILLSPONSORS_CHECK, (bid, pid, vid))

  if dd_cursor.rowcount == 0:
#    print pid, vid, contribution
    dd_cursor.execute(QI_BILLSPONSORS, (pid, bid, vid, contribution))

'''
Grabs capublic's information and selectively adds bill sponsors into DDDB. 
Also, bill authors can be either Legislators or Committees.

|ca_cursor|: capublic database cursor
|dd_cursor|: DDDB database cursor
'''
def get_bill_sponsors(ca_cursor, dd_cursor):
  ca_cursor.execute(QS_BILL_VERSION_AUTHORS_TBL)
  print 'row count', ca_cursor.rowcount
  rows = ca_cursor.fetchall()
  print len(rows)
  counter = 0

  # Iterate over each bill author row in capublic
  for vid, author_type, house, name, contrib, prim_author_flg in rows:
    if counter % 1000 == 0:
      print counter
#      dd_cursor.flush()
#      print 'flush data'
    counter += 1
#    print counter       
    vid = '%s_%s' % (STATE, vid)
    bid = get_bid(dd_cursor, vid)
    contribution = contrib.title().replace('_', ' ')

    # IF bid in database and is a Legislator add to BillSponsors
    if bid is not None:
      pid = get_person(dd_cursor, name, house)
      if pid is not None:
        add_sponsor(dd_cursor, pid, bid, vid, contribution)
      else:
        print 'pid', pid, 'not found for', name

def get_bill_rolls(ca_cursor, dd_cursor):
  ca_cursor.execute(QS_BILL_ROLLS)
  rows = ca_cursor.fetchall()


  for contrib in rows:
    print contrib[0]
    contribution = contrib[0].title().replace('_', ' ')
    dd_cursor.execute(QS_BILLSPONSORROLL_CHECK, (contribution,))

    if dd_cursor.rowcount == 0:
      dd_cursor.execute(QI_BILLSPONSORROLLS, (contribution,))


def main():
  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                         port=3306,
                         db='EricTest',
                         user='awsDB',
                         passwd='digitaldemocracy789') as dd_cursor:
    with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       user='monty',
                       db='capublic',
                       passwd='python') as ca_cursor:
      get_bill_rolls(ca_cursor, dd_cursor)
      get_bill_sponsors(ca_cursor, dd_cursor)

if __name__ == '__main__':
  main()  