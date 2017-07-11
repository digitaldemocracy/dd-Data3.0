#!/usr/bin/env python
'''
File: insert_Contributions_CSV.py
Author: Daniel Mangin & Mandy Chan
Date: 6/11/2015

Description:
- Gathers Contribution Data and puts it into DDDB2015.Contributions
- Used once for the Insertion of all the Contributions
- Fills table:
  Contribution (id, pid, year, date, house, donorName, donorOrg, amount)

Sources:
- Maplight Data
  - cand_2001.csv
  - cand_2003.csv
  - cand_2005.csv
  - cand_2007.csv
  - cand_2009.csv
  - cand_2011.csv
  - cand_2013.csv
  - cand_2015.csv
'''
import os
import csv
import sys
import json
import urllib
import MySQLdb
import zipfile
import traceback
import subprocess
import contextlib
from datetime import datetime
from Utils.Generic_Utils import *
from Utils.Database_Connection import *

logger = None
LOG = {'Contribution': {'inserted': 0}}
C_INSERT = 0

#queries used for insertion
QI_CONTRIBUTION = '''INSERT INTO Contribution 
                     (id, pid, year, date, house, donorName, donorOrg, amount, state, oid) 
                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'CA', %s)'''
#QI_CONTRIBUTION = '''INSERT INTO Contribution 
#                     (id, pid, year, date, house, donorName, donorOrg, amount) 
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''

# SELECT
QS_CONTRIBUTION_CHECK = '''SELECT * 
                           FROM Contribution
                           WHERE id = %s
                            AND pid = %s
                            AND year = %s
                            AND date = %s
                            AND house = %s
                            AND donorName = %s
                            AND donorOrg = %s
                            AND amount = %s
                            AND state = %s
                            AND oid = %s'''
QS_CONTRIBUTION_CHECK = '''SELECT * 
                           FROM Contribution
                           WHERE id = %s'''
QS_ORGANIZATION = '''SELECT oid
                     FROM Organizations
                     WHERE name = %s'''

# DELETE
QD_CONTRIBUTION = '''DELETE FROM Contribution
                     WHERE id = %s'''

zipURL = '''http://data.maplight.org/CA/{0}/records/cand.zip'''
zipName = '''cand.zip'''

def dl_csv():
  remove_zip()
  try:
    url = urllib.urlretrieve(zipURL, zipName)
    print('url', zipURL)
  except:
    print zipURL, 'download failed'

  with contextlib.closing(zipfile.ZipFile(zipName, 'r')) as z:
    z.extractall()

  remove_zip()

def remove_zip():
  if os.path.exists('./' + zipName):
    subprocess.call('rm -f ' + zipName, shell=True)

def checkLegislator(cursor, pid):
  select_pid = "SELECT pid FROM Legislator WHERE pid = %(pid)s ORDER BY Legislator.pid;"
  cursor.execute(select_pid, {'pid':pid})
  if cursor.rowcount > 0:
    return pid
  else:
    return -1

def getHouse(cursor, pid):
  select_pid = "SELECT house FROM Legislator, Term WHERE Legislator.pid = Term.pid AND Legislator.pid = %(pid)s;"
  cursor.execute(select_pid, {'pid':pid})
  if cursor.rowcount == 1:
    return cursor.fetchone()[0]

def getPerson(cursor, first, last, floor):
  pid = -1
  #print "{0} {1}".format(first, last)
  first = '%' + first + '%'
  select_pid = "SELECT Person.pid, last, first FROM Person, Legislator WHERE Legislator.pid = Person.pid AND last = %(last)s AND first LIKE %(first)s ORDER BY Person.pid;"
  #print select_pid
  cursor.execute(select_pid, {'last':last,'first':first})
  #print cursor.rowcount
  if cursor.rowcount == 1:
    pid = cursor.fetchone()[0]
  elif cursor.rowcount > 1:
    #print "found more"
    a = []
    for j in range(0, cursor.rowcount):
      temp = cursor.fetchone()
      a.append(temp[0])
    for j in range(0, len(a)):
      select_term = "SELECT pid, house FROM Term WHERE pid = %(pid)s ORDER BY Term.pid;"
      cursor.execute(select_term, {'pid':a[j],'house':floor})
      if(cursor.rowcount == 1):
        pid = cursor.fetchone()[0]
      else:
        print "Too many duplicates"
  else:
    last = '%' + last + '%'
    select_pid = "SELECT Person.pid, last, first FROM Person, Legislator WHERE Legislator.pid = Person.pid AND  last LIKE %(last)s AND first LIKE %(first)s ORDER BY Person.pid;"
    cursor.execute(select_pid, {'last':last,'first':first})
    if(cursor.rowcount > 0):
      pid = cursor.fetchone()[0]
    else:
      select_pid = "SELECT Person.pid, last, first FROM Person, Legislator WHERE Legislator.pid = Person.pid AND  last LIKE %(last)s ORDER BY Person.pid;"
      cursor.execute(select_pid, {'last':last})
      if(cursor.rowcount == 1):
        pid = cursor.fetchone()[0]
      else:
        #print "could not find {0} {1}".format(first, last)
        pass
  if pid == -1:
    print(first, last, floor)
  return pid


notfound_org = set()
'''
Using direct matching for name
Should be changed later for more results
'''
def get_oid(cursor, donorOrg):
  global notfound_org
  cursor.execute(QS_ORGANIZATION, (donorOrg,))

  if cursor.rowcount == 1:
    oid = cursor.fetchone()[0]
    #print 'found oid: ', oid
    return oid
  print('did nto find donorOrg',donorOrg)
  notfound_org.add(donorOrg)
  return None

def insert_Contributor(cursor, id, pid, year, date, house, donorName, donorOrg, amount, oid):
  global C_INSERT, LOG
  cursor.execute(QS_CONTRIBUTION_CHECK, (id,))
  print(cursor.fetchall())
  if cursor.rowcount == 1:
    cursor.execute(QD_CONTRIBUTION, (id,))
  #else:
  #  print(id, pid, year, date, house, donorName, donorOrg, amount, oid)
  cursor.execute(QI_CONTRIBUTION, (id, pid, year, date, house, donorName, donorOrg, amount, oid))
  C_INSERT += cursor.rowcount
  LOG['Contribution']['inserted'] += cursor.rowcount

#db = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
#conn = db.cursor(buffered = True)

def getContributions(file, conn):
  with open(file, 'rb') as tsvin:
      tsvin = csv.reader(tsvin, delimiter=',')
      next(tsvin, None)
      val = 0
      index = 0
      row_in_csv = 1

      for row in tsvin:
        #print index
        #print row_in_csv
        row_in_csv += 1

        try:
          year = row[1]
          id = row[4]
          if len(id) > 25:
            print id
          date = row[5]
          amount = row[6]
          house = row[13]
          name = row[9]
          first = name.split(', ')[1]
          first = first.title()
          temp = first.split(' ')
          temp2 = []
          for i in range(0, len(temp)):
            if not '.' in temp[i]:
              temp2.append(temp[i])
          first = ' '.join(temp2)
          last = name.split(',')[0]
          last = last.title()
          temp = last.split(' ')
          temp2 = []
          for i in range(0, len(temp)):
            if not '.' in temp[i]:
              temp2.append(temp[i])
          last = ' '.join(temp2)
          if "Assembly" in house:
            house = "Assembly"
          elif "Senate" in house:
            house = "Senate"
          else:
            house = "null"
          district = row[14]
          donorName = row[15]
          donorOrg = row[21]
          pid = getPerson(conn, first, last, house)
          oid = get_oid(conn, donorOrg)
          
          if house == "null" and pid != -1:
            house = getHouse(conn, pid)

          if house != "null" and pid != -1:
            try:
                print('gonna insert', id, oid)
                insert_Contributor(conn, id, pid, year, date, house, donorName, donorOrg, amount, oid)
            except:
                print(traceback.format_exc())
                raise
          else:
            val = val + 1
            #print 'did not go in successfully'
            #print "house: {0} pid: {1}".format(house, pid)
        except IndexError:
          #print traceback.format_exc()
          pass
          #print row[9]
          #print row
        except:
          #If it says 'list index out of range', it's probably an empty name (row 9) and is ignored
          #print 'error!', sys.exc_info()[0], sys.exc_info()[1]
          print traceback.format_exc()
          #exit(0)
      print 'no pid and house', val

def main():
  global zipURL
  with connect() as dd_cursor:
    year = datetime.now().year
    sessionyear = year + 1 if year % 2 == 0 else year
    zipURL = zipURL.format(sessionyear)
    dl_csv()
    getContributions('cand_{0}.csv'.format(sessionyear), dd_cursor)

  LOG = {'tables': [{'state': 'CA', 'name': 'Contribution', 'inserted':C_INSERT, 'updated': 0, 'deleted': 0}]}
  sys.stderr.write(json.dumps(LOG))
  logger.info(LOG)
if __name__ == "__main__":
    logger = create_logger()
    main()

