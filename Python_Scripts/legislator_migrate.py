'''
File: legislator_migrate.py
Author: ???
Date: 6/11/2015

Description:
- Gathers Legislator Data from capublic.legislator_tbl and inserts the data into DDDB2015Apr.Person, DDDB2015Apr.Legislator, and DDDB2015Apr.Term
- Used in the daily update of DDDB2015Apr
- Fills table:
	Person (last, first)
	Legislator (pid)
	Term (pid, year, district, house, party)

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
	- legislator_tbl
'''

import mysql.connector
from lxml import etree 
import re
import sys
import Name_Fixes_Legislator_Migrate

query = 'SELECT last_name, first_name, SUBSTRING(session_year, 1, 4), CONVERT(SUBSTRING(district, -2), UNSIGNED), house_type, party, active_legislator FROM legislator_tbl'
query_person = 'SELECT pid FROM Person WHERE last = %s AND first = %s'
query_insert_person = 'INSERT INTO Person (last, first) VALUES (%s, %s)'
query_insert_legislator = 'INSERT INTO Legislator (pid) VALUES (%(pid)s)'
query_insert_term = 'INSERT INTO Term (pid, year, district, house, party) VALUES (%s, %s, %s, %s, %s)'
query_update_term = 'UPDATE Term SET year=%s, district=%s, house=%s, party=%s WHERE pid=%s'
query_legislator = 'SELECT pid FROM Legislator WHERE pid = %(pid)s'
query_term = 'SELECT * FROM Term WHERE pid=%s AND year=%s'

def check_pid(cursor, querystring, pid):
   print 'checking pid={0}..'.format(pid)
   result = cursor.execute(querystring, {'pid':pid})
   print (result)
   return cursor.fetchone()

def check_term(cursor, pid, year, district, house):
   print 'pid={0},year={1},district={2},house={3}'.format(pid,year,district,house)
   result = cursor.execute(query_term, (pid, year))
   pid = cursor.fetchone()
   return pid

def check_name(cursor, last, first):
   name = Name_Fixes_Legislator_Migrate.clean_name_legislator_migrate(last, first).split("<SPLIT>")
   first = name[0]
   last = name[1]
   cursor.execute(query_person, (last, first))
   return cursor.fetchone()

conn = mysql.connector.connect(user="root", database="capublic", password="")
capublic = conn.cursor(buffered=True)

conn2 = mysql.connector.connect(user="root", database="DDDB2015AprTest", password="")
dd = conn2.cursor(buffered=True)

new_members = []
capublic.execute(query)
try:
   for (last, first, year, district, house, party, active) in capublic:
      if house == "A":
            house = "Assembly"
      if house == "S":
            house = "Senate"
      if party == "REP":
            party = "Republican"
      if party == "DEM":
            party = "Democrat"
      print 'house:{0}, party:{1}'.format(house, party) 
      exist = check_name(dd, last, first)
      if exist is None:
         print 'new member'
         result = dd.execute(query_insert_person, (last, first))
         print 'inserting Person {0} {1}".format(first, last)
         new_members.append((last,first,year,district,house,party,active))
      else:
         pid = exist[0]
         result = check_pid(dd, query_legislator, pid)
         if result is None and active == "Y":
            dd.execute(query_insert_legislator, {'pid':pid})
         result = check_term(dd,pid,year,district,house)
         if result is None and active == "Y":
            result = dd.execute(query_insert_term, (pid, year, district, house, party))

   conn2.commit();
except:
   conn2.rollback()
   print 'error!', sys.exc_info()[0]
   exit()

try:
   for (last, first, year, district, house, party, active) in new_members:
      print 'getting pid for {0}, {1}'.format(last, first)
      pid = check_name(dd, last, first)
      if pid is not None and active == "Y":
         print 'inserting Legislator {0}".format(pid)
         dd.execute(query_insert_legislator, {'pid':pid[0]})
         print 'inserting Term {0} {1} {2} {3} {4}".format(pid, year, district, house, party)
         dd.execute(query_insert_term, (year, district, house, party, pid[0]))
      else:
         print 'this person exists!? pid={0}'.format(pid[0])

   conn2.commit()      
except:
   conn2.rollback()
   print 'error!', sys.exc_info()[0]
   exit()
   
conn2.close()
conn.close()
