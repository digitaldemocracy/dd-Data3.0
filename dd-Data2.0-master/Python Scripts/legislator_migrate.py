import mysql.connector
from lxml import etree 
import re
import sys

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
   print (querystring)
   result = cursor.execute(querystring, {'pid':pid})
   print (result)
   return cursor.fetchone()

def check_term(cursor, pid, year, district, house):
   print 'pid={0},year={1},district={2},house={3}'.format(pid,year,district,house)
   print (query_term)
   result = cursor.execute(query_term, (pid, year))
   pid = cursor.fetchone()
   print (pid)
   return pid

def check_name(cursor, last, first):
   print 'here'
   result = cursor.execute(query_person, (last, first))
   print 'there'
   return cursor.fetchone()

conn = mysql.connector.connect(user="root", database="capublic", password="")
capublic = conn.cursor(buffered=True)

conn2 = mysql.connector.connect(user="root", database="DDDB2015Test", password="")
dd = conn2.cursor(buffered=True)
result = check_pid(dd, query_legislator, 1)
print (result)

new_members = []
capublic.execute(query)
try:
   for (last, first, year, district, house, party, active) in capublic:
      print 'checking {0}, {1}'.format(last, first)
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
      print(exist)
      print(active)
      if exist is None:
         print 'new member'
         result = dd.execute(query_insert_person, (last, first))
         print (result)
         print 'appending to the list..'
         new_members.append((last,first,year,district,house,party,active))
      else:
         print 'This person exists!'
         pid = exist[0]
         result = check_pid(dd, query_legislator, pid)
         if result is None and active == "Y":
            dd.execute(query_insert_legislator, {'pid':pid})
         result = check_term(dd,pid,year,district,house)
         if result is None and active == "Y":
            print 'inserting Term pid={0},year={1},district={2},house={3},party={4}'.format(pid,year,district,house,party)
            result = dd.execute(query_insert_term, (pid, year, district, house, party))
            print (result)

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
         dd.execute(query_insert_legislator, {'pid':pid[0]})
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
