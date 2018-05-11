#!/usr/bin/python3
'''
File: Lobbying_Firm_Name_Fix.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used to correct names of Lobbying Firms gathered during the Lobbying Info
- Used as an import to the Cal-Access-Accessor.py to clean Lobbying Firm Names
'''


#import loggingdb
import MySQLdb
import re
import sys

from clean_name import clean_name

def clean(index, name):
  if name in ['LLC', 'LLP', 'LP']:
    return name
  return name.title()

def cleanNames(select_query, update_query):
  '''Cleans names.
  |select_query|: A query that returns tuples of id and name, in that order.
  |update_query|: A query that updates names by id.
  '''
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='DDDB2015JulyTest',
                         passwd='python') as dd_cursor:
    dd_cursor.execute(select_query)
    rows = dd_cursor.fetchall()
    for (id_, name) in rows:
      cleaned_name = clean_name(name, clean)
      if cleaned_name == name:
        # Name was already clean.
        continue
      print('Original: %s, Clean: %s' % (name, cleaned_name))
      dd_cursor.execute(update_query, (id_, cleaned_name))

if __name__ == "__main__":
  cleanNames('SELECT filer_id, filer_naml from LobbyingFirm',
             '''UPDATE LobbyingFirm
                SET filer_naml = %s
                WHERE filer_id = %s''')
  cleanNames('SELECT oid, name FROM Organizations',
             '''UPDATE Organizations
                SET name = %s
                WHERE oid = %s''')
