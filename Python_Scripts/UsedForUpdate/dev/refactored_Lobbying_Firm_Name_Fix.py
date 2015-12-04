#!/usr/bin/env python2.6
'''
File: Lobbying_Firm_Name_Fix.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Used to correct names of Lobbying Firms gathered during the Lobbying Info
- Used as an import to the Cal-Access-Accessor.py to clean Lobbying Firm Names
'''

import loggingdb

from clean_name import clean_name

QS_LOBBYING_FIRM = '''SELECT filer_id, filer_naml
                      FROM LobbyingFirm'''
QS_ORGANIZATIONS = '''SELECT oid, name
                      FROM Organizations'''
QU_LOBBYING_FIRM = '''UPDATE LobbyingFirm
                      SET filer_naml = %s
                      WHERE filer_id = %s'''
QU_ORGAINIZATIONS = '''UPDATE Organizations
                       SET name = %s
                       WHERE oid = %s'''
def clean(index, name):
  if name in ['LLC', 'LLP', 'LP']:
    return name
  return name.title()

def cleanNames(select_query, update_query):
  '''Cleans names.
  |select_query|: A query that returns tuples of id and name, in that order.
  |update_query|: A query that updates names by id.
  '''
  with loggingdb.connect(host='transcription.digitaldemocracy.org',
                         user='monty',
                         db='MultiStateTest',
                         passwd='python') as dd_cursor:
    dd_cursor.execute(select_query)
    for id_, name in dd_cursor.fetchall():
      cleaned_name = clean_name(name, clean)
      if cleaned_name == name:
        # Name was already clean.
        continue
      print('Original: %s, Clean: %s' % (name, cleaned_name))
      dd_cursor.execute(update_query, (id_, cleaned_name))

if __name__ == "__main__":
  cleanNames(QS_LOBBYING_FIRM, QU_LOBBYING_FIRM)
  cleanNames(QS_ORGANIZATIONS, QU_ORGANIZATIONS)
