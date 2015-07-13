'''
File: Fix_Bill_Version_State.py
Author: Mandy Chan
Date: 7/12/2015

Problem:
  - Bill Version statuses used to pull their statuses from the Bill's status

Solution:
  - Fixed 'Bill_Extract.py' to pull the statuses from bill_version_tbl
  - Using this script as a (hopefully) one-time run through of DDDB2015Apr's
    database to update BillVersion table

Sources:
  - capublic
    - bill_version_tbl

Affects:
  - DDDB
    - BillVersion
'''

import sys
import mysql.connector
from pprint import pprint

# Queries
select_stmt = '''SELECT bill_version_id,
                        bill_version_action
                 FROM bill_version_tbl
              '''

update_stmt = '''UPDATE BillVersion
                 SET state = %(state)s
                 WHERE vid = %(vid)s
              '''

# Connections
capublic = mysql.connector.connect(user = 'root', db = 'capublic', password = '')
cp_cursor = capublic.cursor(buffered = True)

dddb = mysql.connector.connect(user = 'root', db = 'DDDB2015Apr', password = '')
dd_cursor = dddb.cursor(buffered = True)

def fix():
  try:
    cp_cursor.execute(select_stmt)
    rows = cp_cursor.fetchall()

    for row in rows: 
      vid, state = row
      dd_cursor.execute(update_stmt, {'vid':vid, 'state':state})
    
    dddb.commit()
  except:
    print("Something happened!")
    dddb.rollback()
    print("Error!", sys.exc_info()[0], sys.exc_info()[1])

def main():
  print("Fixing Bill Version Statuses")
  fix()
  print("Fixed")

if __name__ == "__main__":
  main()
