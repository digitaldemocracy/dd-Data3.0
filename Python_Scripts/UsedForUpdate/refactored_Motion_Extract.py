#!/usr/bin/env python
'''
File: Motion_Extract.py
Author: Daniel Mangin
Date: 6/11/2015

Description:
- Gathers the Motions from capublic.bill_motion_tbl and inserts the Motions into DDDB.Motion
- Used in the daily update of DDDB
- Fills table:
  Motion (mid, date, text)

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
  - bill_motion_tbl
'''

import loggingdb
import MySQLdb

QI_MOTION = '''INSERT INTO Motion (mid, date, text, doPass) 
               VALUES (%s, %s, %s, %s)'''
QS_MOTION = '''SELECT mid
               FROM Motion 
               WHERE mid = %(mid)s 
                AND date = %(date)s'''
QS_CPUB_MOTION = '''SELECT motion_id, motion_text, trans_update
                    FROM bill_motion_tbl'''

# Insert the Motion row into DDDB if none is found
def insert_motion(cursor, mid, date, text):
  cursor.execute(QS_MOTION, {'mid':mid, 'date':date})
  if(cursor.rowcount == 0):
    do_pass_flag = 1 if 'do pass' in text.lower() else 0
    cursor.execute(QI_MOTION, (mid, date, text, do_pass_flag))  

def get_motions():
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python') as ca_cursor:
    with loggingdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                           port=3306,
                           db='DDDB2015Dec',
                           user='awsDB',
                           passwd='digitaldemocracy789') as dddb_cursor:
      ca_cursor.execute(QS_CPUB_MOTION)
      for mid, text, update in ca_cursor.fetchall():
        date = update.strftime('%Y-%m-%d %H:%M:%S')
        if date:
          insert_motion(dddb_cursor, mid, date, text)

if __name__ == "__main__":
  get_motions() 
