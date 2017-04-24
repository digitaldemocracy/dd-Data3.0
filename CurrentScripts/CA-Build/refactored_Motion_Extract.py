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

import json
from Database_Connection import mysql_connection
import traceback
import MySQLdb
import sys
from graylogger.graylogger import GrayLogger
API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
INSERTED = 0

QI_MOTION = '''INSERT INTO Motion (mid, text, doPass) 
               VALUES (%s, %s, %s)'''
QS_MOTION = '''SELECT mid
               FROM Motion 
               WHERE mid = %(mid)s'''
QS_CPUB_MOTION = '''SELECT DISTINCT motion_id, motion_text, trans_update
                    FROM bill_motion_tbl'''

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA',
      '_log_type':'Database'
  }


# Insert the Motion row into DDDB if none is found
def insert_motion(cursor, mid, date, text):
  global INSERTED
  cursor.execute(QS_MOTION, {'mid':mid})
  if(cursor.rowcount == 0):
    do_pass_flag = 1 if 'do pass' in text.lower() else 0
    try:
      cursor.execute(QI_MOTION, (mid, text, do_pass_flag))
      INSERTED += cursor.rowcount
    except MySQLdb.Error as error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Motion', 
            (QI_MOTION % (mid, text, do_pass_flag))))

def get_motions():
  dbinfo = mysql_connection(sys.argv) 
  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                       db='capublic',
                       user='monty',
                       passwd='python'
                    ) as ca_cursor:
    with MySQLdb.connect(host=dbinfo['host'],
                           port=dbinfo['port'],
                           db=dbinfo['db'],
                           user=dbinfo['user'],
                           passwd=dbinfo['passwd']) as dddb_cursor:
      ca_cursor.execute(QS_CPUB_MOTION)
      for mid, text, update in ca_cursor.fetchall():
        date = update.strftime('%Y-%m-%d %H:%M:%S')
        if date:
          insert_motion(dddb_cursor, mid, date, text)
      logger.info(__file__ + ' terminated successfully.', 
          full_msg='inserted ' + str(INSERTED) + ' rows in Motion',
          additional_fields={'_affected_rows':'Motion:'+str(INSERTED),
                             '_inserted':'Motion:'+str(INSERTED),
                             '_state':'CA',
                             '_log_type':'Database'})

  LOG = {'tables': [{'state': 'CA', 'name': 'Motion', 'inserted':INSERTED, 'updated': 0, 'deleted': 0}]}
  sys.stderr.write(json.dumps(LOG))

if __name__ == "__main__":
  with GrayLogger(API_URL) as _logger:
    logger = _logger
    get_motions() 
