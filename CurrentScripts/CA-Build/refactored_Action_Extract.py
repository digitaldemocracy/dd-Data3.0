#!/usr/bin/env python
'''
File: Action_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan, Eric Roh
Date: 6/11/2015
Last Changed: 6/20/2016

Description:
- Inserts Actions from the bill_history_tbl from capublic into DDDB.Action
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
    - bill_history_tbl

Populates:
  - Action (bid, date, text)
'''

from Database_Connection import mysql_connection
import sys
import traceback
import MySQLdb
from graylogger.graylogger import GrayLogger
API_URL = 'http://dw.digitaldemocracy.org:12202/gelf' 
logger = None
INSERTED = 0
UPDATED = 0

STATE = 'CA'

# INSERTS
QI_ACTION = '''INSERT INTO Action (bid, date, text, seq_num)
               VALUES (%s, %s, %s, %s)'''

# SELECTS
QS_BILL_HISTORY_TBL = '''SELECT bill_id, action_date, action, action_sequence
                         FROM bill_history_tbl'''
QS_ACTION_CHECK = '''SELECT bid
                     FROM Action
                     WHERE bid = %s
                      AND date = %s
                      AND seq_num = %s'''
QS_ACTION_SEQ_CHECK = '''
SELECT bid
FROM Action
WHERE bid = %s
 AND date = %s
 AND text = %s
 AND seq_num != %s
'''
QS_ACTION_TEXT = '''SELECT bid
                     FROM Action
                     WHERE bid = %s
                      AND date = %s
                      AND text != %s
                      AND seq_num = %s'''

# UPDATE
QU_ACTION_SEQ = '''UPDATE Action
                   SET seq_num = %s
                   WHERE bid = %s
                    AND date = %s
                    AND text = %s'''
QU_ACTION_TEXT = '''UPDATE Action
                    SET text = %s
                    WHERE bid = %s
                     AND date = %s
                     AND seq_num = %s'''

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA',
      '_log_type':'Database'
  }

'''
Checks if the Action is in DDDB. If it isn't, insert it. Otherwise, skip.

|dd_cursor|: DDDB database cursor
|values|: Tuple that includes the following (in order):
  |bid|: Bill id
  |date|: Date of action
  |text|: Text of action
'''
def insert_Action(dd_cursor, values):
  global INSERTED, UPDATED
  values[0] = '%s_%s' % (STATE, values[0])

  # Check if DDDB already has this action
  dd_cursor.execute(QS_ACTION_CHECK, (values[0], values[1], values[3]))
  # If Action not in DDDB, add
  if(dd_cursor.rowcount == 0):
#    logger.info('New Action %s %s %s' % (values[0], values[1], values[2]))
    try:
      dd_cursor.execute(QI_ACTION, values)
      INSERTED += dd_cursor.rowcount
    except MySQLdb.Error:
      logger.warning('Insert Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Action', (QI_ACTION % (values[0], values[1], values[2], values[3]))))
  else:
    # Check if text has changed
    dd_cursor.execute(QS_ACTION_TEXT, values)
    # If text is different update text
    if dd_cursor.rowcount == 1:
      try:
        dd_cursor.execute(QU_ACTION_TEXT, (values[2], values[0], values[1], values[3]))
        UPDATED += dd_cursor.rowcount
      except MySQLdb.Error:
        logger.warning('Update Failed', full_msg=traceback.format_exc(),
            additional_fields=create_payload('Action',
              QU_ACTION_TEXT % (values[2], values[0], values[1], values[3])))

  
def update_Action(dd_cursor, values):
  global UPDATED
  values[0] = '%s_%s' % (STATE, values[0])

  # Check if DDDB already has this action
  dd_cursor.execute(QS_ACTION_SEQ_CHECK, (values[0], values[1], values[2], values[3]))

  if dd_cursor.rowcount == 1:
    dd_cursor.execute(QU_ACTION_SEQ, (values[3], values[0], values[1], values [2]))

    UPDATED += dd_cursor.rowcount

'''
Loops through all Actions from capublic and adds them as necessary
'''
def main():
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
      # Get all of the Actions from capublic
      ca_cursor.execute(QS_BILL_HISTORY_TBL)

      for record in ca_cursor.fetchall():
        insert_Action(dd_cursor, list(record))
        update_Action(dd_cursor, list(record))
      logger.info(__file__ + ' terminated successfully.', 
          full_msg='Inserted ' + str(INSERTED) + ' and updated ' + str(UPDATED) + ' rows in Action',
          additional_fields={'_affected_rows':'Action:'+str(INSERTED + UPDATED),
                             '_inserted':'Action:'+str(INSERTED),
                             '_updated':'Action:'+str(UPDATED),
                             '_state':'CA',
                             '_log_type':'Database'})

if __name__ == "__main__":
  with GrayLogger(API_URL) as _logger:
    logger = _logger
    main()
