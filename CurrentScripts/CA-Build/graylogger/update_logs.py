#!/usr/bin/env python3.4
'''
File: update_logs.py
Author: Chauncey Neyman
Date: 7/18/2016

Description:
- Reads Graylog logs from Elasticsearch and writes them to VisualizerDB

Notes:
  - Uses Python 3.4 for timeout

'''

import MySQLdb
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q

#middb_cursor = None
visualizerdb_cursor = None
overlorddb_cursor = None
logger = None

# Elasticsearch Connection

client = Elasticsearch(
	['dw.digitaldemocracy.org:9200'
	]
)

def query_ES():
  body = {
       # Show only logs within __ timeframe
       "query" : {
          "range" : {
             "timestamp" : {
                "gt" : "now-3s"
             }
          }
       },
       # Added level filter to avoid syslogs (level -1)
       "filter" : {
           "range" : {
               "level" : {
                   "gte" : "0",
                   "lte" : "7"
                }
            }
       },
       # Added size filter to make sure only 1 is passed through
       #"size" : 1,
       # Show the most recent log
       "sort" : [
         {
           "timestamp": {
             "order" : "desc"
           }
         }
       ]
  }
  s = Search.from_dict(body)
  s = s.using(client)
  response = s.execute()

  return response

def script_not_in_db(name):
  overlorddb_cursor.execute('''SELECT * FROM Script WHERE name = %s''', (name,))
  if overlorddb_cursor.rowcount == 0:
    return True
  return False

def sv_not_in_db(sid):
  overlorddb_cursor.execute('''SELECT * FROM ScriptVersion WHERE sid = %s''', (sid,))
  if overlorddb_cursor.rowcount ==0:
    return True
  return False

# Database Queries
def db_magic(hit):
  scriptName = extract_name_from_source(hit.source)
  if script_not_in_db(scriptName):
    print("Script not in DB")
    return
  # Retrieve sid
  overlorddb_cursor.execute('''SELECT sid FROM Script WHERE name=%s''' , (scriptName,))
  sid = overlorddb_cursor.fetchone()[0]
  if sv_not_in_db(sid):
    print("ScriptVersion not in DB")
    return
  # Retrieve svid
  overlorddb_cursor.execute('''SELECT max(sv_id) FROM ScriptVersion WHERE sid=%s''', (sid,))
  svid = overlorddb_cursor.fetchone()[0]
  # Retrieve timestamp
  visualizerdb_cursor.execute('''SELECT max(end_time) FROM ScriptRunHistory WHERE sv_id=%s''', (svid,))
  timestamp = visualizerdb_cursor.fetchone()[0]
  # Retrieve tids
  tids = table_names_to_tids(hit)
  
  # To avoid race conditions, compare timestamps of most recent log vs this log
  if (timestamp == hit.timestamp):
    # Log looked at by hit is the same as the most recent log in Elasticsearch
    print("Queried result is the same as the most recent log in the database")
  else:
    update_script_run_history(hit, svid)
    update_status_history(hit, tids)
    # Retrieve hid
    visualizerdb_cursor.execute('''SELECT max(hid) FROM ScriptRunHistory WHERE sv_id=%s''', (svid,))
    hid = visualizerdb_cursor.fetchone()[0]
    # Update run modifies using that hid
    update_run_modifies(hit, hid, tids)


def extract_name_from_source(source):
  pathArray = source.split("/")
  # return the last string (filename in the path)
  return pathArray[-1]

# Query and insert to ScriptRunHistory
def update_script_run_history(hit, svid):
  statusString = set_status_string(hit)
  visualizerdb_cursor.execute('''SELECT max(run) FROM ScriptRunHistory WHERE sv_id=%s''', (svid,))
  maxRun = visualizerdb_cursor.fetchone()[0]
  if (maxRun == None):
    maxRun = 0
  statusString = set_status_string(hit)
  visualizerdb_cursor.execute('''INSERT INTO ScriptRunHistory (sv_id, run, end_time, exec_type, status)
   VALUES (%s,%s,%s,%s,%s)''', (svid, maxRun + 1, hit.timestamp, 'Manual', statusString))
  return

# Insert into StatusHistory
def update_status_history(hit, tids):
  statusString = set_status_string(hit)
  for tid in tids:
    visualizerdb_cursor.execute('''INSERT INTO StatusHistory (tid, time, exec_type, status)
     VALUES (%s,%s,%s,%s)''', (tid[0], hit.timestamp, 'Manual', statusString))
  return

# Insert into runModifies
def update_run_modifies(hit, hid, tids):
  statusString = set_status_string(hit)
  for tid in tids:
    overlorddb_cursor.execute('''SELECT name FROM DD_Table WHERE tid=%s''', (tid[0],))
    table_name = overlorddb_cursor.fetchone()[0]
    total_rows = get_total_rows(table_name)
    visualizerdb_cursor.execute('''INSERT INTO runModifies (hid, tid, status, affected_rows, total_rows)
     VALUES (%s,%s,%s,%s,%s)''', (hid, tid[0], statusString, tid[1], total_rows))
  return

def set_status_string(hit):
  statusString = 'Failure'
  if hit.level == 4:
    statusString = 'Outdated'
  if hit.level >= 5:
    statusString = 'Updated'
  return statusString

def table_names_to_tids(hit):
  tid_array = []
  table_couple = []
  if hasattr(hit, 'affected_rows'):
    tables_with_numbers = hit.affected_rows.split(",")
    for tables in tables_with_numbers:
      table_affected = tables.split(":")
      overlorddb_cursor.execute('''SELECT tid FROM DD_Table WHERE name = %s''', (table_affected[0].strip(),))
      if overlorddb_cursor.rowcount == 0:
        #print table_affected[0] + ' not found'
        pass
      else:
        tid_array.append((overlorddb_cursor.fetchone()[0], table_affected[1]))
  return tid_array

def get_total_rows(table_name):
  # connect to dddb
  with MySQLdb.connect(host='dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
    user='awsDB',
    db='DDDB2016Aug',
    port=3306,
    passwd='digitaldemocracy789',
    charset='utf8') as dddb:
      QS_ROWCOUNT = '''SELECT COUNT(*) FROM %s''' % table_name
      dddb.execute(QS_ROWCOUNT)
      total_rows = dddb.fetchone()[0]

  return total_rows

def update_logs_main():
  main()

def main():
  global overlorddb_cursor
  global visualizerdb_cursor
  response = query_ES()
  # Initialize OverlordDB cursor
  overlorddb = MySQLdb.connect(host="dw.digitaldemocracy.org", user="monty", passwd="python", db="OverlordDB")
  overlorddb_cursor = overlorddb.cursor()
  # Initialize VisualizerDB cursor
  with MySQLdb.connect(host="dw.digitaldemocracy.org", user="monty", passwd="python", db="VisualizerDB") as visualizerdb:
    visualizerdb_cursor = visualizerdb

    for hit in response:
      db_magic(hit)

if __name__ == '__main__':
    main()
