'''
  File: statement_table.py
  Author: Miguel Aguilar
  Maintained: Miguel Aguilar
  Date: 11/22/2016
  Last Modified: 11/22/2016
'''

import pymysql
import traceback
from datetime import datetime
import pandas as pd


CONN_INFO = {'host': 'dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
             'port': 3306,
             'db': 'MikeyTest',
             #'db': 'DDDB2016Aug',
             'user': 'awsDB',
             'passwd': 'digitaldemocracy789'}

QC_STATEMENT = '''CREATE TABLE IF NOT EXISTS Statement (
                  hid int(11), 
                  vid int(11), 
                  pid int(11), 
                  start int(11),
                  end int(11),
                  startUid int(11),
                  endUid int(11),
                  totalTime int(11),
                  totalWords int(11),
                  PRIMARY KEY (startUid, endUid),
                  UNIQUE KEY (startUid, endUid, hid, vid, pid),
                  FOREIGN KEY (startUid) REFERENCES Utterance(uid),
                  FOREIGN KEY (endUid) REFERENCES Utterance(uid),
                  FOREIGN KEY (hid) REFERENCES Hearing(hid),
                  FOREIGN KEY (vid) REFERENCES Video(vid),
                  FOREIGN KEY (pid) REFERENCES Person(pid)
                  )
                  ENGINE = INNODB
                  CHARACTER SET utf8 COLLATE utf8_general_ci;'''

QS_TABLE = '''SHOW TABLES like "Statement"'''

QS_STATEMENT = '''SELECT *
                  FROM Statement
                  WHERE hid = %(hid)s AND vid = %(vid)s AND pid = %(pid)s
                  AND startUid = %(startUid)s AND endUid = %(endUid)s'''

QI_STATEMENT = '''INSERT INTO Statement (hid, vid, pid, start, end, startUid, endUid, totalTime, totalWords)
                  VALUES (%(hid)s, %(vid)s, %(pid)s, %(start)s, %(end)s, %(startUid)s, %(endUid)s, %(totalTime)s, %(totalWords)s)'''

QS_PERSON = '''SELECT p.first, p.last, l.pid
              FROM Person p, Legislator l
              WHERE p.pid = l.pid'''

QS_UTTERANCES = '''SELECT v.vid, v.hid, u.uid, u.pid, u.time, u.endTime, u.text
                  FROM Utterance u, Video v
                  WHERE u.vid = v.vid AND u.current = 1 AND u.finalized = 1
                  ORDER BY v.vid, v.hid, u.time'''


'''
Insert individual statements into Statement table.
'''
def insert_statement(dddb, stmt):
  try:
    dddb.execute(QS_STATEMENT, stmt)
    #If entry does not exist in table
    if dddb.rowcount == 0:
      dddb.execute(QI_STATEMENT, stmt)
  except:
    print(QI_STATEMENT%stmt)
    print(traceback.format_exc())
    #exit(1)

'''
This function creates the Statement table and merges contiguous utterances if they belong
to the same person to create an uninterrupted statement. 
'''
def create_statement_table(dddb):
  #Create Statement table if it doesn't exist
  dddb.execute(QS_TABLE)
  if dddb.rowcount == 0:
    dddb.execute(QC_STATEMENT)
    print('Statement table created..')

  #Query the DB for all the utterances in the Utterance table
  #Gets the finalized versions of them only
  utterances = []
  dddb.execute(QS_UTTERANCES)
  if dddb.rowcount > 0:
    utterances = dddb.fetchall()
    print(len(utterances))

  #Collects the first utterance in order to have the first pid as a starting reference
  first_utter = utterances[0]
  cur_pid = first_utter[3]
  stmt = {'hid':first_utter[1], 'vid':first_utter[0], 'pid':first_utter[3], 'start':first_utter[4],
          'end':first_utter[5], 'startUid':first_utter[2], 'endUid':first_utter[2], 
          'totalTime': (first_utter[5] - first_utter[4]), 'totalWords': len(first_utter[6].split())}

  #Counts how many statement insertions will be made
  counter = 0

  #Iterate through all the utterances starting on the second one
  for utter in utterances[1:]:
    #If the current pid is not the same as the previous utterance pid
    #then the statement is over because it's interrupted, so insert to DB
    if cur_pid != utter[3]:
      #Prints something to show that the script is alive haha
      if (counter%10000) == 0:
        print(counter)
      insert_statement(dddb, stmt)

      counter += 1

      #Starts a new statement because a new person is talking
      cur_pid = utter[3]
      stmt['start'] = utter[4]
      stmt['startUid'] = utter[2]
      stmt['hid'] = utter[1]
      stmt['vid'] = utter[0]
      stmt['pid'] = utter[3]
      stmt['totalTime'] = 0
      stmt['totalWords'] = 0

    #Replace the end time and end uid because statement is expanding
    #Also increase the total word and total time count
    stmt['end'] = utter[5]
    stmt['endUid'] = utter[2]
    if utter[6] is not None and len(utter[6]) > 0:  
      stmt['totalWords'] = stmt['totalWords'] + len(utter[6].split())
    stmt['totalTime'] = stmt['totalTime'] + (utter[5] - utter[4])


'''
TOTAL RUNTIME:
  9:51:53.046914

  YES THIS TAKES FUCKING LONG..
  ALREADY IN THE DB, SO AVOID RUNNING IT 

TOTAL INSERTS:
  330000
'''

def main():
  #Save the start time
  startTime = datetime.now()

  #Create DDDB connection
  cnxn = pymysql.connect(**CONN_INFO)
  dddb = cnxn.cursor()

  #Create that statement table
  print('Making statements from utterances..')
  create_statement_table(dddb)
  print('Done..')

  #Close DDDB connection
  cnxn.commit()
  cnxn.close()

  #Print how long the runtime of the script was
  print(datetime.now() - startTime)

if __name__ == '__main__':
  main()