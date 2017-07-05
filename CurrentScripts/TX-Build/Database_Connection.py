#!/usr/bin/env python

'''
File: Database_Connection.py
Author: Eric Roh
Date: 2016/09/23

Description:
- Function that makes a connection to the current database
- Script is called by other scripts making connection to MySQL database
'''
import MySQLdb

QS_DBINFO = '''SELECT * FROM DBConnection
               WHERE db = %s'''


def mysql_connection(args):
  if len(args) == 1:
    return {'host':'dddb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
            'port':3306,
            #'db':'EricTest',
            'db':'DDDB2016Aug',
            'user':'awsDB',
            'passwd':'digitaldemocracy789'
            #'host':'localhost',
            #'db':'russo_dddb',
            #'user':'root',
            #'passwd':''
            }
  else:
    with MySQLdb.connect(host='localhost',
                         db='OverlordDB',
                         user='root',
                         passwd='') as db:
      db.execute(QS_DBINFO, (args[1],))
      if db.rowcount == 1:
        dbinfo = db.fetchone()
        print type(dbinfo), dbinfo
      else:
        print 'failed'
        return -1
      return {'host':dbinfo[0],
              'port':dbinfo[1],
              'db':dbinfo[2],
              'user':dbinfo[3],
              'passwd':dbinfo[4]}
    print args[-1]
  #with MySQLdb.connect(host='dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
  #                     port=3306,
  #                     #db='DDDB2016Aug',
  #                     db='EricTest',
  #                     user='awsDB',
  #                     passwd='digitaldemocracy789') as dd_cursor:
  #  return dd_cursor

