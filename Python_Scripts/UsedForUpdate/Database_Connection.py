#!/usr/bin/env python2.6

'''
File: Database_Connection.py
Author: Eric Roh
Date: 2016/09/23

Description:
- Function that makes a connection to the current database
- Script is called by other scripts making connection to MySQL database
'''

import MySQLdb

def mysql_connection():
  with MySQLdb.connect(host='dddb2016-mysql5-7-11.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                       port=3306,
                       db='DDDB2016Aug',
                       #db='EricTest',
                       user='awsDB',
                       passwd='digitaldemocracy789') as dd_cursor:
    return dd_cursor

