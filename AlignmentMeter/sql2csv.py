#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: sql2csv.py
Author: Miguel Aguilar
Maintained: Miguel Aguilar
Date: 08/19/2016
Last Modified: 08/19/2016
'''

import sys
import csv
import traceback
import MySQLdb

def execute_queries(dddb, output, query_list):
  for que in query_list:
    try:
      dddb.execute(que)
      if dddb.rowcount > 0:
        fetch = dddb.fetchall()
        query = ' '.join(que.split('\n'))
        #output.writerow([query])
        output.writerow([i[0] for i in dddb.description])
        for f in fetch:
          output.writerow(f)
        output.writerow([])
        output.writerow([])
    except MySQLdb.Error:
      print traceback.format_exc()
      print que
      

def main():
  if len(sys.argv) != 2:
    print 'Usage: python sql2csv.py [file.sql]'
    print 'Where file.sql contains sql queries to execute'
    exit(1)

  if '.sql' not in sys.argv[1]:
    print 'File passed in is not in .sql format'
    exit(1)

  with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
                        user='awsDB',
                        db='MikeyTest',
                        port=3306,
                        passwd='digitaldemocracy789',
                        charset='utf8') as dddb:
    with open(sys.argv[1], 'r') as sqlFile:
      outfile_name = sys.argv[1].split('.')[0] + '.csv'
      output = csv.writer(open(outfile_name, 'w'))

      sql = sqlFile.read()
      queries = sql.split(';')
      query_list = []
      for que in queries:
        query = que.strip() + ';'
        query_list.append(query)

      execute_queries(dddb, output, query_list[:-1])
      print 'Done.. %s was created'%(outfile_name)


if __name__ == '__main__':
  main()