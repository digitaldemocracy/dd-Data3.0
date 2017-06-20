#!/usr/bin/env python

'''
File: ca_agenda.py
Author: Sam Lakes
Date Created: July 27th, 2016
Last Modified: August 24th, 2016
Description:
- Grabs the California Legislative Agendas for database population
Sources:
- capublic database on transcription.digitaldemocracy.org
'''

from bs4 import BeautifulSoup as bs
import sys
import requests
import re
from datetime import datetime
from pytz import timezone
import time
#import pymysql.cursors
import MySQLdb

#Select statement to get the proper information from the capublic database
sql_select = '''SELECT DISTINCT(committee_hearing_tbl.bill_id), committee_type,
                long_description, hearing_date
                FROM committee_hearing_tbl JOIN location_code_tbl
                ON committee_hearing_tbl.location_code=location_code_tbl.location_code
                WHERE `hearing_date` >= %s'''


'''
Formats the string in this format: <House> <Type> Committee On <Committee Name> 
|agenda|: The agenda to have it's title changed
|to_remove|: A list of strings to remove from the Committee Name
Returns an agenda in proper format 
'''
def modify_agenda(agenda, to_remove):
  for string in to_remove:
    agenda[2] = agenda[2].replace(string, '') 
  agenda[2] = agenda[2].strip()
  #This is a temporary fix, will try and make it less hacky later
  #Since there will be a recess for a while it shouldn't be problematic
  #Also almost all of the hearing entries are for standing committees
  agenda[2] = '%(house)s %(type)s Committee On ' + agenda[2]


  if agenda[1] == 'CX':
    agenda[2] = agenda[2] % {'house': 'Assembly', 'type': 'Standing'}
  else:
    agenda[2] = agenda[2] % {'house': 'Senate', 'type': 'Standing'}

  return agenda  
  

'''
Changes the committee name capitalization in a list of bill agendas.
|agendas|: The list of agendas to be altered
Returns: An altered list of agendas
'''
def change_committee_names(agendas):
  new_agendas = []
  rem = ['Sen.', 'Assembly', 'Senate']

  for item in agendas:
    item = list(item)
    if item[2] == 'Water, Parks and Wildlife':
      item[2] = 'Water, Parks, and Wildlife'
    if item[2] == "Public Employees, Retirement and Social Security":
        item[2] = "Public Employees, Retirement, and Social Security"
        print(item)
        print("Asdf")
    if item[2] == "Aging and Long Term Care":
        item[2] = "Aging And Long-Term Care"
        print(item)
    if "Long" in item[2]:
        print("fa;lsdkfa\n\n")
        print(item[2])

    item[2] = item[2].lower().title()
    new_agendas.append(item)

  return [modify_agenda(agenda, rem) for agenda in new_agendas]



'''
Take the date and find any agendas on or after that date in the database.
|dd_cursor|: capublic database cursor
|date|: Date passed in
Returns: A list of tuples containing bill agendas
'''
def fetch_agendas(cursor, date):
  result = [] 
  
  cursor.execute(sql_select, (date,))
  result = cursor.fetchall()
  print(cursor.rowcount)
  return result

'''
Runs the functions and returns a list of data.
Returns a list of dictionary objects
'''
def ca_scraper():
  keys = ['bid', 'house', 'c_name', 'date', 'state']  

  with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                               user='monty',
                               passwd='python',
                               db='capublic',
                               charset='utf8') as connection:
    
    cur_date = datetime.now(timezone('US/Pacific')).strftime('%Y-%m-%d')
    print(cur_date)
    agendas = change_committee_names(fetch_agendas(connection, cur_date))

    #format the bills to fit the standards set by the database
    #add the state
    for i in range(0, len(agendas)):
      agendas[i][0] = 'CA_' + agendas[i][0]
      agendas[i].append('California')

#    connection.close()
  
    #zip it to put it in dictionary format for the driver
    ret = [dict(zip(keys, agenda)) for agenda in agendas]
    #for some reason I get double everything so I remove duplicate elements
    ret = [dict(t) for t in set([tuple(d.items()) for d in ret])]
    return ret


if __name__ == '__main__':
  ca_scraper()
