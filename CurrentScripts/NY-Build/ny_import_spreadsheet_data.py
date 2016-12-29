#!/usr/bin/python
# -*- coding: utf8 -*-


'''
File: ny_import_spreadsheet_data.py
Author: Miguel Aguilar
Date: 02/29/2016
Modified by: Eric Roh
Date: 6/23/2016

Description:
  - This script scrapes NY legislator data from a google spreadsheet and 
  populates the Legislator and Term tables.


Populates:
  - Legislator (twitter_handle, capitol_phone, website_url, 
            email_form_link, OfficialBio)
  - Term (party, district)
'''

import sys
from Database_Connection import mysql_connection
import traceback
import MySQLdb
import csv
import re
import subprocess
from graylogger.graylogger import GrayLogger
API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None
logged_list = list()
L_UPDATE = 0
T_UPDATE = 0

QS_PERSON = '''SELECT pid
               FROM Person
               WHERE first = %(first)s
                AND last = %(last)s'''

QU_LEGISLATOR = '''UPDATE Legislator
                   SET twitter_handle = %(twitter)s, 
                    capitol_phone = %(phone)s,
                    website_url = %(website)s,
                    email_form_link = %(email_link)s,
                    OfficialBio = %(OfficialBio)s
                   WHERE pid = %(pid)s'''
QU_TERM = '''UPDATE Term
             SET party = %(party)s,
              district = %(district)s
             WHERE pid = %(pid)s'''

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'NY'
  }

'''
  This function returns the pid number given a first and last name
'''
def get_pid_from_Person(dddb, legislator):
  #Query execution for the Select statement
  dddb.execute(QS_PERSON, legislator)

  #Get the pid from the query result
  if dddb.rowcount >= 1:
    pid = dddb.fetchone()[0]
    return pid
# pidX = []
# for p in pid:
#   pidX = [x[0] for x in pid]

  #If the query returned a pid else return -1
# if len(pidX):
#   pid_value = pidX[0]
# else:
#   pid_value = -1
  if legislator not in logged_list:
    logged_list.append(legislaotr)
    logger.warining('Person not fond ' + legislator['first'] + ' ' + legislaotr['last'],
        additional_fields={'_state':'NY'})
  return None


'''
  This function populates the tables with the data
'''
def populate_table(dddb, legislator):
  global L_UPDATE, T_UPDATE
  #Get legislator pid from Person table
  pid = get_pid_from_Person(dddb, legislator)
  
  if pid is not None:

    #Update the Legislator table  
    ##MISSING ROOM NUMBER AND DESCRIPTION
#   update = 'UPDATE Legislator '
#   columns = 'SET twitter_handle = %(twitter)s, capitol_phone = %(phone)s, website_url = %(website)s, email_form_link = %(email_link)s, OfficialBio = %(OfficialBio)s '
#   where = 'WHERE pid = %d;' % (pid)

#   update_stmt = ''.join([update, columns, where])
    #print update_stmt
    legislator['pid'] = str(pid)

    try:
#      qu_stmt = QU_LEGISLATOR + (' WHERE pid = %d' % (pid))
#      print qu_stmt
      dddb.execute(QU_LEGISLATOR, legislator)
      L_UPDATE += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Update Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Legislator', (QU_LEGISLATOR % legislator)))

    #Update the Term table
#   update = 'UPDATE Term '
#   columns = 'SET party = %(party)s, district = %(district)s '
#   where = 'WHERE pid = %d;' % (pid)

#   update_stmt = ''.join([update, columns, where])
    #print update_stmt
    try:
#      qu_stmt = QU_TERM + (' WHERE pid = %d' % (pid))
      dddb.execute(QU_TERM, legislator)
      T_UPDATE += dddb.rowcount
    except MySQLdb.Error:
      logger.warning('Update Failed', full_msg=traceback.format_exc(),
          additional_fields=create_payload('Term', (QU_TERM % legislator)))


def clean_name(name):
    problem_names = {
        "Inez Barron":("Charles", "Barron"), 
        "Philip Ramos":("Phil", "Ramos"), 
        "Thomas McKevitt":("Tom", "McKevitt"), 
        "Albert Stirpe":("Al","Stirpe"), 
        "Peter Abbate":("Peter","Abbate, Jr."),
        "Sam Roberts":("Pamela","Hunter"),
        "Herman Farrell":("Herman", "Farrell, Jr."),
        "Fred Thiele":("Fred", "Thiele, Jr."),
        "William Scarborough":("Alicia", "Hyndman"),
        "Robert Oaks":("Bob", "Oaks"),
        "Andrew Goodell":("Andy", "Goodell"),
        "Peter Rivera":("José", "Rivera"),
        "Addie Jenne Russell":("Addie","Russell"),
        "Kenneth Blankenbush":("Ken","Blankenbush"),
        "Alec Brook-Krasny":("Pamela","Harris"),
        "Mickey Kearns":("Michael", "Kearns"),
        "Steven Englebright":("Steve", "Englebright"),
    "Philip Boyle":("Phil", "Boyle"),
    "Richard Funke":("Rich", "Funke"),
    "Tom O'Mara":("Thomas", "O'Mara"),
    "Ed Ra":("Edward", "Ra"),
    "Erik Martin Dilan":("Erik", "Dilan"),
    "Tom Abinanti":("Thomas", "Abinanti"),
    "Kieran Lalor":("Kieran Michael", "Lalor"),
    "Jim Tedisco":("James", "Tedisco"),
    "Crystal Peoples":("Crystal", "Peoples-Stokes"),
    "Pam Hunter":("Pamela", "Hunter"),
    "L Dean Murray":("Dean", "Murray"),
    "Rubén Díaz, Sr.":("Ruben", "Diaz"),
    "George Amedore, Jr.":("George", "Amedore"),
    "Kenneth Zebrowski, Jr.":("Kenneth", "Zebrowski"),
        
    }
    ending = {'Jr':', Jr.','Sr':', Sr.','II':' II','III':' III', 'IV':' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()      
    suffix = "";               
    for word in name_arr:
        if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
            name_arr.remove(word)
            if word in ending.keys():
                suffix = ending[word]            
            
    first = name_arr.pop(0)
    while len(name_arr) > 1:
        first = first + ' ' + name_arr.pop(0)            
    last = name_arr[0]
    last = last.replace(' ' ,'') + suffix
    
    if (first + ' ' + last) in problem_names.keys():             
        return problem_names[(first + ' ' + last)]
        
    return (first, last)

'''
  Get the NY Legislators data from the spreadsheet
  To be placed in Legislator and Term tables
'''
def scrape_legislator_data(dddb):
  #Open the tsv file with the NY legislator data
  with open("spreadsheet.tsv") as tsv:
    for line in csv.reader(tsv, delimiter="\t"): 
      name = line[0]
      if (name != "" and name != 'Senators' and name != 'Assembly' 
        and name != 'Name' and name != '\n'):
        party = line[1]
        bio = line[3]
        bioURL = line[4]
        twitter = line[5]
        district = line[7]
        email = line[8]
        phone = line[9]
        
        #Cleans the name and returns the name in the DB
        cleanName = clean_name(name)
        first = cleanName[0]
        last = cleanName[1]

        #Get party
        if party == 'R':
          party = 'Republican'
        elif party == 'D':
          party = 'Democrat'
        elif party == '':
          party = ''
        else:
          party = 'Other'

        #Get twitter handle
        twitter_list = twitter.split('/')
        temp = twitter.split('.com/')
        tweetName = ''
        if len(temp) > 1:
          tweetName = temp[1].split('?')[0]
        twitter_handle = ''
        if len(tweetName): 
          twitter_handle = '@' + tweetName

        #Get the main website url from the BioUrl
        url = bioURL.split('/about')[0].split('/bio')[0]
        #The email_form_link is the same as the contact url
        if url:
          email_link = ''.join([url, '/contact'])
        else:
          email_link = ''

        #Dictionary of the legislator
        legislator = {'name':name, 'OfficialBio':bio, 'phone':phone, 
              'email_link':email_link, 'website':url, 
              'twitter':twitter_handle, 'party':party,
              'district':district, 'last':last, 'first':first}

        #Populates the tables
        populate_table(dddb, legislator);


def get_spreadsheet():
    url = 'https://spreadsheets.google.com/feeds/download/spreadsheets/Export?key=1VaLLtIgD3HGPVTVd1MN0ftWV8R54GR9UKForhFOh_F4&exportFormat=tsv&gid=0'
    
    returncode = subprocess.call('wget -O spreadsheet.tsv "%s"'%(url), shell=True)

    if returncode != 0:
        print 'Error'


def delete_spreadsheet():
   returncode = subprocess.call('rm spreadsheet.tsv', shell=True)

   if returncode != 0:
       print 'Error'



def main():
  ddinfo = mysql_connection(sys.argv)
  #Connect to the Database
  with MySQLdb.connect(host=ddinfo['host'],
                        user=ddinfo['user'],
                        db=ddinfo['db'],
                        port=ddinfo['port'],
                        passwd=ddinfo['passwd'],
                        charset='utf8') as dddb:

  #Need a cursor in order to execute queries
# dddb = dddb_conn.cursor()
# dddb_conn.autocommit(True)
    #Download Google spreadsheet
    get_spreadsheet()
    #Collect the necessary data for each legislator
    scrape_legislator_data(dddb)
    #Delete Google spreadsheet
    delete_spreadsheet()
    logger.info(__file__ + ' terminated successfully.', 
        full_msg='Updated ' + str(L_UPDATE) + ' rows in Legislator and updated '
                  + str(T_UPDATE) + ' rows in Term',
        additional_fields={'_affected_rows':'Legislator:'+str(L_UPDATE)+
                                      ', Term:'+str(T_UPDATE),
                           '_updated':'Legislator:'+str(L_UPDATE)+
                                      ', Term:'+str(T_UPDATE),
                           '_state':'NY'})
  
  #Close database connection
# dddb_conn.close()


if __name__ == "__main__":
  with GrayLogger(API_URL) as _logger:
    logger = _logger
    main()