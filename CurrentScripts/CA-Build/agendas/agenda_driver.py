#!/usr/bin/env python

'''
File: scraper_driver.py
Author: Sam Lakes
Date Created: August 8th, 2016
Last Modified: August 24th, 2016

Description:
- Runs all of the agenda scripts and updates the database
Sources:
- ny_asm.py (Will add once ny_sen.py is up and running)
- ca_agenda.py
- ny_sen.py (Not yet)
'''

#import pymysql.cursors
import traceback
from Database_Connection import mysql_connection
import MySQLdb
import sys
import time
import datetime
from graylogger.graylogger import GrayLogger

#Whenever you need to add a scraper just import the driver function
from ca_agenda import ca_scraper 

GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'

#and add the function to this list
scrapers = [
  ca_scraper
]

#if we need to scrape more info add the key name here
scraper_keys = [
 'date',
 'state', 
 'c_name', 
 'bid'
]


#used in ins_hearing()
insert_hearing = '''
INSERT INTO Hearing (Date, state)
VALUES (date(%s), 
        (SELECT abbrev FROM State WHERE name=%s))
'''

#used in ins_hearing()
insert_committee_hearing = '''
INSERT INTO CommitteeHearings (cid, hid)
VALUES ((SELECT cid FROM Committee WHERE name=%s LIMIT 1), 
        (SELECT hid FROM Hearing WHERE hid=%s LIMIT 1)) 
'''

#used in insert_hearings()
check_db_c_hearing = '''
SELECT cid
FROM CommitteeHearings
WHERE hid =%s
AND cid = (SELECT cid FROM Committee WHERE name=%s LIMIT 1)
'''


#used in ins_hearing()
select_committee_hearing = '''
SELECT cid 
FROM Committee
WHERE name=%s
LIMIT 1
'''

#used in insert_hearing_agenda()
insert_hearing_agendas = '''
INSERT INTO HearingAgenda (hid, bid, date_created, current_flag)
VALUES ((SELECT hid FROM Hearing WHERE hid=%s LIMIT 1),
        (SELECT bid FROM Bill WHERE bid=%s LIMIT 1),
        date(%s), 
        True)
'''

#used in update_db()
update_agenda_date = '''
UPDATE HearingAgenda
SET current_flag=0
WHERE date_created < date(%s)
'''


#used in update_db()
check_db_hearing = '''
SELECT hid
FROM Hearing
WHERE date=date(%s)
AND state= (SELECT abbrev FROM State WHERE name=%s)
'''


#used in check_agendas()
check_db_agenda = '''
SELECT bid, hid
FROM HearingAgenda
WHERE current_flag=1
'''


#used in check_agendas()
db_set_inactive = '''
UPDATE HearingAgenda
SET current_flag=0
WHERE bid=%s
AND hid=%s
'''

def create_payload(table, sqlstmt):
  return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA',
      '_log_type': 'Database'
      }


'''
Takes a list of agendas and converts each of them into 
a database-friendly format for the hearing table in DDDB.
|agendas|: Variable amount of agenda lists to be modified
Returns a tuple of the modified agenda lists.
'''
def create_hearings(agenda_lists):
  hearings = [] 
  for agenda_list in agenda_lists:
    try: 
      for item in agenda_list:
        #grab relevant info from the dictionary
        #It needs to follow these naming conventions though
        hearings.append({'date' : item['date'], 
                         'state': item['state'], 
                         'c_name': item['c_name'],
                         'bid': item['bid'],
                         'hid' : None,
                         'cid' : None,
                         'in_db' : False}) 
    except: 
      exception = sys.exc_info()[0]
      error_msg = ('Error: ' + 
                  str(exception) + 
                  'when trying to create hearing.')
      logger.error(error_msg,
                   additional_fields={'_state':agenda_list[0]['state']})
      hearings = False
      
  return hearings



'''
Sets the flags of the HearingAgenda items to not current if they are old.
|connection|: The mysql connection to DDDB
'''
def update_db(cursor):
  cur_date = datetime.datetime.today()
  #Go into HearingAgenda table of database
  #with connection.cursor() as cursor:
  try:
    #set anything old to inactive 
    cursor.execute(update_agenda_date,
                   (cur_date.date(),))
        
  except:
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' +
                str(exception) +
                'when trying to update database.')
    logger.error('Update Failed', full_msg=traceback.format_exc(),
        additional_fields=create_payload('HearingAgenda',
          update_agenda_date % (cur_date.date(),)))
    return False
  return True
  #Get every row where current flag is set to true
  #If the date is before today, then set to inactive
  

'''
Takes a list of agendas and puts them into the HearingAgenda table 
of DDDB.
|agendas|: The list of agendas to put in the table
|connection|: The mysql connection to DDDB
'''
def insert_hearing_agenda(agendas, cursor):
  #just take the agendas and throw them in the database
  cur_date = datetime.datetime.today()
  #with connection.cursor() as cursor:
  try:
    for agenda in agendas:
      if not agenda['in_db']:
        cursor.execute(insert_hearing_agendas, 
                       (agenda['hid'],
                        agenda['bid'], 
                        cur_date.date()))
  except:
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' +
                str(exception) +
                'when trying to insert hearing agenda.')
    logger.error(error_msg,
                 additional_fields={'_state':agenda['state']})
    return False
  return True


'''
Verifies that the return of the scraper function is a list of dictionaries
with the correct keys and returns the data if it is in the proper format.
|scraper_func|: The scraper function being checked
|keys|: The keys that the dictionary needs to have
Returns either the correct dictionary or 'False'.
'''
def verify_hearings(scraper_func, scraper_name, keys):
  error_msg = ''
  return_dict = False
  script_failed = False
  try:
    return_dict = scraper_func()
  except:
    #log the fact that the scraper failed
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' + 
                 str(exception) +
                ' issue while running ' + 
                str(scraper_name) +
                ' script.')
    logger.error(error_msg)
    script_failed = True
  if type(return_dict) is list:
    if len(return_dict) > 0:
      if type(return_dict[0]) is dict:
        for key in keys:
          if key not in return_dict[0]:
            #log the fact that a required key is missing
            error_msg = ('Error: ' + 
                        scraper_name + 
                        ' script is missing ' + 
                        key + 
                        ' key.')
            logger.warning(error_msg)
      else:
        error_msg = ('Error: ' + 
                     scraper_name + 
                     ' returns a list, but not a list of dictionaries.')
        logger.warning(error_msg)
        return False
  else:
    if not script_failed:
      #log the fact that the return type isn't a dictionary
      error_msg = ('Error: ' + 
                   scraper_name + 
                   " script doesn't return a list of dictionaries.")
      logger.warning(error_msg)
      return_dict = False

  return return_dict
  
'''
Checks all the recently scraped hearings to see if they already exist
in the database (same date and state, with an associated 
CommitteeAgenda table entry).  If not then add them, otherwise do nothing
|hearings|: The list of hearings to be inserted (or not) into DDDB
|connection|: The DDDB connection
Returns the updated list of hearings with cids and hids for each hearing.
'''
def insert_hearings(hearings, cursor):
  #with connection.cursor() as cursor:
    #check if every scraped hearing is already a part of the database
  for i in range(0, len(hearings)):
    try:
      cursor.execute(check_db_hearing, 
                     (hearings[i]['date'],
                      hearings[i]['state']))
      db_hearings = cursor.fetchall()
    except MySQLdb.Error:
      exception = sys.exc_info()[0]
      error_msg = ('Error: ' + 
                    exception + 
                    'exception during selection of DDDB hearings.')
      logger.error('Select failed', full_msg=traceback.format_exc(),
          additional_fields={'_state':'CA'})
      return False
  
    #There are hearings on that date so we have to check CommitteeHearing
    if len(db_hearings) > 0:
      for hid in db_hearings:
        try:
          cursor.execute(check_db_c_hearing, 
                         (hid[0],
                          hearings[i]['c_name']))
          cid = cursor.fetchall()
        except:
          exception = sys.exc_info()[0]
          error_msg = ('Error: ' + 
                       exception + 
                       'exception during selection of committee hearings.')
          logger.error(error_msg,
                       additional_fields={'_state':'CA'})
          return False
 

        if cid:
          hearings[i]['hid'] = hid[0]
          hearings[i]['cid'] = cid[0][0]
        else:
          #put into database
          hearings[i] = ins_hearing(hearings[i], cursor)
    else:
      hearings[i] = ins_hearing(hearings[i], cursor)
  return hearings


'''
Inserts a new hearing into both the Hearing table 
and the CommitteeHearing table.  Then return the hearing
with the new hid and cid.
|hearing|: The hearing to insert.
|cursor|: The DDDB connection cursor
'''
def ins_hearing(hearing, cursor):
  #insert into Hearing table
  try:
    cursor.execute(insert_hearing,
                   (hearing['date'],
                    hearing['state']))

    hearing['hid'] = cursor.lastrowid
  except:
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' +
                 exception + 
                 'exception during insertion of Hearing.')
    logger.error(error_msg,
                 additional_fields={'_state':hearing['state']})
    return False

  #insert into CommitteeHearing table
  try:
    cursor.execute(insert_committee_hearing,
                   (hearing['c_name'],
                    hearing['hid']))
  except:
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' + 
                 str(exception) + 
                 'exception during inesrtion of CommitteeHearing.')
    logger.error(error_msg, full_msg=traceback.format_exc(),
                 additional_fields=create_payload('CommitteeHearing',
                   insert_committee_hearing % (hearing['c_name'], hearing['hid'])))
    return False

  #Get the CID of the hearing
  try:
    cursor.execute(select_committee_hearing, 
                   (hearing['c_name'],))
  except MySQLdb.Error:
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' + 
                 str(exception) + 
                 'exception during selection of cid.')
    logger.error(error_msg,
                 additional_fields={'_state':hearing['state']})
    return False

  hearing['cid'] = cursor.fetchone()
  return hearing


'''
Checks the HearingAgenda table to make sure all agendas are a part of 
the new list of agendas.  If not set them to inactive.  If they are,
then set the in_db flag to True.
|agendas|: The list of agendas to be referenced against.
|connection|: The DDDB connection
'''
def check_agendas(agendas, cursor):
  #with connection.cursor() as cursor:
    #Fetch all active agendas
  try:
    cursor.execute(check_db_agenda)
  except:
    exception = sys.exc_info()[0]
    error_msg = ('Error: ' + 
                 exception + 
                 'exception during checking of DDDB agendas.')
    logger.error(error_msg,
               additional_fields={'_state':'CA'})
    return

  db_agendas = cursor.fetchall()

  #format them properly to check for equivalence
  db_agendas = [[agenda[0], agenda[1]] for agenda in db_agendas]
  updated_agendas = [[agenda['bid'], agenda['hid']] for agenda in agendas]


  for agenda in db_agendas:
    #if an active agenda wasn't scraped again, set it to inactive
    if agenda not in updated_agendas:
      try:
        cursor.execute(db_set_inactive, 
                       (agenda[0],
                        agenda[1]))
      except:
        exception = sys.exc_info()[0]
        error_msg = ('Error: ' + 
                     exception + 
                     'exception during setting inactive of agendas.')
        logger.error(error_msg,
               additional_fields={'_state':'CA'})
        return
    #Otherwise set the scraped agenda to "in_db" so it isn't added again
    else:
      update_ind = None
      for i in range(0, len(agendas)):
        if ((agenda[0] in agendas[i].values()) and 
            (agenda[1] in agendas[i].values())):
          update_ind = i
      agendas[update_ind]['in_db'] = True

  return agendas



'''
Runs the committee agenda scripts, cleans the data, and then puts 
it in the database if it is new.  
'''
def main():
  #call all scraper driver functions and put them in a list and aggregate them into 
  #a list of dictionaries for entry into Hearing, CommitteeHearing, and HearingAgenda tables
  hearings = [verify_hearings(scraper, 
                              scraper.__name__, 
                              scraper_keys) for scraper in scrapers]

  #tell Greylog what percenage of scrapers failed, the specific failures
  #will have already been logged in verify_hearings
  if False in hearings:
    hearings.count(False)
    error_msg = (str(hearings.count(False)) + 
                ' of ' + 
                str(len(hearings)) + 
                ' scrapers failed.')
    print(error_msg)

  #we still want the working ones to be checked
  hearings = [hearing for hearing in hearings if hearing]
  hearings = create_hearings(hearings)
 
  #keep going even if a few of the scrapers didn't work and fix them separately
  dbinfo = mysql_connection(sys.argv)
  with MySQLdb.connect(host=dbinfo['host'],
                               user=dbinfo['user'],
                               db=dbinfo['db'],
                               port=dbinfo['port'],
                               passwd=dbinfo['passwd'],
                               charset='utf8') as connection:
    #set all agendas that aren't current (Today and afterwards) to inactive
    update_db(connection)
    print(len(hearings),hearings)

    #insert the new hearings (and only them)
    hearings = insert_hearings(hearings, connection)
    print(hearings)
    #check if any of the scraped agendas are already in the database
    hearings = check_agendas(hearings, connection)

    #insert agendas that aren't already in the database
    insert_hearing_agenda(hearings, connection)

#  connection.commit()
#  connection.close()

if __name__ == '__main__':
  with GrayLogger(GRAY_URL) as _logger:
    logger = _logger 
    main()
