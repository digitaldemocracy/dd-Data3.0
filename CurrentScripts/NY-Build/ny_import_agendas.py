#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: ny_import_agendas.py
Author: James Ly
Date: 01/06/2017
Last Maintaned: James Ly
Last Updated: 01/08/2017
Description:
- Imports Hearing dates from nyassembly.gov and nysenate.gov

Tables affected:
- Hearing
- CommitteeHearings
- HearingAgenda

'''

from Database_Connection import mysql_connection
import requests
import MySQLdb
import sys
import traceback
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger
GRAY_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

STATE = 'NY'

#global counters
I_H = 0      #inserted Hearing
I_HA = 0     #inserted HearingAgenda    
I_CH = 0     #inserted CommitteeHearings
U_HA = 0     #updated HearingAgenda

#SELECT QUERIES
S_HEARING = '''SELECT hid
                FROM Hearing
                WHERE date = %s
                AND state = %s
                AND session_year = %s'''
S_HEARING_2 = '''SELECT hid
                 FROM Hearing
                 WHERE date < %s
                 AND state = %s'''
S_SESSION = '''SELECT MAX(start_year)
               FROM Session
               WHERE start_year <= %s'''
S_COMMITTEE = '''SELECT cid
                 FROM Committee
                 WHERE short_name IS NOT NULL 
                 AND house = %s
                 AND name = %s
                 AND state = %s
                 AND session_year = %s'''
S_COMMITTEE_HEARINGS = '''SELECT cid, hid
                          FROM CommitteeHearings
                          WHERE cid = %s
                          AND hid = %s'''
S_BILL = '''SELECT bid
            FROM Bill
            WHERE type = %s
            AND number = %s
            AND house = %s
            AND state = %s
            AND sessionYear = %s'''
S_HEARING_AGENDA = '''SELECT hid, bid, date_created
                      FROM HearingAgenda
                      WHERE hid = %s
                      AND bid = %s'''

#INSERT QUERIES
I_HEARING = '''INSERT INTO Hearing (date, state, session_year)
                VALUES (%s, %s, %s)'''
I_COMMITTEE_HEARINGS = '''INSERT INTO CommitteeHearings
                         (cid, hid)
                         VALUES (%s, %s)'''
I_HEARING_AGENDA = '''INSERT INTO HearingAgenda (hid, bid, date_created, current_flag)
                      VALUES (%s, %s, %s, 1)'''

#UPDATE QUERIES
U_HEARING_AGENDA = '''UPDATE HearingAgenda
                      SET current_flag = 0
                      WHERE hid = %s'''

def create_payload(table, sqlstmt):
    return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'NY'
    }

'''
given a date in mm/dd/yyyy format convert it to yyyy-mm-dd
|date|: date to be cleaned
'''
def clean_date(date):
    date = date.split("/")
    result = date[2] + "-" + date[0] + "-" + date[1]
    return result

'''
finds the current session year
|cursor|: dddb connection
'''
def get_session_year(cursor):
    year = time.strftime("%Y")
    cursor.execute(S_SESSION, (year,))
    result = cursor.fetchone()[0]
    return result

'''
gets committee names, hearing date, and url to agenda
returns a list of dictonaries {'name': name of committee, 'date': date of hearing, 'url': url to agenda}
'''
def get_assembly_comm_hearings():
    page = requests.get("http://nyassembly.gov/leg/?sh=agen")
    soup = BeautifulSoup(page.content, 'html.parser')

    results = []
    for s in soup.find_all("div", "module"):
        if 'Committee Agenda' in s.get_text():
            content = s
            for c in content.find_all("li"):
                d = dict()
                #find url to committee agenda
                a = c.find("a", href=True)
                # get committee name and date
                text = c.get_text()
                strings = re.split('(\d.*)', text)
                date = strings[1].strip()
                if len(date) > 10:
                    date = date[0:10]

                #put into dictionay
                d['name'] = strings[0].strip()
                d['date'] = clean_date(date)
                d['url'] = "http://nyassembly.gov/leg/" + a['href']
                results.append(d)

    return results


'''
finds hearing id if it is in db, returns None if not in db
|cursor|: dddb connection
|date|: date
|year|: session year
'''
def get_hid(cursor, date, year):
    cursor.execute(S_HEARING, (date, STATE, year))
    if cursor.rowcount > 0:
        result = cursor.fetchone()[0]
    else:
        result = None
    return result

'''
inserts hearing in db if it is not found
|cursor|: dddb connection
|date|: date in yyyy-mm-dd format
|year|: session year
'''
def insert_hearing(cursor, date, year):
    global I_H
    #checks to see that hearing is not in db
    if get_hid(cursor, date, year) == None:
        try:
            cursor.execute(I_HEARING, (date, STATE, year))
            I_H += cursor.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload('Hearing',(I_HEARING % (date, STATE, year))))

'''
finds committee id if it is in db, returns None if it is not in db
|cursor|: dddb connection
|house|: house the committee belongs to
|name|: name of committee
'''
def get_cid(cursor, house, name, year):
    cursor.execute(S_COMMITTEE, (house, name, STATE, year))
    if cursor.rowcount > 0:
        result = cursor.fetchone()[0]
    else:
        result = None
    return result

'''
returns a dict {'cid': committee id, 'hid': hearing id} if found in db, 
returns None if not found
|cursor|: dddb connection
|cid|: committee id
|hid|: hearing id
'''
def get_comm_hearing(cursor, cid, hid):
    d = dict()
    cursor.execute(S_COMMITTEE_HEARINGS, (cid, hid))
    if cursor.rowcount > 0:
        result = cursor.fetchone()
        d['cid'] = result[0]
        d['hid'] = result[1]
        result = d
    else:
        result = None
    return result

'''
insets committee hearing if it is not in db
|cursor|: dddb connection
|cid|: committee id
|hid|: hearing id
'''
def insert_comm_hearing(cursor, cid, hid):
    global I_CH
    result = get_comm_hearing(cursor, cid, hid)
    if result == None:
        try:
            cursor.execute(I_COMMITTEE_HEARINGS, (cid, hid))
            I_CH += cursor.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload('CommitteeHearings',(I_COMMITTEE_HEARINGS % (cid, hid))))


'''
returns a bid if found in db, else returns None
|cursor|: dddb connection
|b_type|: bill type
|b_number|: bill number
|house|: house the bill belongs to
'''
def get_bid(cursor, b_type, b_number, house):
    state = "NY"
    year = time.strftime("%Y")
    cursor.execute(S_BILL, (b_type, b_number, house, state, year))
    if cursor.rowcount > 0:
        result = cursor.fetchone()[0]
    else:
        result = None
    return result


'''
scrapes assembly hearing agenda (bills that will be discussed) from url
returns a list of bill id
|url|: url that has hearing agenda
'''
def scrape_hearing_agenda(cursor, url, house):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    results = []
    for s in soup.find_all("td"):
        for s2 in s.find_all("a", href=True):
            text = s2.get_text()
            text = re.split('(\d.*)', text)
            b_type = text[0]
            b_number = text[1]
            bid = get_bid(cursor, b_type, b_number, house)
            if bid != None:
                results.append(bid)
    return results

'''
checks to see if hearing agenda is in db,
returns hid, bid, and date_created if in db,
else returns None
|cursor|: dddb connection
|hid|: hearing id
|bid|: bill id
'''
def get_hearing_agenda(cursor, hid, bid):
    cursor.execute(S_HEARING_AGENDA, (hid, bid))
    r_hid = None
    r_bid = None
    r_date = None
    if cursor.rowcount > 0:
        result = cursor.fetchone()
        r_hid = result[0]
        r_bid = result[1]
        r_date = result[2]

    return r_hid, r_bid, r_date



'''
inserts hearing agenda if it is not in db
|cursor|: dddb connection
|hid|: hearing id
|bid|: bill id
'''
def insert_hearing_agenda(cursor, hid, bid):
    global I_HA
    date = time.strftime("%Y-%m-%d")
    r_hid, r_bid, r_date = get_hearing_agenda(cursor, hid, bid)
    if r_hid == None:
        try:
            cursor.execute(I_HEARING_AGENDA, (hid, bid, date))
            I_HA += cursor.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload('HearingAgenda',(I_HEARING_AGENDA % (hid, bid, date))))

'''
returns a list of current hid from Hearing with dates prior to today
|cursor|: dddb connection
'''
def get_curr_hearing_agenda(cursor):
    date = time.strftime("%Y-%m-%d")
    cursor.execute(S_HEARING_2, (date, STATE))
    result = []
    if cursor.rowcount > 0:
        for ha in cursor.fetchall():           
            result.append(ha[0])
    return result

'''
executes update query to set current_flag in HearingAgenda to 0
|cursor|: dddb connection
|hid|: hearing id
'''
def set_inactive(cursor, hid):
    global U_HA
    cursor.execute(U_HEARING_AGENDA, (hid,))
    U_HA += cursor.rowcount

'''
finds all current hearing agendas and sets the ones prior to today to inactive
|cursor|: dddb connection
'''
def update_hearing_agenda(cursor):
    curr_ha = get_curr_hearing_agenda(cursor)
    for ha in curr_ha:
        set_inactive(cursor, ha)

'''
given a date in format Jan 12, 2017 convert to 2017-01-12
|date|: date to convert
'''
def convert_senate_date(date):
    date = date.split(" ")
    months = {'jan' : '01',
              'feb' : '02',
              'mar' : '03',
              'apr' : '04',
              'may' : '05',
              'jun' : '06',
              'jul' : '07',
              'aug' : '08',
              'sep' : '09',
              'oct' : '10',
              'nov' : '11',
              'dec' : '12'}
    date[1] = date[1].strip(",")
    if len(date[1]) == 1:
        date = date[2] + "-" + months[date[0][:3].lower()] + "-" + "0" + date[1]
    else: 
        date = date[2] + "-" + months[date[0][:3].lower()] + "-" + date[1]
    return date

'''
get senate hearings,
returns a list of dictionaries {url: url to senate committee agenda,
                                date: date of hearing
                                name: name of committee}
'''
def get_senate_comm_hearings():
    page = requests.get("https://www.nysenate.gov/search/legislation?sort=desc&searched=true&type=f_agenda&agenda_year=2017&page=1")
    soup = BeautifulSoup(page.content, 'html.parser')
    
    results = []
    for s in soup.find_all("div", class_="c-block c-list-item c-block-legislation"):
        h3 = s.find("h3")
        a = h3.find("a", href=True)
        url = "https://www.nysenate.gov/" + a['href']
        name = s.get_text().strip()
        if "Committee" in name:
            name = name.split("Committee")
            name = name[0].strip()
        if "Meeting" in name:
            name = name.split("Meeting")
            name = name[0].strip()
        date = s.find("h4").get_text().strip()
        if "Meeting" in date:
            date = date.split("Meeting")
            date = date[1].strip()
            date = convert_senate_date(date)
        d = dict()
        d['url'] = url
        d['date'] = date
        d['name'] = name
        results.append(d)
    return results


'''
scrapes senate committee hearing agenda and gets the bills discussed
|cursor| dddb connection
|url|: url with agenda info
'''
def scrape_senate_agenda(cursor, url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    house = "Senate"

    results = []
    for s in soup.find_all("h4", class_="c-listing--bill-num"):
        text = s.get_text().strip()
        text = re.split('(\d.*)', text)
        bill_type = text[0]
        bill_num = text[1]
        bid = get_bid(cursor, bill_type, bill_num, house)
        if bid != None:
            results.append(bid)

    return results

def main():
    ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=ddinfo['host'],
                        user=ddinfo['user'],
                        db=ddinfo['db'],
                        port=ddinfo['port'],
                        passwd=ddinfo['passwd'],
                        charset='utf8') as dddb:

        #set all hearing agendas prior to today to inactive
        update_hearing_agenda(dddb)

        #get current session year
        year = get_session_year(dddb)

        #scrape assembly committee agendas
        assembly_comm_hearings = get_assembly_comm_hearings()
        for hearing in assembly_comm_hearings:
            insert_hearing(dddb, hearing['date'], year)
            hid = get_hid(dddb, hearing['date'], year)
            cid = get_cid(dddb, 'Assembly', hearing['name'], year)
            
            if hid != None and cid != None:
                insert_comm_hearing(dddb, cid, hid)
            bills = scrape_hearing_agenda(dddb, hearing['url'], 'Assembly')
            if hid != None and len(bills) > 0:
                for bid in bills:
                    insert_hearing_agenda(dddb, hid, bid)

        #scrape senate committee agendas
        senate_comm_hearings = get_senate_comm_hearings()
        for hearing in senate_comm_hearings:
            insert_hearing(dddb, hearing['date'], year)
            hid = get_hid(dddb, hearing['date'], year)
            cid = get_cid(dddb, 'Senate', hearing['name'], year)
            if hid != None and cid != None:
                insert_comm_hearing(dddb, cid, hid)
            bills = scrape_senate_agenda(dddb, hearing['url'])
            if hid != None and len(bills) > 0:
                for bid in bills:
                    insert_hearing_agenda(dddb, hid, bid)

        
        logger.info(__file__ + ' terminated successfully.', 
            full_msg='Inserted ' + str(I_H) + ' rows in Hearing and inserted ' 
                      + str(I_CH) + ' rows in CommitteeHearings and inserted '
                      + str(I_HA) + ' rows in HearingAgenda',
            additional_fields={'_affected_rows':'Hearing:'+ str(I_H) +
                                           ', CommitteeHearings:'+ str(I_CH) +
                                           ', HearingAgenda:' + str(I_HA+U_HA),
                               '_inserted':'Hearing:'+ str(I_H) +
                                           ', CommitteeHearings:' + str(I_CH) +
                                           ', HearingAgenda:'+ str(I_HA),
                                '_updated':'HearingAgenda:'+ str(U_HA),
                               '_state':'NY'})

        print "Updated " + str(U_HA) + " rows in HearingAgenda"
        print "Inserted " + str(I_H) + " rows into Hearing"
        print "Inserted " + str(I_CH) + " rows into CommitteeHearings"
        print "Inserted " + str(I_HA) + " rows into HearingAgenda"
    

if __name__ == '__main__':
    with GrayLogger(GRAY_URL) as _logger:
        logger = _logger
        main()
