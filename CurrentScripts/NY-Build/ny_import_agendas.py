#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
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
"""

import json
import requests
import MySQLdb
import sys
import traceback
import re
import time
from datetime import datetime
from bs4 import BeautifulSoup
from Utils.Generic_Utils import *
from Utils.Database_Connection import connect

logger = None

STATE = 'NY'

#global counters
I_H = 0      #inserted Hearing
I_HA = 0     #inserted HearingAgenda    
I_CH = 0     #inserted CommitteeHearings
U_HA = 0     #updated HearingAgenda

#SELECT QUERIES
S_HEARING = '''SELECT h.hid
                FROM Hearing h
                JOIN CommitteeHearings ch
                ON h.hid = ch.hid
                WHERE ch.cid = %s
                AND h.date = %s'''
S_HEARING_2 = '''SELECT hid
                 FROM Hearing
                 WHERE date < %s
                 AND state = %s'''
S_HEARING_3 = '''SELECT MAX(hid)
                 FROM Hearing
                 WHERE date = %s
                 AND state = %s'''
S_SESSION = '''SELECT MAX(start_year)
               FROM Session
               WHERE start_year <= %s'''
S_COMMITTEE = '''SELECT cid
                 FROM Committee
                 WHERE house = %s
                 AND short_name = %s
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
                      AND bid = %s
                      AND date_created = %s'''
S_HEARING_AGENDA_2 = '''SELECT hid, bid, date_created
                        FROM HearingAgenda
                        WHERE date_created < %s
                        AND bid LIKE %s
                        AND current_flag = 1'''

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
                      WHERE hid = %s
                      AND bid = %s
                      AND date_created = %s'''


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
def get_hid(cursor, cid, date):
    cursor.execute(S_HEARING, (cid, date))
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
def insert_hearing(cursor, date, year, cid):
    global I_H
    #checks to see that hearing is not in db
    result = get_hid(cursor, cid, date)
    if result is None:
        try:
            cursor.execute(I_HEARING, (date, STATE, year))
            I_H += cursor.rowcount
            newHid = cursor.lastrowid

        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for Hearing', (I_HEARING % (date, STATE, year))))
    else:
        newHid = result

    return newHid

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
    if result is None:
        try:
            cursor.execute(I_COMMITTEE_HEARINGS, (cid, hid))
            I_CH += cursor.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for CommitteeHearing', (cid, hid)))


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
            b_number = int(text[1])
            bid = get_bid(cursor, b_type, b_number, house)
            if bid is None:
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
def get_hearing_agenda(cursor, hid, bid, date):
    cursor.execute(S_HEARING_AGENDA, (hid, bid, date))
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
    r_hid, r_bid, r_date = get_hearing_agenda(cursor, hid, bid, date)
    if r_hid is None:
        try:
            cursor.execute(I_HEARING_AGENDA, (hid, bid, date))
            I_HA += cursor.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for HearingAgenda', (I_HEARING_AGENDA % (hid, bid, date))))


'''
returns a list of current hid,bid,date_created from HearingAgenda with date_created prior to today
|cursor|: dddb connection
'''
def get_curr_hearing_agenda(cursor):
    date = time.strftime("%Y-%m-%d")
    bid = "NY%"
    cursor.execute(S_HEARING_AGENDA_2, (date, bid))
    result = []
    if cursor.rowcount > 0:
        for ha in cursor.fetchall():           
            result.append(ha)
    return result

'''
executes update query to set current_flag in HearingAgenda to 0
|cursor|: dddb connection
|hid|: hearing id
'''
def set_inactive(cursor, ha):
    global U_HA
    hid = ha[0]
    bid = ha[1]
    dateCreated = ha[2]
    cursor.execute(U_HEARING_AGENDA, (hid, bid, dateCreated))
    U_HA += cursor.rowcount

'''
finds all current hearing agendas and sets the ones with date_created prior to today to inactive
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
    year = time.strftime("%Y")
    mainUrl = "https://www.nysenate.gov/search/legislation?sort=desc&searched=true&type=f_agenda&agenda_year=" + year + "&page=1"
    print(mainUrl)
    page = requests.get(mainUrl)
    soup = BeautifulSoup(page.content, 'html.parser')

    #find number of pages
    agendaPages = soup.find_all(class_="pagination pager")
    p = []
    for ap in agendaPages:
        p = ap.find_all("li")
    
    numPages = len(p)

    mainUrl = "https://www.nysenate.gov/search/legislation?sort=desc&searched=true&type=f_agenda&agenda_year=" + year + "&page="
    print(mainUrl)
    results = []

    # for each page
    for i in range(1, numPages):
        newUrl = mainUrl + str(i)
        print(newUrl)
        page = requests.get(newUrl)
        soup = BeautifulSoup(page.content, 'html.parser')

        #look for meetings and grab the committee names and dates
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
    results = []

    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    house = "Senate"

    for s in soup.find_all("h4", class_="c-listing--bill-num"):
        text = s.get_text().strip()
        text = re.split('(\d.*)', text)
        bill_type = text[0]
        bill_num = text[1]
        bid = get_bid(cursor, bill_type, bill_num, house)
        if bid is not None:
            results.append(bid)

    nextUrl = get_load_more_sen_url(url)
    #loop to grab the urls under the load more button
    while nextUrl is not None:
        print(nextUrl)
        page = requests.get(nextUrl)
        soup = BeautifulSoup(page.content, 'html.parser')
        for s in soup.find_all("h4", class_="c-listing--bill-num"):
            text = s.get_text().strip()
            text = re.split('(\d.*)', text)
            bill_type = text[0]
            bill_num = text[1]
            bid = get_bid(cursor, bill_type, bill_num, house)
            if bid is not None:
                results.append(bid)

        nextUrl = get_load_more_sen_url(nextUrl)

    return results

'''
gets the url to next set of bills in meeting agenda
under the load more button
if there is no next url it will return None
if there is a next url it will return the next url
'''
def get_load_more_sen_url(url):
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    result = None
    for s in soup.find_all("div", class_="panel-pane pane-views pane-meeting-agenda-block"):
        for d in s.find_all("div", class_="item-list"): 
            if len(d.find_all("ul", class_="pager pager-load-more")) != 0:
                for ul in d.find_all("ul", class_="pager pager-load-more"):
                    for li in ul.find_all("li"):
                        for a in li.find_all("a"):
                           result = "https://www.nysenate.gov" + a['href']
    return result

def main():
    with connect() as dddb:

        #set all hearing agendas prior to today to inactive
        update_hearing_agenda(dddb)

        #get current session year
        year = get_session_year(dddb)

        #scrape assembly committee agendas
        assembly_comm_hearings = get_assembly_comm_hearings()
        for hearing in assembly_comm_hearings:
            cid = get_cid(dddb, 'Assembly', hearing['name'], year)
            if cid is not None:
                hid = insert_hearing(dddb, hearing['date'], year, cid)
            
                if hid is not None and cid is not None:
                    insert_comm_hearing(dddb, cid, hid)
                bills = scrape_hearing_agenda(dddb, hearing['url'], 'Assembly')
                if hid is not None and len(bills) > 0:
                    for bid in bills:
                        insert_hearing_agenda(dddb, hid, bid)

        #scrape senate committee agendas
        # senate_comm_hearings = get_senate_comm_hearings()
        # for hearing in senate_comm_hearings:
        #     cid = get_cid(dddb, 'Senate', hearing['name'], year)
        #     if cid is not None:
        #         hid = insert_hearing(dddb, hearing['date'], year, cid)
        #
        #         if hid is not None and cid is not None:
        #             insert_comm_hearing(dddb, cid, hid)
        #
        #         bills = scrape_senate_agenda(dddb, hearing['url'])
        #         if hid is not None and len(bills) > 0:
        #             for bid in bills:
        #                 insert_hearing_agenda(dddb, hid, bid)

    LOG = {'tables': [{'state': 'NY', 'name': 'Hearing', 'inserted':I_H, 'updated': 0, 'deleted': 0},
      {'state': 'NY', 'name': 'CommitteeHearings', 'inserted':I_CH, 'updated': 0, 'deleted': 0},
      {'state': 'NY', 'name': 'HearingAgenda', 'inserted':I_HA, 'updated': U_HA, 'deleted': 0}]}
    sys.stdout.write(json.dumps(LOG))
    logger.info(LOG)

if __name__ == '__main__':
    logger = create_logger()
    main()
