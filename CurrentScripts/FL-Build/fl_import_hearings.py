#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_import_hearings.ppy
Author: Andrew Rose
Date: 4/6/2017
Last Updated: 4/24/2017

IMPORTANT NOTE: For this script to work, the program Xpdf (specifically the pdftotext component)
MUST be installed on the server

Description:
    - This file downloads and scrapes files from Florida legislative websites
      and adds information on upcoming hearings to the database.

Source:
    - https://www.flsenate.gov/Session/Calendars/2017
    - http://www.myfloridahouse.gov/Sections/HouseSchedule/houseschedule.aspx

Populates:
    - Hearing (date, type, session_year, state)
    - CommitteeHearing (cid, hid)
    - HearingAgenda (hid, bid, date_created, current_flag)
"""

import MySQLdb
import traceback
import re
import urllib2
import requests
import sys
import datetime as dt
import subprocess
import json
import os
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger
from Constants.Hearings_Queries import *
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import *


logger = None

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated



'''
Formats the dates found in the agenda PDFs
'''
def clean_date(line):
    line = line.split(',')
    line = ''.join(line[1:]).strip()

    date = dt.datetime.strptime(line, '%B %d %Y')
    date = dt.datetime.strftime(date, '%Y-%m-%d')
    print(date)

    return date


'''
Takes the committee names listed in the agenda files
and converts them to the format that commmittee names take in our database
'''
def format_committee(comm, house, date):
    comm_name = dict()

    comm_name['house'] = house
    comm_name['state'] = 'FL'
    comm_name['session_year'] = date[:4]

    comm = comm.replace('\x0c', '')

    if house.lower() == 'house':
        subcommittee = re.match(r'.*?(?=\sSubcommittee)', comm)

        if 'Select' in comm:
            committee = comm.replace('Select Committee on', '').strip()
            comm_name['name'] = committee
            comm_name['type'] = 'Select'

        elif subcommittee is not None:
            comm_name['name'] = subcommittee.group(0)
            comm_name['type'] = 'Subcommittee'

        else:
            committee = re.match(r'.*?(?=\sCommittee)', comm)
            comm_name['name'] = committee.group(0)
            comm_name['type'] = 'Standing'

    elif house.lower() == 'senate':
        if 'Subcommittee' in comm:
            print comm
            committee = re.search(r'(?<=Subcommittee\son\s).*', comm).group(0)

            if committee[:3] == 'the':
                committee = committee.replace('the', '')

            comm_name['name'] = committee.strip()
            comm_name['type'] = 'Subcommittee'
        else:
            comm_name['name'] = comm
            comm_name['type'] = 'Standing'

    print comm_name
    return comm_name


def is_hearing_agenda_in_db(hid, bid, date, dddb):
    ha = {'hid': hid, 'bid': bid, 'date': date}

    try:
        dddb.execute(SELECT_HEARING_AGENDA, ha)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("HearingAgenda selection failed.", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("HearingAgenda", (SELECT_HEARING_AGENDA % ha)))


def is_comm_hearing_in_db(cid, hid, dddb):
    comm_hearing = {'cid': cid, 'hid': hid}

    try:
        dddb.execute(SELECT_COMMITTEE_HEARING, comm_hearing)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("CommitteeHearing selection failed.", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("CommitteeHearing", (SELECT_COMMITTEE_HEARING % comm_hearing)))


'''
Gets a specific Hearing's HID from the database
'''
def get_hearing_hid(date, house, dddb):
    session_year = date[:4]

    hearing = {'date': date, 'year': session_year, 'state': 'FL', 'house': house}

    try:
        dddb.execute(SELECT_CHAMBER_HEARING, hearing)

        if dddb.rowcount == 0:
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("Hearing selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Hearing", (SELECT_CHAMBER_HEARING % hearing)))


'''
Gets CID from our database using the committee names listed in the agendas
'''
def get_comm_cid(comm, house, date, dddb):
    comm_name = format_committee(comm, house, date)

    try:
        dddb.execute(SELECT_COMMITTEE, comm_name)

        if dddb.rowcount == 0:
            print("ERROR: Committee not found")
            return None

        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("Committee selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Committee", (SELECT_COMMITTEE % comm_name)))


'''
Gets BID using a bill's type and number
'''
def get_bill_bid(bill, date, dddb):
    session_year = date[:4]

    bill = bill.split(' ')

    bill_type = bill[0]
    bill_number = bill[1]

    bill_info = {'state': 'FL', 'session_year': session_year, 'type': bill_type, 'number': bill_number}

    try:
        dddb.execute(SELECT_BILL, bill_info)

        if dddb.rowcount == 0:
            print("ERROR: Bill not found")
            return None

        else:
            return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Bill selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Bill", (SELECT_BILL % bill_info)))


def update_hearing_agendas(hid, bid, dddb):
    global HA_UPD

    ha = {'hid': hid, 'bid': bid}

    try:
        dddb.execute(UPDATE_HEARING_AGENDA, ha)
        HA_UPD += dddb.rowcount
    except MySQLdb.Error:
        logger.warning("HearingAgenda update failed", fill_msg=traceback.format_exc(),
                       additional_fields=create_payload("HearingAgenda", (UPDATE_HEARING_AGENDA % ha)))


'''
Check if a HearingAgenda is current
If multiple agenda files list a certain HearingAgenda,
the most recent one is marked as current, and the others
are marked as not current.
'''
def check_current_agenda(hid, bid, date, dddb):
    ha = {'hid': hid, 'bid': bid}

    try:
        dddb.execute(SELECT_CURRENT_AGENDA, ha)

        if dddb.rowcount == 0:
            return 1
        else:
            curr_date = dddb.fetchone()[0]

            date = dt.datetime.strptime(date, '%Y-%m-%d').date()

            if date > curr_date:
                update_hearing_agendas(hid, bid, dddb)

                return 1
            elif date < curr_date:
                return 0
            else:
                return None

    except MySQLdb.Error:
        logger.warning("HearingAgenda selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("HearingAgenda", (SELECT_CURRENT_AGENDA % ha)))


'''
Inserts Hearings into the DB
'''
def insert_hearing(date, dddb):
    global H_INS

    hearing = dict()

    hearing['date'] = date
    hearing['session_year'] = date[:4]
    hearing['state'] = 'FL'

    print hearing

    try:
        hearing['session_year'] = 2017
        dddb.execute(INSERT_HEARING, {'date': hearing['date'], 'session_year': hearing['session_year'],
                                      'state': 'FL'})
        H_INS += dddb.rowcount

        return dddb.lastrowid

    except MySQLdb.Error:
        logger.warning("Hearing insert failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Hearing", (INSERT_HEARING % hearing)))


'''
Inserts CommitteeHearings into the DB
'''
def insert_committee_hearing(cid, hid, dddb):
    global CH_INS

    comm_hearing = {'cid': cid, 'hid': hid}

    try:
        dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
        CH_INS += dddb.rowcount

    except MySQLdb.Error:
        #print traceback.format_exc()
        logger.warning("CommitteeHearing insert failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("CommitteeHearings", (INSERT_COMMITTEE_HEARING % comm_hearing)))


'''
Inserts HearingAgendas into the DB
'''
def insert_hearing_agenda(hid, bid, date, dddb):
    global HA_INS
    current_flag = check_current_agenda(hid, bid, date, dddb)

    if current_flag is not None:
        agenda = {'hid': hid, 'bid': bid, 'date_created': date, 'current_flag': current_flag}

        try:
            dddb.execute(INSERT_HEARING_AGENDA, agenda)
            HA_INS += dddb.rowcount

        except MySQLdb.Error:
            #print traceback.format_exc()
            logger.warning("HearingAgenda insert failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("HearingAgenda", (INSERT_HEARING_AGENDA % agenda)))


'''
Scrapes agenda information on House hearings from the converted text
'''
def import_house_agendas(f, dddb):
    h_flag = 0

    date = None
    posted_date = None

    for line in f:
        if 'MEETINGS' in line:
            h_flag = 1
            # print(line)
        elif h_flag == 0 and re.match(r'^([A-Z]+),\s([A-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line):
            match = re.match(r'^([A-Z]+),\s([A-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line)
            posted_date = clean_date(match.group(0))
        elif h_flag == 1:
            if re.match(r'^([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line) is not None:
                date = clean_date(line)
                print date

                hid = get_hearing_hid(date, 'House', dddb)

            elif 'DAILY ORDER OF BUSINESS' in line:
                break

            elif date is not None:
                if 'Consideration' in line:
                    match = re.findall(r'[A-Z]{2,3}?\s[0-9]+', line)
                    if match is not None:
                        for item in match:
                            # print item
                            bid = get_bill_bid(item, date, dddb)

                            if not is_hearing_agenda_in_db(hid, bid, posted_date, dddb):
                                insert_hearing_agenda(hid, bid, posted_date, dddb)

                else:
                    comm = re.search(r'^.*?Committee.*?(?=[0-9])', line)
                    if comm is not None:
                        print comm.group(0)

                        if hid is None:
                            hid = insert_hearing(date, dddb)

                        committee = get_comm_cid(comm.group(0), 'House', date, dddb)

                        if not is_comm_hearing_in_db(committee, hid, dddb):
                            insert_committee_hearing(committee, hid, dddb)

        else:
            continue


'''
Scrapes agenda information on Senate hearings from the converted text
'''
def import_senate_agendas(f, dddb):
    h_flag = 0

    date = None
    posted_date = None
    hid = None

    for line in f:
        d_match = re.search(r'^([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line)

        if h_flag == 0 and d_match:
            posted_date = clean_date(d_match.group(0))

        elif 'SESSION DATES' in line:
            h_flag = 1

        elif h_flag == 1:
            if re.search(r'([A-Z]+),\s([A-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line):
                date = re.search(r'([A-Z]+),\s([A-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line).group(0)
                date = clean_date(date)

            elif 'BILLS ON THE CALENDAR' in line:
                break

            elif date is not None:
                if re.search(r'.*?(?=(:\s([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})))', line) is not None:
                    comm = re.search(r'.*?(?=(:\s([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})))', line)

                    if "Special Order" not in comm.group(0):
                        hid = get_hearing_hid(date, 'Senate', dddb)

                        if hid is None:
                            hid = insert_hearing(date, dddb)

                        committee = get_comm_cid(comm.group(0), 'Senate', date, dddb)

                        if not is_comm_hearing_in_db(committee, hid, dddb):
                            insert_committee_hearing(committee, hid, dddb)

                elif re.findall(r'[A-Z]{2,3}?\s[0-9]+', line) is not None:
                    match = re.findall(r'[A-Z]{2,3}?\s[0-9]+', line)
                    for item in match:
                        bid = get_bill_bid(item, date, dddb)

                        if not is_hearing_agenda_in_db(hid, bid, posted_date, dddb):
                            insert_hearing_agenda(hid, bid, posted_date, dddb)

        else:
            continue


'''
Uses XPDF's pdftotext utility to convert the agenda PDFs to text
'''
def get_agenda_text(link):
    response = requests.get(link)
    f = open("calendar.pdf", "wb")
    f.write(response.content)
    f.close()

    subprocess.call(["./pdftotext", "calendar.pdf"])


'''
Gets all House agenda PDFs listed on the Florida website
'''
def get_house_agenda(dddb):
    #html_soup = BeautifulSoup(urllib2.urlopen(HOUSE_SOURCE).read())

    f = open("fl_house_hearings.txt", "r")
    html_soup = BeautifulSoup(f.read())

    for link in html_soup.find_all('li', class_='calendarlist'):
        doc_link = 'http://www.myfloridahouse.gov' + link.find('a').get('href').strip()
        print doc_link
        get_agenda_text(doc_link)

        with open("calendar.txt", "r") as f:
            import_house_agendas(f, dddb)


'''
Gets all Senate agenda PDFs listed on the Florida website
'''
def get_senate_agenda(dddb):
    html_soup = BeautifulSoup(urllib2.urlopen(FL_HEARING_SENATE_SOURCE).read())

    for link in html_soup.find('div', class_='grid-33').find_all('li'):
        doc_link = 'https://www.flsenate.gov' + link.find('a').get('href').strip()
        print doc_link
        get_agenda_text(doc_link)

        with open("calendar.txt", "r") as f:
            import_senate_agendas(f, dddb)


def main():
    os.chdir('FL-Build/')

    dbinfo = mysql_connection(sys.argv)
    # MUST SPECIFY charset='utf8' OR BAD THINGS WILL HAPPEN.
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:

        get_senate_agenda(dddb)
        get_house_agenda(dddb)

        logger.info(__file__ + " terminated successfully",
                    full_msg="Inserted " + str(H_INS) + " rows in Hearing, "
                             + str(CH_INS) + " rows in CommitteeHearing, "
                             + str(HA_INS) + " rows in HearingAgenda, and updated "
                             + str(HA_UPD) + " rows in HearingAgenda",
                    additional_fields={'_affected_rows': 'Hearing: ' + str(H_INS)
                                                         + ', CommitteeHearing: ' + str(CH_INS)
                                                         + ', HearingAgenda: ' + str(HA_INS + HA_UPD),
                                       '_inserted': 'Hearing: ' + str(H_INS)
                                                    + ', CommitteeHearing: ' + str(CH_INS)
                                                    + ', HearingAgenda: ' + str(HA_INS),
                                       '_updated': 'HearingAgenda: ' + str(HA_UPD),
                                       '_state': 'FL'})

        LOG = {'tables': [{'state': 'FL', 'name': 'Hearing', 'inserted': H_INS, 'updated': 0, 'deleted': 0},
                          {'state': 'FL', 'name': 'CommitteeHearing', 'inserted': CH_INS, 'updated': 0, 'deleted': 0},
                          {'state': 'FL', 'name': 'HearingAgenda', 'inserted': HA_INS, 'updated': HA_UPD, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    with GrayLogger(GRAY_LOGGER_URL) as _logger:
        logger = _logger
        main()
