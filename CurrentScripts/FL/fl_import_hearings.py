#!/usr/bin/python3

"""
File: fl_import_hearings.py
Author: Andrew Rose
Date: 4/6/2017
Last Updated: 10/27/2017

IMPORTANT NOTE: For this script to work, the program Xpdf (specifically the pdftotext component)
MUST be installed on the server

Description:
    - This file downloads and scrapes files from Florida legislative websites
      and adds information on upcoming hearings to the database.

Source:
    - https://www.flsenate.gov/Session/Calendars/2018
    - http://www.myfloridahouse.gov/Sections/HouseSchedule/houseschedule.aspx

Populates:
    - Hearing (date, type, session_year, state)
    - CommitteeHearing (cid, hid)
    - HearingAgenda (hid, bid, date_created, current_flag)
"""

import os
import json
import requests
import subprocess
import traceback
from urllib.request import urlopen
from Models.Hearing import *
from bs4 import BeautifulSoup
from Utils.Generic_MySQL import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Hearing_Manager import *
from Utils.File_Comparator import *
from Constants.General_Constants import *
from Constants.Hearings_Queries import *


logger = None

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated

CURRENT_TXT = "" # Name of the Current hearing text converted from pdf
NO_PARSING_ERROR = 1 # Flag used to indicate whether an error occurred during the import of the current text's hearing info

'''
Formats the dates found in the agenda PDFs
'''
def clean_date(line):
    line = line.split(',')
    line = ''.join(line[1:]).strip()

    date = dt.datetime.strptime(line, '%B %d %Y')
    date = dt.datetime.strftime(date, '%Y-%m-%d')

    return date


'''
Takes the committee names listed in the agenda files
and converts them to the format that commmittee names take in our database
'''
def format_committee(comm, house, date, subcomm=None):
    # if comm == 'and Economic Development':
    #     comm = 'Appropriations Subcommittee on Transportation, Tourism, and Economic Development'
    # print(comm)
    comm_name = dict()

    comm_name['house'] = house
    comm_name['state'] = 'FL'
    comm_name['session_year'] = str(date.year)

    comm = comm.replace('\x0c', '')
    comm = comm.split('(')[0]

    if subcomm is not None:
        subcomm = subcomm.replace('\x0c', '')
        subcomm = subcomm.split('(')[0]
        subcomm = subcomm.split('Subcommittee')[0].strip()

        comm_name['name'] = subcomm
        comm_name['type'] = 'Subcommittee'

    elif house.lower() == 'house':
        subcommittee = re.match(r'.*?(?=\sSubcommittee)', comm)

        if 'Joint' in comm:
            comm_name['house'] = 'Joint'

            if ' Select ' in comm:
                comm_name['type'] = 'Joint Select'
                committee = comm.replace('Joint Select Committee on', '').strip()
                comm_name['name'] = committee

            else:
                committee = comm.replace('Joint Committee on', '').strip()
                committee = committee.replace('Committee', '').strip()
                comm_name['name'] = committee
                comm_name['type'] = 'Joint'

        elif 'Select' in comm:
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
        if 'Joint' in comm:
            comm_name['house'] = 'Joint'

            if ' Select ' in comm:
                comm_name['type'] = 'Joint Select'
                committee = comm.replace('Joint Select Committee on', '').strip()
                comm_name['name'] = committee

            else:
                committee = comm.replace('Joint Committee on', '').strip()
                committee = committee.replace('Committee', '').strip()
                comm_name['name'] = committee
                comm_name['type'] = 'Joint'

        elif 'Subcommittee' in comm:
            committee = re.search(r'(?<=Subcommittee\son\s).*', comm).group(0)

            if committee[:3] == 'the':
                committee = committee.replace('the', '')

            comm_name['name'] = committee.strip()
            comm_name['type'] = 'Subcommittee'
        else:
            comm_name['name'] = comm
            comm_name['type'] = 'Standing'
    return comm_name


def is_hearing_agenda_in_db(hid, bid, date, dddb):
    global NO_PARSING_ERROR
    ha = {'hid': hid, 'bid': bid, 'date': date}

    try:
        dddb.execute(SELECT_HEARING_AGENDA, ha)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.exception(format_logger_message("Selection failed for HearingAgenda", (SELECT_HEARING_AGENDA % ha)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)

def is_comm_hearing_in_db(cid, hid, dddb):
    global NO_PARSING_ERROR
    comm_hearing = {'cid': cid, 'hid': hid}

    try:
        dddb.execute(SELECT_COMMITTEE_HEARING, comm_hearing)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.exception(format_logger_message("Selection failed for CommitteeHearing", (SELECT_COMMITTEE_HEARING % comm_hearing)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)

'''
Gets a specific Hearing's HID from the database
'''
def get_hearing_hid(date, house, dddb):
    global NO_PARSING_ERROR
    session_year = date[:4]

    hearing = {'date': date, 'year': session_year, 'state': 'FL', 'house': house}

    try:
        dddb.execute(SELECT_CHAMBER_HEARING, hearing)

        if dddb.rowcount >= 1:
            return dddb.fetchone()[0]
        else:
            return None

    except MySQLdb.Error:
        logger.exception(format_logger_message("Selection failed for Hearing", (SELECT_CHAMBER_HEARING % hearing)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)


'''
Gets CID from our database using the committee names listed in the agendas
'''
def get_comm_cid(comm, house, date, dddb, subcomm=None):
    global NO_PARSING_ERROR
    comm_name = format_committee(comm, house, date, subcomm)
    if comm_name['name'] == "Noticed Meeting":
        return None
    try:
        cid = get_entity_id(dddb, SELECT_COMMITTEE_SHORT_NAME, comm_name, 'Committee', logger)
        if not cid:
            cid = get_entity_id(dddb, SELECT_COMMITTEE_ALTERNATE_NAME, comm_name, 'Committee', logger)
            if not cid:
                cid = get_entity_id(dddb, SELECT_COMMITTEE_SHORT_NAME_NO_TYPE, comm_name, 'Committee', logger)
                if not cid:
                    cid = get_entity_id(dddb, SELECT_COMMITTEE, comm_name, 'Committee', logger)
                    if not cid:
                        logger.exception("ERROR: Committee not found: " + str(comm_name))
                        NO_PARSING_ERROR = 0
                        move_to_error_folder(CURRENT_TXT)
                        return None
                    else:
                        return cid
                else:
                    return cid
            else:
                return cid
        else:
            return cid

    except MySQLdb.Error:
        logger.exception(format_logger_message("Selection failed for Committee", (SELECT_COMMITTEE % comm_name)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)


'''
Gets BID using a bill's type and number
'''
def get_bill_bid(bill, date, dddb):
    global NO_PARSING_ERROR
    session_year = get_session_year(dddb, 'FL', logger)

    s_bill = bill.split(' ')
    if len(s_bill) < 2:
        logger.exception('bill code not formatted correctly: ' + str(bill))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)
        return None
    bill_type = s_bill[0]
    bill_number = s_bill[1]

    bill_info = {'state': 'FL', 'session_year': session_year, 'type': bill_type, 'number': bill_number}

    try:
        dddb.execute(SELECT_BILL, bill_info)

        if dddb.rowcount == 0:
            logger.exception("ERROR: Bill not found: " + str(bill_info))
            print(str(bill) + " date: " + str(date) + " session_year: " + str(session_year))
            NO_PARSING_ERROR = 0
            move_to_error_folder(CURRENT_TXT)
        else:
            return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.exception(format_logger_message("Selection failed for Bill", (SELECT_BILL % bill_info)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)


def update_hearing_agendas(hid, bid, dddb):
    global HA_UPD, NO_PARSING_ERROR

    ha = {'hid': hid, 'bid': bid}

    try:
        dddb.execute(UPDATE_HEARING_AGENDA, ha)
        HA_UPD += dddb.rowcount
    except MySQLdb.Error:
        logger.exception(format_logger_message("Update failed for HearingAgenda", (UPDATE_HEARING_AGENDA % ha)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)

'''
Check if a HearingAgenda is current
If multiple agenda files list a certain HearingAgenda,
the most recent one is marked as current, and the others
are marked as not current.
'''
def check_current_agenda(hid, bid, date, dddb):
    global NO_PARSING_ERROR
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
        logger.exception(format_logger_message("Selection failed for HearingAgenda", (SELECT_CURRENT_AGENDA % ha)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)

'''
Inserts Hearings into the DB
'''
def insert_hearing(date, dddb):
    global H_INS, NO_PARSING_ERROR

    hearing = dict()

    hearing['date'] = date
    hearing['session_year'] = get_session_year(dddb, 'FL', logger)
    hearing['state'] = 'FL'

    try:
        dddb.execute(INSERT_HEARING, {'date': hearing['date'], 'session_year': hearing['session_year'],
                                      'state': 'FL'})
        H_INS += dddb.rowcount

        return dddb.lastrowid

    except MySQLdb.Error:
        logger.exception(format_logger_message("Insert failed for Hearing", (INSERT_HEARING % hearing)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)

'''
Inserts CommitteeHearings into the DB
'''
def insert_committee_hearing(cid, hid, dddb):
    global CH_INS, NO_PARSING_ERROR

    comm_hearing = {'cid': cid, 'hid': hid}

    try:
        # if cid == None:
        #     print(traceback.format_stack())
        #     exit()
        dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
        CH_INS += dddb.rowcount

    except MySQLdb.Error:
        logger.exception(format_logger_message("Insert failed for CommitteeHearing", (INSERT_COMMITTEE_HEARING % comm_hearing)))
        NO_PARSING_ERROR = 0
        move_to_error_folder(CURRENT_TXT)

'''
Inserts HearingAgendas into the DB
'''
def insert_hearing_agenda(hid, bid, date, dddb):
    global HA_INS, NO_PARSING_ERROR
    current_flag = check_current_agenda(hid, bid, date, dddb)
    if hid is None:
        exit()
    if current_flag is not None:
        agenda = {'hid': hid, 'bid': bid, 'date_created': date, 'current_flag': current_flag}

        try:
            dddb.execute(INSERT_HEARING_AGENDA, agenda)
            HA_INS += dddb.rowcount

        except MySQLdb.Error:
            logger.exception(format_logger_message("Insert failed for HearingAgenda", (INSERT_HEARING_AGENDA % agenda)))
            NO_PARSING_ERROR = 0
            move_to_error_folder(CURRENT_TXT)

'''
Scrapes agenda information on House hearings from the converted text
'''
def import_house_agendas(f, dddb):
    hearing_list = []

    session_year = get_session_year(dddb, 'FL', logger)
    h_flag = 0

    date = None
    committee = None

    is_subcomm = False
    subcomm = None

    for line in f:
        if 'MEETINGS' in line:
            h_flag = 1

        elif h_flag == 1:
            if re.match(r'^([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line) is not None:
                date = clean_date(line)
                date = dt.datetime.strptime(date, '%Y-%m-%d')
                print(date)

            elif 'DAILY ORDER OF BUSINESS' in line \
                    or 'BILL INDEX' in line \
                    or 'IMPORTANT LEGISLATIVE DATES' in line \
                    or 'BILLS FILED' in line\
                    or 'COMMITTEES & SUBCOMMITTEES OF THE FLORIDA HOUSE OF REPRESENTATIVES' in line:
                print('**********reached sentinel line for hearing schedule in text file*********\nLINE: ' + line)
                break

            elif date is not None:
                if 'Consideration' in line:
                    match = re.findall(r'(HB\s[0-9]+|HCR\s[0-9]+|HJR\s[0-9]+|HR\s[0-9]+|HM\s[0-9]+)', line)
                    if match is not None:
                        for item in match:

                            bid = get_bill_bid(item, date, dddb)
                            #print(bid)
                            hearing = Hearing(date, 'House', 'Regular', 'FL', session_year,
                                              committee, bid, source=CURRENT_TXT)

                            hearing_list.append(hearing)

                else:
                    if is_subcomm is True:
                        comm = re.search(r'^.*?Committee.*?(?=[0-9])', line)
                        if comm is not None:
                            if 'the' not in comm.group(0).lower():
                                committee = get_comm_cid(comm.group(0), 'House', datetime.strptime(str(session_year),"%Y"), dddb, subcomm.group(0))
                                # print(committee)

                                hearing = Hearing(date, 'House', 'Regular', 'FL', session_year,
                                                  committee, None, source=CURRENT_TXT)

                                hearing_list.append(hearing)
                        is_subcomm = False
                        subcomm = None

                    else:
                        subcomm = re.search(r'^.*?Subcommittee$', line)
                        if subcomm is not None:
                            is_subcomm = True
                            # print(subcomm)

                        comm = re.search(r'^.*?Committee.*?(?=[0-9])', line)
                        if comm is not None:
                            if 'the' not in comm.group(0).lower():
                                cleaned = re.sub(r'^[0-9]:[0-9]{2}\s[APM]{2}\s-\s[0-9]:[0-9]{2}\s[APM]{2}\s', '', comm.group(0))
                                committee = get_comm_cid(cleaned, 'House', datetime.strptime(str(session_year),"%Y"), dddb)
                                # print(committee)
                                hearing = Hearing(date, 'House', 'Regular', 'FL', session_year,
                                                  committee, None, source=CURRENT_TXT)

                                hearing_list.append(hearing)

        else:
            continue

    return hearing_list


'''
Scrapes agenda information on Senate hearings from the converted text
'''
def import_senate_agendas(f, dddb):
    h_flag = 0

    date = None
    committee = None

    session_year = get_session_year(dddb, 'FL', logger)

    hearings_list = []

    for line in f:

        if 'SESSION DATES' in line:
            h_flag = 1

        elif h_flag == 1:
            if re.search(r'([A-Z]+),\s([A-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line):
                date = re.search(r'([A-Z]+),\s([A-Z]+)\s([0-9]{1,2}),\s([0-9]{4})', line).group(0)
                date = clean_date(date)
                date = dt.datetime.strptime(date, '%Y-%m-%d')
                print(date)

            elif 'BILLS ON THE CALENDAR' in line:
                break

            elif date is not None:
                if re.search(r'.*?(?=(:\s([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})))', line) is not None:
                    # Should check to make sure we're still parsing senate committee names correctly
                    comm = re.search(r'.*?(?=(:\s([a-zA-Z]+),\s([a-zA-Z]+)\s([0-9]{1,2}),\s([0-9]{4})))', line)
                    # print(comm)
                    if "Special Order" not in comm.group(0):

                        committee = get_comm_cid(comm.group(0), 'Senate', date, dddb)

                        hearing = Hearing(date, 'Senate', 'Regular', 'FL', session_year,
                                          committee, None, source=CURRENT_TXT)

                        hearings_list.append(hearing)

                elif re.findall(r'(SB\s[0-9]+|SCR\s[0-9]+|SJR\s[0-9]+|SR\s[0-9]+|SM\s[0-9]+|SPB\s[0-9]+)', line) is not None:
                    match = re.findall(r'(SB\s[0-9]+|SCR\s[0-9]+|SJR\s[0-9]+|SR\s[0-9]+|SM\s[0-9]+|SPB\s[0-9]+)', line)
                    for item in match:
                        # there is no SB 10 or SB 12 for the 2018 session however 2017 and 2016 bills
                        # which already passed are mentioned in hearing agenda pdfs,
                        # Currently only including in agendas current bills making
                        # there way through the legislature.
                        # if session_year == 2018 and (item == 'SB 10' or item == 'SB 12'):
                            # print('****************************SKIPPING ' + item + '*************************')
                            # continue
                        bid = get_bill_bid(item, date, dddb)

                        hearing = Hearing(date, 'Senate', 'Regular', 'FL', session_year,
                                          committee, bid, source=CURRENT_TXT)
                        hearings_list.append(hearing)

        else:
            continue

    return hearings_list


'''
Uses XPDF's pdftotext utility to convert the agenda PDFs to text
'''
def get_agenda_text(link, file_comparator):
    response = requests.get(link)
    f = open("calendar.pdf", "wb")
    f.write(response.content)
    f.close()
    # print(pdf_to_text_path())
    global CURRENT_TXT
    CURRENT_TXT = "calendar_txt/" + (link[-37:-4]).strip() + ".txt"
    print(CURRENT_TXT)
    test = pdf_to_text_path()
    subprocess.call([test, "-enc","UTF-8", "calendar.pdf", CURRENT_TXT])

    if file_comparator.is_new('FL_Hearings', CURRENT_TXT):
        return;
    else:
        os.remove(CURRENT_TXT)
        CURRENT_TXT = ""


'''
Gets all House agenda PDFs listed on the Florida website
'''
def get_house_agenda(dddb, file_comparator):
    global NO_PARSING_ERROR
    #get interim session hearing agendas:
    # source_url = FL_HEARING_HOUSE_INTERIM_SOURCE + "?calendarListType=Session&date=" + dt.datetime.now().strftime('%m-%d-%Y')

    html_soup = BeautifulSoup(urlopen(FL_HEARING_HOUSE_INTERIM_SOURCE).read(), "lxml")

    hearing_list = []

    for link in html_soup.find_all('li', class_='calendarlist'):
        doc_link = 'http://www.myfloridahouse.gov' + link.find('a').get('href').strip()
        print(doc_link)
        get_agenda_text(doc_link, file_comparator)

        if CURRENT_TXT != "":
            with open(CURRENT_TXT, "r") as f:
                hearing_list += import_house_agendas(f, dddb)
                if NO_PARSING_ERROR == 1:
                    file_comparator.add_file_hash('FL_Hearings', CURRENT_TXT)
                else:
                    NO_PARSING_ERROR = 1
    #get Regular session hearing agendas
    html_soup = BeautifulSoup(urlopen(FL_HEARING_HOUSE_SESSION_SOURCE).read(), "lxml")

    for link in html_soup.find_all('div', class_='doc_listing'):
        doc_link = 'http://www.myfloridahouse.gov' + link.find('a').get('href').strip()
        print(doc_link)
        get_agenda_text(doc_link, file_comparator)

        if CURRENT_TXT != "":
            with open(CURRENT_TXT, "r") as f:
                hearing_list += import_house_agendas(f, dddb)
                if NO_PARSING_ERROR == 1:
                    file_comparator.add_file_hash('FL_Hearings', CURRENT_TXT)
                else:
                    NO_PARSING_ERROR = 1
    return hearing_list


'''
Gets all Senate agenda PDFs listed on the Florida website
'''
def get_senate_agenda(dddb, file_comparator):
    global NO_PARSING_ERROR
    html_soup = BeautifulSoup(urlopen(FL_HEARING_SENATE_SOURCE).read(), "lxml")

    hearings_list = []

    for link in html_soup.find('div', class_='grid-33').find_all('li'):
        doc_link = 'https://www.flsenate.gov' + link.find('a').get('href').strip()
        print(doc_link)

        get_agenda_text(doc_link, file_comparator)
        if CURRENT_TXT != "":
            with open(CURRENT_TXT, "r") as f:
                hearings_list += import_senate_agendas(f, dddb)
                if NO_PARSING_ERROR == 1:
                    print('adding file')
                    file_comparator.add_file_hash('FL_Hearings', CURRENT_TXT)
                else:
                    print("parsing error")
                    NO_PARSING_ERROR = 1

    return hearings_list


def remove_error_file_hashes(file_comparator, logger):
    path = os.getcwd() + '/ErrorFiles'
    if os.path.exists(path):
        for f_name in os.listdir(path):
            if '.txt' in f_name:
                file_comparator.remove_file_hash('FL_Hearings', f_name)
    else:
        logger.exception('No error file directory at: ' + path)


def remove_imported_txt_files(cwd):
    path = cwd + '/calendar_txt/'
    if os.path.exists(path):
        for f_name in os.listdir(path):
            if '.txt' in f_name:
                os.remove(path+f_name)
            else:
                print(f_name + " is not a .txt file")
    else:
        logger.exception('Could not find txt file directory using path: ' + path)


def main():
    with connect() as dddb:
        with connect_to_hashDB() as hashDB:
            cur_date = dt.datetime.now().strftime('%Y-%m-%d')

            hearing_manager = Hearings_Manager(dddb, 'FL', logger)
            file_comparator = KnownFileComparator(hashDB, logger)
            # senate_hearings = list(set(get_senate_agenda(dddb, file_comparator)))
            house_hearings = list(set(get_house_agenda(dddb, file_comparator)))

            # hearing_manager.import_hearings(senate_hearings, cur_date)
            hearing_manager.import_hearings(house_hearings, cur_date)
            remove_error_file_hashes(file_comparator, logger)
            remove_imported_txt_files(os.getcwd())
            hearing_manager.log()


if __name__ == '__main__':
    logger = create_logger()
    main()
