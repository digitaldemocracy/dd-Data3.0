#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
File: get_Committee_Consultants.py
Author: Andrew Rose
Date: 01/05/2017

Description:
- Scrapes the Assembly and Senate websites to gather committee consultants

Sources:
    - California Assembly Website
    - California Senate Website

Populates:
    - ConsultantServesOn (pid, session_year, cid, position, current_flag, start_date, end_date, state)

'''

from Database_Connection import mysql_connection
from graylogger.graylogger import GrayLogger
from bs4 import BeautifulSoup
import sys
import MySQLdb
import urllib2
import traceback
import datetime as dt
API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

# Global Counters
C_INSERT = 0
C_UPDATE = 0

STATE = 'CA'
SENATE_PROBLEM_SITES = ['seuc', 'shum', 'sjud', 'spsf', 'srul', 'stran', 'svet', 'shea']

# SQL Queries

SELECT_SESSION_YEAR = '''SELECT MAX(start_year)
                         FROM Session
                         WHERE state = 'CA'
                      '''

SELECT_COMMITTEE_CID = '''SELECT cid
                          FROM Committee
                          WHERE house = %(house)s
                          AND state = 'CA'
                          AND short_name = %(committee)s
                       '''

SELECT_CONSULT_PID = '''SELECT p.pid
                        FROM Person p, LegislativeStaff l
                        WHERE p.pid = l.pid
                        AND l.state = 'CA'
                        AND p.first = %(first)s
                        AND p.last = %(last)s
                     '''

SELECT_PERSON = '''SELECT pid
                   FROM Person
                   WHERE first = %(first)s
                   AND last = %(last)s
                '''

SELECT_CONSULT_SERVESON = '''SELECT pid
                             FROM ConsultantServesOn
                             WHERE pid = %(pid)s
                             AND state = 'CA'
                             AND session_year = %(year)s
                             AND cid = %(cid)s
                          '''

SELECT_CURRENT_MEMBERS = '''SELECT pid
                            FROM ConsultantServesOn
                            WHERE cid = %(cid)s
                            AND state = 'CA'
                            AND current_flag = true
                         '''

INSERT_PERSON = '''INSERT
                   INTO Person
                   (first, last)
                   VALUES
                   (%(first)s, %(last)s)
                '''

INSERT_LEGSTAFF = '''INSERT
                     INTO LegislativeStaff
                     (pid, state)
                     VALUES
                     (%(pid)s, %(state)s)
                  '''

INSERT_CONSULT_SERVESON = '''INSERT
                             INTO ConsultantServesOn
                             (pid, session_year, cid, position, current_flag, start_date, state)
                             VALUES
                             (%(pid)s, %(session_year)s, %(cid)s, %(position)s, %(current_flag)s, %(start_date)s, 'CA')
                          '''

UPDATE_CONSULTANTS = '''UPDATE ConsultantServesOn
                        SET current_flag = %(current_flag)s,
                            end_date = %(end_date)s
                        WHERE pid = %(pid)s
                        AND cid = %(cid)s
                        AND state = %(state)s
                     '''

def create_payload(table, sqlstmt):
    return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'CA',
      '_log_type': 'Database'
    }


def clean_strings(con_name):
    cleaned_strings = []

    if 'NavigableString' in str(type(con_name)):
        con_name = str(con_name.encode('utf-8'))

    # Remove whitespace and colons from string
    con_name = con_name.strip(':').strip('\xc2\xa0').strip()
    
    # If consultant names are listed with commas, separate them
    if ',' in con_name and 'Jr.' not in con_name:
        name_list = con_name.split(',')
        #print(name_list)
        for name in name_list:
            strip_name = name.strip().strip('\xc2\xa0')

            # If the last name is separated with 'and'
            if ' and ' in name:
                and_names = name.split(' and ')
                for a_name in and_names:
                    strip_name = a_name.strip().strip('\xc2\xa0')
                    if len(strip_name) >= 1:
                        print(strip_name)
                        cleaned_strings.append(strip_name)
            else:
                strip_name = name.strip().strip('\xc2\xa0')
                if len(strip_name) >= 1:
                    print(strip_name)
                    cleaned_strings.append(strip_name)
    # If consultant names are separated with 'and', separate.
    elif ' and ' in con_name:
        name_list = con_name.split(' and ')
        for name in name_list:
            strip_name = name.strip().strip('\xc2\xa0')
            print(strip_name)
            cleaned_strings.append(strip_name)
    else:
        if len(con_name) >= 1:
            print(con_name)
            cleaned_strings.append(con_name)
    return cleaned_strings


def get_committee_cid(house, committee, dd):
    query_dict = {'house': house, 'committee': committee}

    try:
        dd.execute(SELECT_COMMITTEE_CID, query_dict)
        cid = dd.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Committee selection failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('Committee', (SELECT_COMMITTEE_CID % query_dict)))
        return None

    return cid


def get_consultant_pid(consultant, dd):
    legstaff_pid = is_legstaff_in_db(consultant, dd)
    if legstaff_pid is not False:
        return legstaff_pid
    else:
        legstaff_pid = is_person_in_db(consultant, dd)

        if legstaff_pid is not False:
            insert_legstaff(legstaff_pid, dd)
            return legstaff_pid
        else:
            insert_person(consultant, dd)
            legstaff_pid = is_person_in_db(consultant, dd)
            insert_legstaff(legstaff_pid, dd)
            return legstaff_pid


def is_legstaff_in_db(consultant, dd):
    person_query_dict = {'first': consultant['first'], 'last': consultant['last']}
    try:
        dd.execute(SELECT_CONSULT_PID, person_query_dict)
        query = dd.fetchone()
        if dd.rowcount > 0:
            return query[0]
        else:
            return False
    except MySQLdb.Error:
        logger.warning("Consultant selection failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('Person', (SELECT_CONSULT_PID % person_query_dict)))
        return None


def is_person_in_db(consultant, dd):
    query_dict = {'first': consultant['first'], 'last': consultant['last']}

    try:
        dd.execute(SELECT_PERSON, query_dict)
        query = dd.fetchone()
        if dd.rowcount > 0:
            return query[0]
        else:
            return False
    except MySQLdb.Error:
        logger.warning("Person selection failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('Person', (SELECT_PERSON % query_dict)))
        return None


def is_consultant_in_db(consultant, house, dd):
    query_dict = {'pid': consultant['pid'], 'cid': consultant['cid'], 'year': consultant['session_year']}

    try:
        dd.execute(SELECT_CONSULT_SERVESON, query_dict)
        query = dd.fetchone()
        if query is None:
            return False
    except MySQLdb.Error:
        logger.warning("Select statement failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('ConsultantServesOn', (SELECT_CONSULT_SERVESON % query_dict)))
    return True


def insert_person(consultant, dd):
    insert_dict = {'first': consultant['first'], 'last': consultant['last']}

    try:
        dd.execute(INSERT_PERSON, insert_dict)
    except MySQLdb.Error:
        logger.warning("Insert statement failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('Person', (INSERT_PERSON % insert_dict)))


def insert_legstaff(legstaff_pid, dd):
    insert_dict = {'pid': legstaff_pid, 'state': STATE}

    try:
        dd.execute(INSERT_LEGSTAFF, insert_dict)
    except MySQLdb.Error:
        logger.warning("Insert statement failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('LegislativeStaff', (INSERT_LEGSTAFF % insert_dict)))


def get_consultant_info(name, header, dd):
    consultant = dict()

    names = name.strip().split(' ')

    if len(names) > 2:
        if 'Jr' in names[-1] or 'Sr' in names[-1]:
            consultant['first'] = names[0]
            consultant['last'] = names[-2]
        elif names[-2].lower == 'de':
            consultant['first'] = names[0]
            consultant['last'] = names[-2] + ' ' + names[-1]
        else:
            consultant['first'] = ' '.join(names[0:-1])
            consultant['last'] = names[-1]
    else:
        consultant['first'] = names[0]
        consultant['last'] = names[1]

    consultant['name'] = name
    if 'Deputy Chief' in header.contents:
        consultant['position'] = 'Deputy Chief Consultant'
    elif 'Secretary' in header.contents:
        consultant['position'] = 'Committee Secretary'
    elif 'Chief' in header.contents:
        consultant['position'] = 'Chief Consultant'
    else:
        consultant['position'] = None
    consultant['session_year'] = get_session_year(dd)

    return consultant


def get_session_year(dd):
    dd.execute(SELECT_SESSION_YEAR)

    try:
        year = dd.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Select statement failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload("Session", SELECT_SESSION_YEAR))
        return None

    return year


def get_problematic_sites(comm_url, dd):
    consultant_names = list()
    commName = comm_url.split('.')[0][7:]
    htmlSoup = BeautifulSoup(urllib2.urlopen(comm_url).read())

    print("Problem site identified: " + commName)
    if commName == 'shum':
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('ul'):
            #print(header.contents[1].string)
            title_header = header.find_previous_sibling('p').find('strong')
            for name in clean_strings(header.contents[1].string):
                consultant_names.append(get_consultant_info(name, title_header, dd))
    elif commName == 'spsf':
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('p'):
            hcontents = header.contents[0]
            if hcontents.name is not None:
                title_header = hcontents
            elif hcontents.name is None:
                #print(hcontents)
                for name in clean_strings(hcontents):
                    consultant_names.append(get_consultant_info(name, title_header, dd))
    elif commName == 'srul':
        header = htmlSoup.find_all('div', 'sidebar-information')[1].find('strong')
        #print(header.next_sibling)
        #print(header.find_next_sibling('a').string)
        for name in clean_strings(header.next_sibling):
            consultant_names.append(get_consultant_info(name, header, dd))
        for name in clean_strings(header.find_next_sibling('a').string):
            consultant_names.append(get_consultant_info(name, header, dd))
    else:
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('strong'):
            if commName == 'seuc':
                #print(header.find_next_sibling('a').string)
                for name in clean_strings(header.find_next_sibling('a').string):
                    consultant_names.append(get_consultant_info(name, header, dd))
            if commName == 'sjud':
                nextSib = header.next_sibling
                if nextSib is not None:
                    sibName = header.next_sibling.name
                    if sibName != 'br':
                        #print(nextSib)
                        for name in clean_strings(nextSib):
                            consultant_names.append(get_consultant_info(name, header, dd))
            if commName == 'stran' or commName == 'svet':
                #print(header.find_next_sibling('a').string)
                for name in clean_strings(header.find_next_sibling('a').string):
                    consultant_names.append(get_consultant_info(name, header, dd))
            if commName == 'shea':
                nextSib = header.next_sibling
                sibName = nextSib.name
                while nextSib is not None:
                    if nextSib.name == 'strong':
                        break;
                    elif nextSib.name == 'br':
                        nextSib = nextSib.next_sibling
                    else:
                        for name in clean_strings(nextSib):
                            consultant_names.append(get_consultant_info(name, header, dd))
                        nextSib = nextSib.next_sibling
    return consultant_names


def scrape_consultants(comm_url, house, dd):
    global SENATE_PROBLEM_SITES
    consultant_names = list()
    
    if house.lower() == 'assembly':
        commName = comm_url.split('.')[0][7:]
        if commName in ASSEMBLY_PROBLEM_SITES:
            consultant_names = get_problem_sites(comm_url, house)
        else:
            host = comm_url + '/committeestaff'
            htmlSoup = BeautifulSoup(urllib2.urlopen(host).read())

    elif house.lower() == 'senate':
        commName = comm_url.split('.')[0][7:]
        if commName in SENATE_PROBLEM_SITES:
            consultant_names = get_problematic_sites(comm_url, dd)
        else:    
            host = comm_url
            htmlSoup = BeautifulSoup(urllib2.urlopen(host).read())
            for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('strong'):
                nextSib = header.next_sibling
                if nextSib is not None:
                    sibName = header.next_sibling.name
                    #print("Sibling name is: %s" % sibName)
                    while sibName == 'br':
                        nextSib = nextSib.next_sibling
                        sibName = nextSib.name
                    #print(nextSib)
                    for name in clean_strings(nextSib):
                        consultant_names.append(get_consultant_info(name, header, dd))
    #print(consultant_names)
    return consultant_names


def get_past_consultants(consultants, house, committee, dd):
    update_consultants = list()
    cid = get_committee_cid(house, committee, dd)
    select_dict = {'cid': str(cid)}
    try:
        dd.execute(SELECT_CURRENT_MEMBERS, select_dict)
        query = dd.fetchall()
        for consultant in query:
            isCurrent = False
            for commStaff in consultants:
                if consultant[0] == commStaff['pid']:
                    isCurrent = True
            if isCurrent is False:
                staff = dict()
                staff['current_flag'] = 0
                staff['end_date'] = dt.datetime.today().strftime("%Y-%m-%d")
                staff['pid'] = consultant[0]
                staff['cid'] = select_dict['cid']
                staff['state'] = STATE
                update_consultants.append(staff)
    except MySQLdb.Error:
        logger.warning("Select statement failed", full_msg = traceback.format_exc(),
                       additional_fields = create_payload('ConsultantServesOn', (SELECT_CURRENT_MEMBERS % select_dict)))

    return update_consultants


def get_committees(house, dd):
    host = 'http://%s.ca.gov/committees' % house.lower()
    htmlSoup = BeautifulSoup(urllib2.urlopen(host).read())
    
    for block in htmlSoup.find_all('div', 'block-views'):
        if 'Standing' in block.find('h2').string:
            for link in block.find(class_ = 'content').find_all('a'):
                print(link.get('href'))
                consultants = scrape_consultants(link.get('href'), house, dd)
                insert_consultants(consultants, house, link.string, dd)


def insert_consultants(consultants, house, committee, dd):
    global C_INSERT, C_UPDATE

    for consultant in consultants:
        print(consultant)
        cid = get_committee_cid(house, committee, dd)
        pid = get_consultant_pid(consultant, dd)
        consultant['cid'] = str(cid)
        consultant['pid'] = str(pid)
        if is_consultant_in_db(consultant, house, dd) is False:
            consultant['current_flag'] = '1'
            consultant['start_date'] = dt.datetime.today().strftime("%Y-%m-%d")
            try:
                dd.execute(INSERT_CONSULT_SERVESON, consultant)
                C_INSERT += dd.rowcount
            except MySQLdb.Error:
                logger.warning("Insert statement failed", full_msg = traceback.format_exc(),
                               additional_fields = create_payload("ConsultantServesOn", (INSERT_CONSULT_SERVESON % consultant)))

    #update_consultants = get_past_consultants(consultants, house, committee, dd)

    #if len(update_consultants) > 0:
    #   for consultant in update_consultants:
    #        try:
    #            dd.execute(UPDATE_CONSULTANTS, consultant)
    #            C_UPDATE += dd.rowcount
    #        except MySQLdb.Error:
    #            logger.warning("Update failed", full_msg = traceback.format_exc(),
    #                           additional_fields = create_payload("ConsultantServesOn", (UPDATE_CONSULTANTS % consultant)))


def main():
    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dd:
        get_committees('Senate', dd)

if __name__ == '__main__':
    with GrayLogger(API_URL) as _logger:
        logger = _logger
    main()
