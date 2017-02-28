#!/usr/bin/env python
#  -*- coding: utf-8 -*-
'''
File: get_Committee_Consultants.py
Author: Andrew Rose
Date: 01/05/2017

Last Edited: 02/23/2017

Description:
- Scrapes the Assembly and Senate websites to gather committee consultants

Sources:
    - California Assembly Website
    - California Senate Website

Populates:
    - ConsultantServesOn (pid, session_year, cid, position, current_flag, start_date, end_date, state)
    - Person (pid, first, last)
    - PersonStateAffiliation (pid, state)
    - LegislativeStaff (pid, state)
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
P_INSERT = 0
PSA_INSERT = 0
L_INSERT = 0

STATE = 'CA'

ASSEMBLY_PROBLEM_SITES = ['aaar', 'abnk', 'acom', 'altc', 'apro', 'agri', 'abp', 'aedn', 'aesm', 'agov', 'ahea', 'ahed',
                          'ahum',
                          'ains', 'ajed', 'antr', 'privacycp', 'aper', 'apsf', 'arev', 'arul', 'atrn', 'autl', 'avet',
                          'awpw']

SENATE_PROBLEM_SITES = ['seuc', 'shum', 'sjud', 'spsf', 'srul', 'stran', 'svet', 'shea',
                        'apia', 'childrenspecialneeds', 'mobilehomes', 'smup', 'sros', 'womenandinequality']

SENATE_SELECT_STAFF = ['altc', 'apia', 'childrenspecialneeds', 'mobilehomes', 'smup', 'sros', 'womenandinequality']

# SQL Queries
SELECT_SESSION_YEAR = '''SELECT MAX(start_year)
                         FROM Session
                         WHERE state = 'CA'
                      '''

SELECT_COMMITTEE_CID = '''SELECT cid
                          FROM Committee
                          WHERE house = %(house)s
                          AND state = 'CA'
                          AND name = %(committee)s
                          AND session_year = %(year)s
                          AND current_flag = TRUE
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

INSERT_PERSON_STATE_AFF = '''INSERT
                             INTO PersonStateAffiliation
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

    con_name = str(con_name.encode('utf-8'))

    # Remove whitespace and colons from string
    con_name = con_name.replace('\xc2\xa0', ' ')
    con_name = con_name.strip(':').strip()

    # If consultant names are listed with commas, separate them
    if ',' in con_name and 'Jr.' not in con_name \
            and 'Ph.D' not in con_name:
        name_list = con_name.split(',')
        for name in name_list:
            strip_name = name.strip().strip('\xc2\xa0')

            # If the last name is separated with 'and'
            if ' and ' in name:
                and_names = name.split(' and ')
                for a_name in and_names:
                    strip_name = a_name.strip().strip('\xc2\xa0')
                    if len(strip_name) >= 1:
                        cleaned_strings.append(strip_name)
            else:
                strip_name = name.strip().strip('\xc2\xa0')
                if len(strip_name) >= 1:
                    cleaned_strings.append(strip_name)
    # If consultant names are separated with 'and', separate.
    elif ' and ' in con_name:
        name_list = con_name.split(' and ')
        for name in name_list:
            strip_name = name.strip().strip('\xc2\xa0')
            cleaned_strings.append(strip_name)
    else:
        if len(con_name) >= 1:
            cleaned_strings.append(con_name)
    return cleaned_strings


def get_committee_cid(house, committee, session_year, dd):
    query_dict = {'house': house, 'committee': committee, 'year': session_year}

    try:
        dd.execute(SELECT_COMMITTEE_CID, query_dict)

        cid = dd.fetchone()[0]
    except:
        logger.warning("Committee selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Committee', (SELECT_COMMITTEE_CID % query_dict)))
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
            insert_person_state_aff(legstaff_pid, dd)
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
        logger.warning("Consultant selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Person', (SELECT_CONSULT_PID % person_query_dict)))
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
        logger.warning("Person selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Person', (SELECT_PERSON % query_dict)))
        return None


def is_consultant_in_db(consultant, house, dd):
    query_dict = {'pid': consultant['pid'], 'cid': consultant['cid'], 'year': consultant['session_year']}

    try:
        dd.execute(SELECT_CONSULT_SERVESON, query_dict)
        query = dd.fetchone()
        if query is None:
            return False
    except MySQLdb.Error:
        logger.warning("Select statement failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('ConsultantServesOn', (SELECT_CONSULT_SERVESON % query_dict)))
    return True


def insert_person(consultant, dd):
    global P_INSERT
    insert_dict = {'first': consultant['first'], 'last': consultant['last']}

    try:
        dd.execute(INSERT_PERSON, insert_dict)
        P_INSERT += dd.rowcount
    except MySQLdb.Error:
        logger.warning("Insert statement failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Person', (INSERT_PERSON % insert_dict)))


def insert_person_state_aff(legstaff_pid, dd):
    global PSA_INSERT
    insert_dict = {'pid': legstaff_pid, 'state': STATE}

    try:
        dd.execute(INSERT_PERSON_STATE_AFF, insert_dict)
        PSA_INSERT += dd.rowcount
    except MySQLdb.Error:
        logger.warning("Insert statement failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('PersonStateAffiliation',
                                                        (INSERT_PERSON_STATE_AFF % insert_dict)))


def insert_legstaff(legstaff_pid, dd):
    global L_INSERT
    insert_dict = {'pid': legstaff_pid, 'state': STATE}

    try:
        dd.execute(INSERT_LEGSTAFF, insert_dict)
        L_INSERT += dd.rowcount
    except MySQLdb.Error:
        logger.warning("Insert statement failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('LegislativeStaff', (INSERT_LEGSTAFF % insert_dict)))


def get_consultant_info(name, header, dd):
    consultant = dict()

    names = name.strip().split(' ')

    if len(names) > 2:
        if 'Jr' in names[-1] or 'Sr' in names[-1] \
                or 'Ph.D' in names[-1]:
            consultant['first'] = ' '.join(names[:-2])
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

    if header is None:
        consultant['position'] = 'Consultant'
    elif header.strip() == '':
        consultant['position'] = 'Consultant'
    else:
        consultant['position'] = header.strip(',').strip().strip(':')
        if consultant['position'][-1] == 's':
            consultant['position'] = consultant['position'][:-1]

    return consultant


def get_session_year(dd):
    dd.execute(SELECT_SESSION_YEAR)

    try:
        year = dd.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Select statement failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Session", SELECT_SESSION_YEAR))
        return None

    return year


def get_assembly_problem_sites(comm_url, dd):
    consultant_names = list()
    commName = comm_url.split('.')[0][7:]

    if commName == 'awpw':
        host = comm_url + '/contactthecommittee'
    elif commName == 'ahea':
        host = comm_url + '/committeestaff1'
    else:
        host = comm_url + '/committeestaff'

    print('Problem site identified: ' + comm_url)
    try:
        htmlSoup = BeautifulSoup(urllib2.urlopen(host).read(), 'lxml')
    except urllib2.HTTPError:
        logger.warning("HTTP Error connecting to %s" % comm_url, full_msg=traceback.format_exc())
        return consultant_names

    if commName in ['aesm', 'aedn', 'agri']:
        for header in htmlSoup.find('div', 'content').find_all('strong'):
            nextSib = header.next_sibling
            for name in clean_strings(nextSib):
                consultant_names.append(get_consultant_info(name, header.contents[0], dd))
    elif commName in ['altc', 'ajed', 'avet', 'acom']:
        for header in htmlSoup.find('div', 'content').find_all('strong'):
            nextSib = header.next_sibling
            for name in clean_strings(header.contents[0]):
                consultant_names.append(get_consultant_info(name, nextSib, dd))
    elif commName in ['ahed', 'antr', 'arul', 'atrn']:
        for header in htmlSoup.find('div', 'content').find_all('li'):
            if commName == 'ahed':
                consultant = header.contents[0].split('-')
            else:
                consultant = header.contents[0].split(',')
            for name in clean_strings('-'.join(consultant[:-1])):
                consultant_names.append(get_consultant_info(name, consultant[-1], dd))
    elif commName == 'aaar':
        header = htmlSoup.find('div', 'content')
        for link in header.find_all('a'):
            for name in clean_strings(link.previous_sibling):
                consultant_names.append(get_consultant_info(name, '', dd))
    elif commName == 'abnk':
        for header in htmlSoup.find('div', 'content').find_all('p'):
            consultant = header.contents[0].split(':')
            if len(consultant) > 1:
                for name in clean_strings(consultant[-1]):
                    consultant_names.append(get_consultant_info(name, consultant[0], dd))
    elif commName == 'apro':
        header = htmlSoup.find('div', 'field-item even')
        for title_header in header.children:
            print(title_header)
            if title_header.name == 'h2' and 'Contact Information' in title_header.contents[0]:
                break
            elif title_header.name == 'p':
                for line in title_header.contents:
                    if line.name is None:
                        consultant = line.split('(')
                        consultant = consultant[0].split(',')
                        if len(consultant) > 1:
                            for name in clean_strings(consultant[0]):
                                consultant_names.append(get_consultant_info(name, consultant[1], dd))
    elif commName == 'abgt':
        for header in htmlSoup.find('div', 'content').find_all('p'):
            for child in header.contents:
                if child.name is None:
                    title_header = ''
                    nextSib = child.next_sibling
                    while nextSib is not None:
                        if nextSib.name is None:
                            title_header = nextSib
                        nextSib = nextSib.next_sibling
                    for name in clean_strings(child):
                        consultant_names.append(get_consultant_info(name, title_header, dd))

    elif commName == 'abp':
        for header in htmlSoup.find('div', 'content').find_all('ul'):
            title_header = header.find_previous_sibling('h3')
            if title_header is None:
                title_header = header.find_previous_sibling('h2')
            if title_header.name == 'h2':
                for title in header.find_all('p'):
                    for name in clean_strings(title.contents[0].split('(')[0]):
                        consultant_names.append(get_consultant_info(name, title_header.contents[0], dd))
            elif title_header.name == 'h3':
                for title in header.find_all('li'):
                    for name in clean_strings(title.contents[0].split('(')[0]):
                        consultant_names.append(get_consultant_info(name, title_header.contents[0], dd))
    elif commName == 'agov':
        for header in htmlSoup.find('div', 'content').find_all('p'):
            consultant = header.contents[0].split(' ')
            for name in clean_strings(' '.join(consultant[3:])):
                consultant_names.append(get_consultant_info(name, ' '.join(consultant[:2]), dd))
    elif commName == 'ahea':
        header = htmlSoup.find('div', 'content').find('h4')
        for title_header in header.find_all('strong'):
            for name in clean_strings(title_header.contents[0]):
                consultant_names.append(get_consultant_info(name, title_header.contents[1].contents[0], dd))
        for paragraph_header in header.find_next_siblings('p')[:2]:
            for title_header in paragraph_header.find_all('strong'):
                for name in clean_strings(title_header.contents[0]):
                    consultant_names.append(get_consultant_info(name, title_header.contents[1].contents[0], dd))
    elif commName == 'ahum':
        for header in htmlSoup.find('div', 'content').find_all('td'):
            consultant = header.find_all('p')
            if len(consultant) > 0:
                for name in clean_strings(consultant[0].contents[0]):
                    consultant_names.append(get_consultant_info(name, consultant[1].contents[0], dd))
    elif commName == 'ains':
        for header in htmlSoup.find('div', 'content').find_all('strong'):
            for name in clean_strings(header.contents[0]):
                consultant_names.append(get_consultant_info(name, header.contents[1].contents[0], dd))
    elif commName == 'privacycp':
        for header in htmlSoup.find('div', 'content').find('p').find_all('strong'):
            nextSib = header.next_sibling
            for name in clean_strings(nextSib):
                consultant_names.append(get_consultant_info(name, header.contents[0], dd))
    elif commName == 'aper':
        for header in htmlSoup.find('div', 'content').find_all('p'):
            print(header.contents)
            for child in header.contents:
                print(child.name)
                if child.name is None:
                    if len(child.strip()) < 1:
                        continue
                    title_header = child.next_sibling.next_sibling
                    for name in clean_strings(child):
                        consultant_names.append(get_consultant_info(name, title_header, dd))
                    break
    elif commName == 'apsf':
        for header in htmlSoup.find('div', 'content').find_all('h3'):
            for title_header in header.find_next_sibling('ul').children:
                if title_header.name == 'li':
                    for name in clean_strings(title_header.contents[0]):
                        consultant_names.append(get_consultant_info(name, header.contents[0], dd))
    elif commName == 'arev':
        for header in htmlSoup.find('div', 'content').find_all('h3'):
            consultant = header.contents[0].split('-')
            for name in clean_strings('-'.join(consultant[:-1])):
                consultant_names.append(get_consultant_info(name, consultant[-1], dd))
    elif commName == 'autl':
        for header in htmlSoup.find('div', 'content').find_all('p')[1:-1]:
            print(header.contents)
            for child in header.contents:
                print(child.name)
                if child.name is None:
                    title_header = child.next_sibling.next_sibling
                    for name in clean_strings(child):
                        consultant_names.append(get_consultant_info(name, title_header, dd))
                    break
    elif commName == 'awpw':
        content_div = htmlSoup.find('div', 'content')
        header = content_div.find('p')
        for child in header.children:
            if child.name != 'br':
                if child.name is None:
                    consultant = child.split(',')
                    for name in clean_strings(consultant[1]):
                        consultant_names.append(get_consultant_info(name, consultant[0], dd))

    return consultant_names


def scrape_budget_committees(comm_url, house, dd):
    comm_url += '/committeestaff'

    htmlSoup = BeautifulSoup(urllib2.urlopen(comm_url).read(), 'lxml')

    header = htmlSoup.find('div', class_='field-item even')

    committee = 'Assembly Standing Committee on Budget'
    consultant_names = list()

    for tag in header.contents:
        if tag.name == 'h3':
            if tag.contents[0].name is None:
                insert_consultants(consultant_names, house, committee, dd)
                committee = 'Assembly Budget ' + tag.contents[0].replace('#', 'No. ', 1)
                consultant_names = list()
        elif tag.name == 'p':
            for child in tag.contents:
                if child.name is None:
                    title_header = ''
                    nextSib = child.next_sibling
                    while nextSib is not None:
                        if nextSib.name is None:
                            title_header = nextSib
                        nextSib = nextSib.next_sibling
                    for name in clean_strings(child):
                        consultant_names.append(get_consultant_info(name, title_header, dd))
                    break

    insert_consultants(consultant_names, house, committee, dd)


def get_assembly_special_comms(comm_url, dd):
    consultant_names = list()
    commName = comm_url.split('.')[0][7:]

    if 'assembly' in commName:
        try:
            htmlSoup = BeautifulSoup(urllib2.urlopen(comm_url).read(), 'lxml')
        except urllib2.HTTPError:
            logger.warning("HTTP Error connecting to %s" % comm_url, full_msg=traceback.format_exc())
            return consultant_names

        commName = comm_url.split('/')[-1]

        if commName == 'specialcmtelegethics':
            header = htmlSoup.find('div', 'content').find_all('p')[-1]
            print(header)
            for name in clean_strings(header.contents[0]):
                consultant_names.append(get_consultant_info(name, 'Counsel', dd))
        elif commName == 'Specialcmteattygen':
            for header in htmlSoup.find('div', 'content').find_all('h2'):
                if header.contents[0] == 'Committee Staff':
                    for line in header.find_next_sibling('p'):
                        if line.name is None:
                            for name in clean_strings(line):
                                consultant_names.append(get_consultant_info(name,
                                                                            line.find_next_sibling('em').contents[0],
                                                                            dd))
    # else:
    #    host = comm_url + '/committeestaff'
    #    try:
    #        htmlSoup = BeautifulSoup(urllib2.urlopen(host).read(), 'lxml')
    #    except urllib2.HTTPError:
    #        logger.warning("HTTP Error connecting to %s" % host, full_msg=traceback.format_exc())
    #        return consultant_names

    #    if commName == 'legaudit':
    #        for header in htmlSoup.find('div', 'content').find_all('p')[:-2]:
    #            for name in clean_strings(header.contents[0]):
    #                consultant_names.append(get_consultant_info(name, header.contents[2], dd))
    #    elif commName == 'jtemergencymanagement':
    #        for header in htmlSoup.find('div', 'content').find_all('p'):
    #            consultant = header.contents[0].split('-')
    #            for name in clean_strings(consultant[0]):
    #                consultant_names.append(get_consultant_info(name, consultant[1], dd))

    return consultant_names


def get_problematic_sites(comm_url, dd):
    consultant_names = list()
    commName = comm_url.split('.')[0][7:]

    try:
        htmlSoup = BeautifulSoup(urllib2.urlopen(comm_url).read(), 'lxml')
    except urllib2.HTTPError:
        logger.warning("HTTP Error connecting to %s" % comm_url, full_msg=traceback.format_exc())
        return consultant_names

    print("Problem site identified: " + commName)
    # Select Committee Sites
    if commName == 'apia':
        header = htmlSoup.find('div', 'sidebar-information').find('p').find_all('strong')[1]
        title_header = header.contents[4]
        for name in clean_strings(header.contents[6]):
            consultant_names.append(get_consultant_info(name, title_header, dd))
    elif commName == 'childrenspecialneeds':
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('p'):
            hcontents = header.contents[0]
            if hcontents.name is None:
                split_contents = hcontents.split(',')
                for name in clean_strings(split_contents[0]):
                    consultant_names.append(get_consultant_info(name, ''.join(split_contents[1:]), dd))
    elif commName == 'mobilehomes':
        header = htmlSoup.find('div', 'sidebar-information').find('p')
        title_header = header.find_all('strong')[-1].contents[0]
        for name in clean_strings(header.find('a').contents[0]):
            consultant_names.append(get_consultant_info(name, title_header, dd))
    elif commName == 'smup':
        header = htmlSoup.find_all('div', 'sidebar-information')[1]
        for name in clean_strings(header.find('li').contents[0]):
            consultant_names.append(get_consultant_info(name, '', dd))
    elif commName == 'sros':
        header = htmlSoup.find_all('div', 'sidebar-information')[1].find('p')
        title_header = header.contents[2]
        for name in clean_strings(header.find('strong').contents[0].split(',')[0]):
            consultant_names.append(get_consultant_info(name, title_header, dd))
    elif commName == 'womenandinequality':
        header = htmlSoup.find_all('div', 'sidebar-information')[1].find('p')
        for name in clean_strings(header.contents[0]):
            consultant_names.append(get_consultant_info(name, '', dd))
    # Standing Committee Sites
    elif commName == 'shum':
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('ul'):
            # print(header.contents[1].string)
            title_header = header.find_previous_sibling('p').find('strong')
            for name in clean_strings(header.contents[1].string):
                consultant_names.append(get_consultant_info(name, title_header.contents[0], dd))
    elif commName == 'spsf':
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('p'):
            hcontents = header.contents[0]
            if hcontents.name is not None:
                title_header = hcontents.find('em')

                if title_header is None:
                    title_header = hcontents.find('strong')
            elif hcontents.name is None:
                for name in clean_strings(hcontents):
                    consultant_names.append(get_consultant_info(name, title_header.contents[0], dd))
    elif commName == 'srul':
        header = htmlSoup.find_all('div', 'sidebar-information')[1].find('strong')
        # print(header.next_sibling)
        # print(header.find_next_sibling('a').string)
        for name in clean_strings(header.next_sibling):
            consultant_names.append(get_consultant_info(name, header.contents[0], dd))
        for name in clean_strings(header.find_next_sibling('a').string):
            consultant_names.append(get_consultant_info(name, header.contents[0], dd))
    else:
        for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('strong'):
            if commName == 'seuc':
                # print(header.find_next_sibling('a').string)
                for name in clean_strings(header.find_next_sibling('a').string):
                    consultant_names.append(get_consultant_info(name, header.contents[0], dd))
            if commName == 'sjud':
                nextSib = header.next_sibling
                if nextSib is not None:
                    sibName = header.next_sibling.name
                    if sibName != 'br':
                        # print(nextSib)
                        for name in clean_strings(nextSib):
                            consultant_names.append(get_consultant_info(name, header.contents[0], dd))
            if commName == 'stran' or commName == 'svet':
                # print(header.find_next_sibling('a').string)
                for name in clean_strings(header.find_next_sibling('a').string):
                    consultant_names.append(get_consultant_info(name, header.contents[0], dd))
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
                            consultant_names.append(get_consultant_info(name, header.contents[0], dd))
                        nextSib = nextSib.next_sibling
    return consultant_names


def scrape_joint_committees(comm_url, dd):
    consultant_names = list()

    comm_name = comm_url.split('.')[0][7:]
    try:
        htmlSoup = BeautifulSoup(urllib2.urlopen(comm_url).read(), 'lxml')
    except urllib2.HTTPError:
        logger.warning("HTTP Error connecting to %s" % comm_url, full_msg=traceback.format_exc())
        return consultant_names

    if 'senate' in comm_name:
        comm_name = comm_url.split('/')[-1]

        if comm_name == 'legislativebudget' or comm_name == 'fairsallocation':
            content_div = htmlSoup.find('div', 'content')
            title_header = content_div.find_all('p')[-1].find('strong')
            next_sib = title_header.next_sibling
            for name in clean_strings(next_sib):
                consultant_names.append(get_consultant_info(name, title_header.contents[0], dd))
        elif comm_name == 'jointrules':
            content_div = htmlSoup.find('div', 'content').find_all('p')[-1]
            title_header = content_div.find('strong')
            for name in clean_strings(title_header.next_sibling):
                consultant_names.append(get_consultant_info(name, title_header.contents[0], dd))
        elif comm_name == 'legislativeaudit':
            content_div = htmlSoup.find('div', 'content').find_all('p')[1]
            for header in content_div.find_all('strong')[:2]:
                next_sib = header.next_sibling
                for name in clean_strings(next_sib):
                    consultant_names.append(get_consultant_info(name, header.contents[0], dd))
    elif 'fisheries' in comm_name:
        content_div = htmlSoup.find_all('div', 'sidebar-information')[1]
        for header in content_div.find_all('strong'):
            consultant_info = header.find_next_sibling('a').contents[0]
            consultant_info = consultant_info.split(', ')
            for name in clean_strings(consultant_info[0]):
                consultant_names.append(get_consultant_info(name, consultant_info[1], dd))

    return consultant_names


def scrape_consultants(comm_url, house, dd):
    global SENATE_PROBLEM_SITES
    consultant_names = list()

    if house.lower() == 'assembly':
        commName = comm_url.split('.')[0][7:]
        if commName in ASSEMBLY_PROBLEM_SITES:
            consultant_names = get_assembly_problem_sites(comm_url, dd)
        else:
            host = comm_url + '/committeestaff'
            try:
                htmlSoup = BeautifulSoup(urllib2.urlopen(host).read(), 'lxml')
            except urllib2.HTTPError:
                logger.warning("HTTP Error connecting to %s" % comm_url, full_msg=traceback.format_exc())
                print("HTTP Error")
                return consultant_names
            content_div = htmlSoup.find('div', 'content')
            header = content_div.find('p')
            for child in header.children:
                if child.name != 'br':
                    if child.name is None:
                        consultant = child.split(',')
                        for name in clean_strings(consultant[0]):
                            consultant_names.append(get_consultant_info(name, consultant[1], dd))

    elif house.lower() == 'senate':
        commName = comm_url.split('.')[0][7:]
        if commName in SENATE_PROBLEM_SITES:
            consultant_names = get_problematic_sites(comm_url, dd)
        else:
            host = comm_url
            try:
                htmlSoup = BeautifulSoup(urllib2.urlopen(host).read(), 'lxml')
            except urllib2.HTTPError:
                logger.warning("HTTP Error connecting to %s" % comm_url, full_msg=traceback.format_exc())
                print("HTTP Error")
                return consultant_names
            for header in htmlSoup.find_all('div', 'sidebar-information')[1].find_all('strong'):
                nextSib = header.next_sibling
                if nextSib is not None:
                    sibName = header.next_sibling.name
                    # print("Sibling name is: %s" % sibName)
                    while sibName == 'br':
                        nextSib = nextSib.next_sibling
                        sibName = nextSib.name
                    # print(nextSib)
                    for name in clean_strings(nextSib):
                        consultant_names.append(get_consultant_info(name, header.contents[0], dd))
    # print(consultant_names)
    return consultant_names


def get_past_consultants(consultants, house, committee, session_year, dd):
    update_consultants = list()
    cid = get_committee_cid(house, committee, session_year, dd)
    select_dict = {'cid': str(cid)}
    try:
        dd.execute(SELECT_CURRENT_MEMBERS, select_dict)
        query = dd.fetchall()
        for consultant in query:
            isCurrent = False
            for commStaff in consultants:
                if str(consultant[0]) == commStaff['pid']:
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
        logger.warning("Select statement failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('ConsultantServesOn', (SELECT_CURRENT_MEMBERS % select_dict)))

    return update_consultants


def get_committees(house, dd):
    host = 'http://%s.ca.gov/committees' % house.lower()
    htmlSoup = BeautifulSoup(urllib2.urlopen(host).read(), 'lxml')

    if house.lower() == 'senate':
        for block in htmlSoup.find_all('div', 'block-views'):
            if 'Standing' in block.find('h2').string:
                for link in block.find(class_='content').find_all('a'):
                    print(link.get('href'))
                    consultants = scrape_consultants(link.get('href'), house, dd)
                    committee = house + ' Standing Committee on ' + link.string
                    insert_consultants(consultants, house, committee, dd)
            elif 'Select' in block.find('h2').string:
                for link in block.find(class_='content').find_all('a'):
                    comm_name = link.get('href')
                    print(comm_name)
                    if comm_name.split('.')[0][7:] in SENATE_SELECT_STAFF:
                        consultants = scrape_consultants(link.get('href'), house, dd)
                        committee = house + ' Select Committee on ' + link.string
                        insert_consultants(consultants, house, committee, dd)
            elif 'Joint' in block.find('h2').string:
                for link in block.find(class_='content').find_all('a'):
                    comm_name = link.get('href').split('.')[0][7:]
                    print(link.get('href'))
                    committee = link.string
                    if 'Committee' not in committee:
                        committee += " Committee"
                    if comm_name == 'arts' or comm_name == 'emergencymanagement':
                        consultants = scrape_consultants(link.get('href'), house, dd)
                        insert_consultants(consultants, 'Joint', committee, dd)
                    elif committee != 'Joint Legislative Budget':
                        consultants = scrape_joint_committees(link.get('href'), dd)
                        insert_consultants(consultants, 'Joint', committee, dd)
            elif 'Sub' in block.find('h2').string:
                section = block.find(class_='content').find('h3')
                comm_type = section.contents[0]
                for tag in section.next_siblings:
                    if tag.name == 'h3':
                        comm_type = tag.contents[0]
                    elif tag.name == 'div' and tag.find('a') is not None:
                        link = tag.find('a')
                        consultants = scrape_consultants(link.get('href'), house, dd)
                        committee = house + ' ' + comm_type + ' ' + link.string
                        insert_consultants(consultants, house, committee, dd)
            else:
                for link in block.find(class_='content').find_all('a'):
                    print(link.get('href'))
                    consultants = scrape_consultants(link.get('href'), house, dd)
                    committee = house + ' Committee On ' + link.string
                    insert_consultants(consultants, house, committee, dd)

    elif house.lower() == 'assembly':
        for block in htmlSoup.find_all('div', 'block-views'):
            if 'Standing' in block.find('h2').string:
                for link in block.find(class_='content').find_all('a'):
                    print(link.get('href'))
                    comm_name = link.get('href')
                    if comm_name.split('.')[0][7:] == 'abgt':
                        scrape_budget_committees(link.get('href'), house, dd)
                    else:
                        consultants = scrape_consultants(link.get('href'), house, dd)
                        committee = house + ' Standing Committee on ' + link.string
                        insert_consultants(consultants, house, committee, dd)
            if 'Joint' in block.find('h2').string:
                for link in block.find(class_='content').find_all('a'):
                    print(link.get('href'))
                    consultants = get_assembly_special_comms(link.get('href'), dd)
                    committee = link.string
                    if 'Audit' in committee:
                        committee += ' Committee'
                    insert_consultants(consultants, 'Joint', committee, dd)
            if 'Special' in block.find('h2').string:
                for link in block.find(class_='content').find_all('a'):
                    print(link.get('href'))
                    consultants = get_assembly_special_comms(link.get('href'), dd)
                    if 'Ethics' in link.string:
                        committee = 'Assembly Special Committee on ' + link.string
                    else:
                        committee = 'Assembly ' + link.string
                    insert_consultants(consultants, house, committee, dd)


def insert_consultants(consultants, house, committee, dd):
    global C_INSERT, C_UPDATE

    if consultants is not None and len(consultants) > 0:
        session_year = get_session_year(dd)

        for consultant in consultants:
            cid = get_committee_cid(house, committee, session_year, dd)
            pid = get_consultant_pid(consultant, dd)
            consultant['cid'] = str(cid)
            consultant['pid'] = str(pid)
            consultant['session_year'] = session_year
            if is_consultant_in_db(consultant, house, dd) is False:
                consultant['current_flag'] = '1'
                consultant['start_date'] = dt.datetime.today().strftime("%Y-%m-%d")
                try:
                    dd.execute(INSERT_CONSULT_SERVESON, consultant)
                    C_INSERT += dd.rowcount
                except MySQLdb.Error:
                    logger.warning("Insert statement failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("ConsultantServesOn",
                                                                    (INSERT_CONSULT_SERVESON % consultant)))

        update_consultants = get_past_consultants(consultants, house, committee, session_year, dd)

        if len(update_consultants) > 0:
            for consultant in update_consultants:
                try:
                    dd.execute(UPDATE_CONSULTANTS, consultant)
                    C_UPDATE += dd.rowcount
                except MySQLdb.Error:
                    logger.warning("Update failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("ConsultantServesOn",
                                                                    (UPDATE_CONSULTANTS % consultant)))


def main():
    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dd:
        for house in ['Assembly', 'Senate']:
            get_committees(house, dd)
        logger.info(__file__ + " terminated successfully.",
                    full_msg='Inserted ' + str(P_INSERT) + ' rows in Person, '
                             + str(PSA_INSERT) + ' rows in PersonStateAffiliation, '
                             + str(L_INSERT) + ' rows in LegislativeStaff, and inserted '
                             + str(C_INSERT) + ' rows and updated '
                             + str(C_UPDATE) + ' rows in ConsultantServesOn',
                    additional_fields={'_affected_rows': 'ConsultantServesOn: ' + str(C_INSERT + C_UPDATE)
                                                         + ', Person: ' + str(P_INSERT)
                                                         + ', PersonStateAffiliation: ' + str(PSA_INSERT)
                                                         + ', LegislativeStaff: ' + str(L_INSERT),
                                       '_inserted': 'ConsultantServesOn: ' + str(C_INSERT)
                                                    + ', Person: ' + str(P_INSERT)
                                                    + ', PersonStateAffiliation: ' + str(PSA_INSERT)
                                                    + ', LegislativeStaff: ' + str(L_INSERT),
                                       '_updated': 'ConsultantServesOn: ' + str(C_UPDATE),
                                       '_state': 'CA'})


if __name__ == '__main__':
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()
