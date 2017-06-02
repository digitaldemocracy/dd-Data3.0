#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: fl_import_contributions.py
Author: James Ly
Last Maintained: Andrew Rose
Last Updated: 05/19/2017

Description:
 - imports contributions for NY from followthemoney.org

Tables affected:
 - Organizations
 - Contribution
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

# global counters
I_O = 0  # org insert counter
I_C = 0  # contribution insert counter

S_PERSON = '''SELECT p.pid
              FROM Person p, Legislator l
              WHERE p.pid = l.pid
              AND state = 'FL'
              AND first LIKE %s
              AND last LIKE %s'''

S_TERM = '''SELECT house
            FROM Term
            WHERE pid = %s
            AND year = %s
            AND state = %s'''

S_CONTRIBUTION = '''SELECT id
                    FROM Contribution
                    WHERE id = %s
                    AND pid = %s
                    AND year = %s
                    AND date = %s
                    AND house = %s
                    AND donorName = %s
                    AND (donorOrg = %s or donorOrg is Null)
                    AND amount = %s
                    AND state = %s
                    AND (oid = %s or oid is Null)'''

S_ORGANIZATION = '''SELECT oid
                    FROM Organizations
                    WHERE name = %s'''

I_ORGANIZATION = '''INSERT INTO Organizations
                    (name, stateHeadquartered)
                    VALUES (%s, %s)'''

I_CONTRIBUTION = '''INSERT INTO Contribution
                    (id, pid, year, date, house, donorName, donorOrg, amount, state, oid)
                    VALUES
                    (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'FL'
    }


'''
Parse eid from text file

The text file contains URLs to the FollowTheMoney page for each winner
of a state legislative election
'''
def get_candidates(filename):
    s = set()
    f = open(filename)
    for line in f.readlines():
        data = line.split("&default=candidate")[0]
        ndx = data.find("eid=")
        s.add(data[ndx + 4:])
    return s


'''
Given FollowTheMoney's eid, scrape the candidate's name from the website
'''
def get_name(eid):
    candidateUrl = "https://www.followthemoney.org/entity-details?eid="
    candidateUrl += eid

    page = requests.get(candidateUrl)
    soup = BeautifulSoup(page.content, 'html.parser')
    for s in soup.find("title"):
        name = s

    name = name.split('-')[0]
    name = name.strip()
    name = name.split(',')

    first = None
    last = None

    if len(name) == 2:
        first = name[1]
        last = name[0]

        if len(first.split(' ')) > 2:
            first = first.split(' ')
            if len(first[2]) == 1:
                first = first[1]
            elif len(first[1]) == 1:
                first = first[2]
            elif len(first) == 3:
                first = first[1] + " " + first[2]

        if len(last.split(' ')) > 1:
            last = last.split(' ')
            if len(last[0]) == 1:
                last = last[1]
            else:
                last = last[0]

    first = first.strip()
    last = last.strip()

    return first, last


'''
A few people have names that still don't work with
the pattern matching being done in the get_pid query.
This function fixes that.
'''
def special_names(first, last):
    if last == 'PIGMAN':
        first = 'CARY'
    if last == 'CUMMINGS':
        first = 'W.'
    if last == 'RABURN':
        first = 'JAKE'
    if last == 'FANT':
        first = 'JAY'

    return first, last


'''
Get pid from db
'''
def get_pid(cursor, first, last):
    result = None
    first = "%" + first[:3] + "%"
    last = "%" + last + "%"
    try:
        cursor.execute(S_PERSON, (first, last))
        if cursor.rowcount == 1:
            result = cursor.fetchone()[0]
        else:
            print("Person" + first + last + "not found")
    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Legislator', (S_PERSON % (first, last))))

    return result


'''
Get house membership from pid and sessionyear
'''
def get_house(cursor, pid, sessionYear, state):
    result = None
    try:
        cursor.execute(S_TERM, (pid, sessionYear, state))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Term', (S_TERM % (pid, sessionYear, state))))

    return result


'''
Get contribution records from api
'''
def get_records(url):
    page = requests.get(url)
    result = page.json()
    return result['records']


'''
Get oid from db
'''
def get_oid(cursor, name):
    result = None
    try:
        cursor.execute(S_ORGANIZATION, (name,))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Organizations', (S_ORGANIZATION % (name,))))

    return result


'''
If the contributing organization is not in the DB, insert it
'''
def insert_org(cursor, name, state):
    global I_O
    try:
        cursor.execute(I_ORGANIZATION, (name, state))
        I_O += cursor.rowcount

    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Organzations', (I_ORGANIZATION % (name, state))))


'''
Get id from Contribution table
'''
def get_conID(cursor, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid):
    result = None
    try:
        cursor.execute(S_CONTRIBUTION, (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Contribution', (
                       I_CONTRIBUTION % (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))))

    return result


'''
If the contribution is not in the DB, insert it
'''
def insert_contribution(cursor, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid):
    global I_C
    try:
        if get_conID(cursor, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid) is None:
            cursor.execute(I_CONTRIBUTION, (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))
            I_C += cursor.rowcount
    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Contribution', (
                       I_CONTRIBUTION % (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))))


def main():
    ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=ddinfo['host'],
                         user=ddinfo['user'],
                         db=ddinfo['db'],
                         port=ddinfo['port'],
                         passwd=ddinfo['passwd'],
                         charset='utf8') as dddb:

        year = "2016"
        sessionYear = "2017"
        state = "FL"

        apiUrl = "http://api.followthemoney.org/?c-t-eid="
        apiKey = "&gro=d-id&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05"
        mode = "&mode=json"
        eidList = get_candidates("fl_candidates.txt")
        for eid in eidList:
            first, last = get_name(eid)
            first, last = special_names(first, last)
            pid = get_pid(dddb, first, last)
            if pid is not None:
                house = get_house(dddb, pid, sessionYear, state)

                # if can find legislator in 2015 they seem to be in 2017 term
                if house is None:
                    house = get_house(dddb, pid, "2017", state)

                if house is not None:
                    records = get_records(apiUrl + eid + apiKey + mode)

                    # get record data
                    for r in records:
                        date = r['Date']['Date']
                        typeContributor = r['Type_of_Contributor']['Type_of_Contributor']
                        contributor = r['Contributor']['Contributor']
                        amount = r['Amount']['Amount']

                        # some donations apparently dont have dates
                        if str(date) == '':
                            date = None
                            year = None
                        else:
                            date = str(date) + " 00:00:00"
                            year = date.split("-")[0]

                        donorName = None
                        donorOrg = None
                        oid = None
                        if typeContributor == "Individual" or "FRIENDS" in contributor or typeContributor == 'Other':
                            # individual names have commas
                            if ',' in contributor:
                                tempName = contributor.split(',')
                                contributor = tempName[1] + " " + tempName[0]
                                contributor = contributor.strip()
                            donorName = contributor
                        elif typeContributor == "Non-Individual":
                            oid = get_oid(dddb, contributor)
                            if oid is None:
                                insert_org(dddb, contributor, state)
                                oid = get_oid(dddb, contributor)
                            donorOrg = contributor
                            donorName = contributor

                        conID = hash((pid, date, donorName, donorOrg, amount, state))
                        conID = str(conID)
                        conID = conID[0:21]

                        insert_contribution(dddb, conID, pid, year, date, house, donorName, donorOrg, amount, state,
                                            oid)

        print "Inserted" + str(I_O) + " rows into Organizations"
        print "Inserted" + str(I_C) + " rows into Contribution"

        logger.info(__file__ + ' terminated successfully.',
                    full_msg='Inserted ' + str(I_O) + ' rows in Organizations and inserted '
                             + str(I_C) + ' rows in Contribution',
                    additional_fields={'_affected_rows': 'Organizations:' + str(I_O) +
                                                         ', Contribution:' + str(I_C),
                                       '_inserted': 'Organizations:' + str(I_O) +
                                                    ', Contribution:' + str(I_C),
                                       '_state': 'FL'})

        LOG = {'tables': [{'state': 'FL', 'name': 'Organizations', 'inserted': I_O, 'updated': 0, 'deleted': 0},
                          {'state': 'FL', 'name': 'Contribution:', 'inserted': I_C, 'updated': 0, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    with GrayLogger(GRAY_URL) as _logger:
        logger = _logger
        main()
