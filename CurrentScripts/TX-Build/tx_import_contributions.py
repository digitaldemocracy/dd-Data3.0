#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: tx_import_contributions.py
Author: James Ly
Last Maintained: Andrew Rose
Last Updated: 05/19/2017
Description:
 - imports contributions for TX from followthemoney.org
Tables affected:
 - Organizations
 - Contribution
"""
import json
import MySQLdb
import requests
from bs4 import BeautifulSoup
from Utils.Generic_Utils import *
from Utils.Database_Connection import connect
from Constants.Contribution_Queries import *

logger = None

# global counters
I_O = 0  # org insert counter
I_C = 0  # contribution insert counter


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
    candidate_url = "https://www.followthemoney.org/entity-details?eid="
    candidate_url += eid

    page = requests.get(candidate_url)
    soup = BeautifulSoup(page.content, 'html.parser')
    for s in soup.find("title"):
        name = s

    name = name.split(' - ')[0]
    name = name.strip()
    name = name.split(',')

    first = None
    last = None

    if len(name) >= 2:
        first = ','.join(name[1:])
        last = name[0]

        if len(first.split(' ')) > 2:
            first = first.split(' ')
            if len(first[2]) == 1:
                first = first[1]
            elif len(first[1]) == 1:
                first = first[2]
            elif len(first) == 3:
                first = first[1] + " " + first[2]
            else:
                first = ' '.join(first)

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
        cursor.execute(S_PERSON, {'state': 'TX', 'first': first, 'last': last})
        if cursor.rowcount == 1:
            result = cursor.fetchone()[0]
        else:
            print("Person" + first + last + "not found")
    except MySQLdb.Error:
        logger.exception(format_logger_message('Select Failed for Person', (S_PERSON % (first, last))))

    return result


'''
Get house membership from pid and sessionyear
'''
def get_house(cursor, pid, session_year, state):
    result = None
    try:
        cursor.execute(S_TERM, (pid, session_year, state))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.exception(format_logger_message('Select Failed for Term',(S_TERM % (pid, session_year, state))))

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
        logger.exception(format_logger_message('Select Failed for Organization', (S_ORGANIZATION % (name,))))

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
        logger.exception(format_logger_message('Insert Failed for Organization', (I_ORGANIZATION % (name, state))))


'''
Get id from Contribution table
'''
def get_con_id(cursor, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid):
    result = None
    try:
        cursor.execute(S_CONTRIBUTION, (con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:

        logger.exception(format_logger_message('Select Failed for Contribution',
                       I_CONTRIBUTION % (con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid)))

    return result


'''
If the contribution is not in the DB, insert it
'''
def insert_contribution(cursor, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid):
    global I_C
    try:
        if get_con_id(cursor, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid) is None:
            cursor.execute(I_CONTRIBUTION, (con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid))
            I_C += cursor.rowcount
    except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for Contribution',
                                               I_CONTRIBUTION % (con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid)))


def main():
    with connect() as dddb:
        session_year = "2017"
        state = "TX"

        api_url = "http://api.followthemoney.org/?c-t-eid="
        api_key = "&gro=d-id&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05"
        mode = "&mode=json"
        eid_list = get_candidates("tx_candidates.txt")
        for eid in eid_list:
            first, last = get_name(eid)
            first, last = special_names(first, last)
            pid = get_pid(dddb, first, last)
            if pid is not None:
                house = get_house(dddb, pid, session_year, state)

                # if can find legislator in 2015 they seem to be in 2017 term
                if house is None:
                    house = get_house(dddb, pid, "2017", state)

                if house is not None:
                    records = get_records(api_url + eid + api_key + mode)

                    # get record data
                    for r in records:
                        date = r['Date']['Date']
                        type_contributor = r['Type_of_Contributor']['Type_of_Contributor']
                        contributor = r['Contributor']['Contributor']
                        amount = r['Amount']['Amount']

                        # some donations apparently dont have dates
                        if str(date) == '':
                            date = None
                            year = None
                        elif str(date) == '0000-00-00':
                            date = None
                            year = '2017'
                        else:
                            date = str(date) + " 00:00:00"
                            year = date.split("-")[0]

                        donor_name = None
                        donor_org = None
                        oid = None
                        if type_contributor == "Individual" or "FRIENDS" in contributor or type_contributor == 'Other':
                            # individual names have commas
                            if ',' in contributor:
                                temp_name = contributor.split(',')
                                contributor = temp_name[1] + " " + temp_name[0]
                                contributor = contributor.strip()
                            donor_name = contributor
                        elif type_contributor == "Non-Individual":
                            oid = get_oid(dddb, contributor)
                            if oid is None:
                                insert_org(dddb, contributor, state)
                                oid = get_oid(dddb, contributor)
                            donor_org = contributor
                            donor_name = contributor

                        con_id = hash((pid, date, donor_name, donor_org, amount, state))
                        con_id = str(con_id)
                        con_id = con_id[0:21]

                        insert_contribution(dddb, con_id, pid, year, date, house, donor_name, donor_org, amount, state,
                                            oid)

        print "Inserted" + str(I_O) + " rows into Organizations"
        print "Inserted" + str(I_C) + " rows into Contribution"

        LOG = {'tables': [{'state': 'TX', 'name': 'Organizations', 'inserted': I_O, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Contribution:', 'inserted': I_C, 'updated': 0, 'deleted': 0}]}
        logger.info(LOG)
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    logger = create_logger()
    main()