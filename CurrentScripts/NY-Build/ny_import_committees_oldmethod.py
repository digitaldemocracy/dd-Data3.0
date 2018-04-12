#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: ny_import_committees.py
Author: John Alkire
Date: 11/26/2015
Description:
- Imports NY committees using senate API
- Fills Committee and servesOn
"""

import json
import sys
import traceback
import requests
import MySQLdb
import re
from datetime import datetime
from Utils.Generic_Utils import *
from Utils.Database_Connection import connect

logger = None
C_INSERTED = 0
S_INSERTED = 0
C_UPDATED = 0
S_UPDATED = 0
CN_I = 0

select_committee_last = '''SELECT cid FROM Committee
                           ORDER BY cid DESC
                           LIMIT 1'''

select_committee = '''SELECT cid 
                      FROM Committee
                      WHERE house = %(house)s 
                       AND name = %(name)s 
                       AND state = %(state)s
                       AND session_year = %(year)s'''

select_person = '''SELECT p.pid 
                   FROM Person p, Legislator l
                   WHERE first = %(first)s 
                    AND last = %(last)s 
                    AND state = %(state)s
                    AND p.pid = l.pid'''

select_serveson = '''SELECT pid 
                     FROM servesOn
                     WHERE pid = %(pid)s 
                      AND year = %(year)s 
                      AND house = %(house)s 
                      AND cid = %(cid)s 
                      AND state = %(state)s'''
QS_SERVESON_MEMBERS = '''SELECT pid
                         FROM servesOn
                         WHERE year = %s
                          AND cid = %s
                          AND state = %s
                          AND end_date IS NULL'''
QS_COMMITTEENAME_CHECK = '''
SELECT *
FROM CommitteeNames
WHERE name = %s
 AND house = 'Senate'
 AND state = 'NY'
'''

insert_committee = '''INSERT INTO Committee
                       (cid, house, name, type, state, room, session_year)
                      VALUES
                       (%(cid)s, %(house)s, %(name)s, %(type)s, %(state)s, %(short_name)s, %(room)s, %(year)s)'''

insert_serveson = '''INSERT INTO servesOn
                      (pid, year, house, cid, state, position, start_date)
                     VALUES
                      (%(pid)s, %(year)s, %(house)s, %(cid)s, %(state)s,
                      %(position)s, %(date)s)'''

QI_COMMITTEENAME = '''
INSERT INTO CommitteeNames
 (name, house, state)
VALUES
 (%s, 'Senate', 'NY')
'''

update_committee_contact = '''UPDATE Committee
                              SET room = %(room)s
                              WHERE cid = %(cid)s
                               AND house = %(house)s
                               AND state = %(state)s
                               AND name = %(name)s'''
QU_SERVESON_END_DATE = '''UPDATE servesOn
                          SET end_date = %s
                          WHERE pid = %s
                           AND cid = %s
                           AND year = %s
                           AND state = "NY"
                           AND house = %s
                           AND end_date IS NULL'''

API_YEAR = 2016
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"

STATE = 'NY'


# this function takes in a full name and outputs a tuple with a first and last name
# names should be cleaned to maintain presence of Jr, III, etc, but remove middle names.
# many names for assembly members in the New York Senate API do not line up with the assembly
# website. The API names are replaced with the website names.
def clean_name(name):
    problem_names = {
        "Inez Barron": ("Charles", "Barron"),
        "Philip Ramos": ("Phil", "Ramos"),
        "Thomas McKevitt": ("Tom", "McKevitt"),
        "Albert Stirpe": ("Al", "Stirpe"),
        "Peter Abbate": ("Peter", "Abbate, Jr."),
        "Sam Roberts": ("Pamela", "Hunter"),
        "Herman Farrell": ("Herman", "Farrell, Jr."),
        "Fred Thiele": ("Fred", "Thiele, Jr."),
        "William Scarborough": ("Alicia", "Hyndman"),
        "Robert Oaks": ("Bob", "Oaks"),
        "Andrew Goodell": ("Andy", "Goodell"),
        "Peter Rivera": ("José", "Rivera"),
        "Addie Jenne Russell": ("Addie", "Russell"),
        "Kenneth Blankenbush": ("Ken", "Blankenbush"),
        "Alec Brook-Krasny": ("Pamela", "Harris"),
        "Mickey Kearns": ("Michael", "Kearns"),
        "Steven Englebright": ("Steve", "Englebright"),
    }

    ending = {'Jr': ', Jr.', 'Sr': ', Sr.', 'II': ' II', 'III': ' III', 'IV': ' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()
    suffix = ""
    for word in name_arr:
        if word != name_arr[0] and (len(word) <= 1 or word in ending.keys()):
            name_arr.remove(word)
            if word in ending.keys():
                suffix = ending[word]

    first = name_arr.pop(0)
    while len(name_arr) > 1:
        first = first + ' ' + name_arr.pop(0)
    last = name_arr[0]
    last = last.replace(' ', '') + suffix

    if (first + ' ' + last) in problem_names.keys():
        return problem_names[(first + ' ' + last)]

    return first, last


# calls NY Senate API and returns the list of results
def call_senate_api(restCall, house, offset):
    if house != "":
        house = "/" + house
    url = API_URL.format(restCall, API_YEAR, house, offset)
    print(url)
    r = requests.get(url)
    out = r.json()
    return out["result"]["items"]


# function gets the largest CID in the DB because CID does not autoincrement
def get_last_cid_db(cur):
    cur.execute(select_committee_last)

    query = cur.fetchone()
    return query[0]


# checks if Committee is in database.
# If it is, return its CID. Otherwise, return false
def is_comm_in_db(comm, cur):
    try:
        cur.execute(select_committee, {'house': comm['house'], 'name': comm['name'],
                                       'state': comm['state'], 'year': comm['year']})
        query = cur.fetchone()

        if query is None:
            return False
    except:
        logger.exception(format_logger_message('Select query failed for Committee', (select_committee % comm)))
        return False

    return query


# checks if a servesOn item exists in the DB.
# returns true/false as expected
def is_serveson_in_db(member, cur):
    try:
        temp = {'pid': member['pid'], 'house': member['house'], 'state': member['state'], 'cid': member['cid'],
                'year': member['year']}
        cur.execute(select_serveson, temp)
        #    {'pid':member['pid'], 'house':member['house'], 
        #  'state':member['state'], 'cid':member['cid'], 'year':member['year']})
        query = cur.fetchone()

        if query is None:
            return False
    except:
        logger.exception(format_logger_message('Select query failed for servesOn', (select_serveson % member)))

    return True


def update_contact_info(comm, cur):
    rows_updated = 0
    if comm['room'] is not None:
        try:
            temp = {'cid': comm['cid'], 'name': comm['name'], 'house': comm['house'], 'state': comm['state'],
                    'room': comm['room']}
            cur.execute(update_committee_contact, temp)
            rows_updated += 1
        except:
            logger.exception(format_logger_message('Update failed for Committee', (update_committee_contact %
                                                                                   temp)))

    return rows_updated


# function to call senate API and process all necessary data into lists and
# dictionaries. Returns list of committee dicts
def get_committees_api():
    committees = call_senate_api("committees", "senate", 0)
    ret_comms = list()

    for comm in committees:
        committee = dict()
        committee['short_name'] = comm['name'].replace('Committee', '').strip()
        committee['name'] = "Senate Standing Committee on {0}".format(committee['short_name'])
        committee['type'] = 'Standing'
        committee['house'] = "Senate"
        committee['state'] = STATE
        committee['room'] = None
        committee['year'] = comm['sessionYear']
        room_num = re.findall(r'\d+', comm['location'])
        if len(room_num) > 0:
            committee['room'] = room_num[0]
        committee['members'] = list()
        members = comm['committeeMembers']['items']

        for member in members:
            try:
                sen = dict()
                name = clean_name(member['fullName'])
                sen['last'] = name[1]
                sen['first'] = name[0]
                sen['year'] = member['sessionYear']
                sen['house'] = "Senate"
                sen['state'] = STATE

                if member['title'] == "CHAIR_PERSON":
                    sen['position'] = "chair"
                else:
                    sen['position'] = "member"

                committee['members'].append(sen)
            except IndexError:
                logger.exception('Person not found ' + member['fullName'])

        ret_comms.append(committee)

    # print "Downloaded %d committees..." % len(ret_comms)
    return ret_comms


def add_committee_name(cursor, name):
    global CN_I
    cursor.execute(QS_COMMITTEENAME_CHECK, (name,))

    if cursor.rowcount == 0:
        cursor.execute(QI_COMMITTEENAME, (name,))
        CN_I += cursor.rowcount


# function to add committees to DB. Calls API and then processes data
# only adds committees if they do not exist and only adds to servesOn if member
# is not already there.
def add_committees_db(cur):
    global C_INSERTED, S_INSERTED, C_UPDATED
    date = datetime.now().strftime('%Y-%m-%d')
    committees = get_committees_api()
    x = 0
    y = 0
    for committee in committees:
        cid = get_last_cid_db(cur) + 1
        get_cid = is_comm_in_db(committee, cur)
        add_committee_name(cur, committee['name'])
        if get_cid is False:
            x += 1
            committee['cid'] = str(cid)
            try:
                cur.execute(insert_committee, {'cid': committee['cid'],
                                               'house': committee['house'], 'name': committee['name'],
                                               'type': committee['type'],
                                               'state': committee['state'],
                                               'short_name': committee['short_name'],
                                               'room': committee['room'],
                                               'year': committee['year']})
                C_INSERTED += cur.rowcount
            except MySQLdb.Error:
                logger.exception(format_logger_message('Insert failed for Committee', (insert_committee % committee)))
        else:
            committee['cid'] = get_cid[0]
            C_UPDATED += update_contact_info(committee, cur)

        for member in committee['members']:
            member['pid'] = get_pid_db(member, cur)
            member['cid'] = committee['cid']

            if is_serveson_in_db(member, cur) is False:
                try:
                    member['date'] = date
                    cur.execute(insert_serveson, member)
                    S_INSERTED += cur.rowcount
                except MySQLdb.Error:
                    logger.exception(format_logger_message('Insert failed for servesOn', (insert_serveson % member)))
                y += 1
        check_committee_members(committee, cur)

        # print "Added %d committees and %d members" % (x,y)


temp2 = set()


# Checks if the member in database still exists.
# If member is no longer in api then fills in end_date field.
def check_committee_members(committee, cur):
    global S_UPDATED
    date = datetime.now().strftime('%Y-%m-%d')
    temp = dict()

    cur.execute(QS_SERVESON_MEMBERS, (committee['year'], committee['cid'], 'NY'))
    members = cur.fetchall()

    # print(members)
    for member in committee['members']:
        temp[member['pid']] = member

    for member in members:
        if member[0] not in temp:
            temp2.add(member[0])
            print(member[0], committee['cid'], len(committee['members']))
            try:
                cur.execute(QU_SERVESON_END_DATE, (date, member[0], committee['cid'],
                                                   committee['year'], committee['house']))
                S_UPDATED += cur.rowcount
            except MySQLdb.Error:
                logger.exception(format_logger_message('Update failed for servesOn',
                                                       (QU_SERVESON_END_DATE % (date, member[0], committee['cid'],
                                                                                committee['year'], committee['house']))))

    print(temp2)


# function to get PID of person based on name.
def get_pid_db(person, cur):
    cur.execute(select_person, person)

    query = cur.fetchone()
    return query[0]


def main():
    global API_YEAR
    API_YEAR = datetime.now().year

    with connect() as dddb:
        add_committees_db(dddb)

    LOG = {'tables': [{'state': 'NY', 'name': 'Committee', 'inserted': C_INSERTED, 'updated': C_UPDATED, 'deleted': 0},
                      {'state': 'NY', 'name': 'servesOn:', 'inserted': S_INSERTED, 'updated': S_UPDATED, 'deleted': 0},
                      {'state': 'NY', 'name': 'CommitteeNames', 'inserted': CN_I, 'updated': 0, 'deleted': 0}]}
    sys.stdout.write(json.dumps(LOG))
    logger.info(LOG)


if __name__ == '__main__':
    logger = create_logger()
    main()
