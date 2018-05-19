#!/usr/bin/python3

"""
File: ny_import_legislators.py
Author: John Alkire
Modified: Eric Roh
Date: 6/21/2016
Description:
- Imports NY legislators using senate API
- Fills Person, Term, and Legislator
- Missing personal/social info for legislators (eg. bio, twitter, etc)
- Currently configured to test DB
"""

import json
import sys
import traceback
import requests
import MySQLdb
from datetime import datetime
from Utils.Generic_Utils import *
from Utils.Database_Connection import connect

logger = None
P_INSERT = 0
PSA_INSERT = 0
L_INSERT = 0
T_INSERT = 0
T_UPDATE = 0

insert_person = '''INSERT INTO Person
                (last, first, image, source)
                VALUES
                (%(last)s, %(first)s, %(image)s, "ny_import_legislators.py");'''

insert_person_state_affiliation = '''INSERT INTO PersonStateAffiliation
                                     (pid, state)
                                     VALUES
                                     (%(pid)s, 'NY')'''

insert_legislator = '''INSERT INTO Legislator
                (pid, state)
                VALUES
                (%(pid)s, %(state)s);'''

insert_term = '''INSERT INTO Term
                (pid, year, house, state, district, current_term, start)
                VALUES
                (%(pid)s, %(year)s, %(house)s, %(state)s, %(district)s, %(current)s, %(start)s);'''

QS_TERM_MEMBERS = '''
SELECT pid
FROM Term
WHERE year = %s
 AND current_term = 1
 AND state = 'NY'
'''

select_person_state_affiliation = '''select * from PersonStateAffiliation
                                     where state = 'NY'
                                     and pid = %(pid)s'''

select_legislator = '''SELECT p.pid
                       FROM Person p, Legislator l
                       WHERE first = %(first)s 
                        AND last = %(last)s 
                        AND state = %(state)s
                        AND p.pid = l.pid'''

select_term = '''SELECT district, current_term
                 FROM Term
                 WHERE pid = %(pid)s 
                  AND state = %(state)s
                  AND year = %(year)s
                  AND house = %(house)s'''

QU_TERM = '''UPDATE Term
             SET district = %(district)s, current_term = 1
             WHERE pid = %(pid)s
              AND state = %(state)s
              AND year = %(year)s
              AND house = %(house)s'''
QU_TERM_END_DATE = '''
UPDATE Term
SET end = %s, current_term = 0
WHERE pid = %s
 AND year = %s
 AND state = 'NY'
 AND end IS NULL
'''

API_YEAR = 2017
API_URL = "http://legislation.nysenate.gov/api/3/{0}/{1}{2}?full=true&"
# API_URL += "limit=1000&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"
API_URL += "limit=200&key=31kNDZZMhlEjCOV8zkBG1crgWAGxwDIS&offset={3}"


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
        "WILLIAMS": ("Jamie", "Williams"),
        "PEOPLES-STOKE": ("Crystal", "Peoples-Stoke"),
        "KAMINSKY": ("Todd", "Kaminsky"),
        "HYNDMAN": ("Alicia", "Hyndman"),
        "HUNTER": ("Pamela", "Hunter"),
        "HARRIS": ("Pamela", "Harris"),
        "CASTORINA": ("Ron", "Castorina", "Jr"),
        "CANCEL": ("Alice", "Cancel"),
        "BARNWELL": ("Brian", "Barnwell"),
        "BYRNE": ("Kevin", "Byrne"),
        "CARROLL": ("Robert", "CARROLL"),
        "ERRIGO": ("Joseph", "Errigo"),
        "HEASTIE": ("Carl", "Heastie"),
        "JONES": ("Billy", "Jones"),
        "NORRIS": ("Michael", "Norris"),
        "VANEL": ("Clyde", "Vanel"),
        "WALSH": ("Mary", "Walsh"),
        "MILLER": ("Brian", "Miller"),
        "PELLEGRINO": ("Christine", "Pellegrino"),
        "ASHBY": ("Jake", "Ashby"),
        "BOHEN": ("Erik", "Bohen"),
        "MIKULIN": ("John", "Mikulin")


    }

    ending = {'Jr': ', Jr.', 'Sr': ', Sr.', 'II': ' II', 'III': ' III', 'IV': ' IV'}
    name = name.replace(',', ' ')
    name = name.replace('.', ' ')
    name = name.replace('  ', ' ')
    name_arr = name.split()

    suffix = ""

    if len(name_arr) == 1 and name_arr[0] in problem_names.keys():
        name_arr = list(problem_names[name_arr[0]])


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
    r = requests.get(url)
    out = r.json()
    return out["result"]["items"]


def is_person_state_affiliation_in_db(senator, dddb):
    try:
        dddb.execute(select_person_state_affiliation, senator)

        query = dddb.fetchone()

        if query is None:
            return False

    except:
        logger.exception(format_logger_message('Select failed in PersonStateAffiliation', (select_person_state_affiliation % senator)))
        return False

    return query[0]


# checks if Legislator + Person is in database.
# If it is, return its PID. Otherwise, return false
def is_leg_in_db(senator, dddb):
    try:
        dddb.execute(select_legislator, senator)
        query = dddb.fetchone()

        if query is None:
            return False
    except:
        return False

    return query[0]


# checks if Term + Person is in database.
# UPDATES the Term if the district has changed. 
# returns true/false as expected
def is_term_in_db(senator, dddb):
    global T_UPDATE
    try:
        dddb.execute(select_term, senator)
        query = dddb.fetchone()

        if query[0] != senator['district'] or query[1] == 0:
            try:
                dddb.execute(QU_TERM, senator)
                T_UPDATE += dddb.rowcount
            except MySQLdb.Error:
                logger.exception(format_logger_message('Update failed for Term', (QU_TERM % senator)))

            T_UPDATE += dddb.rowcount
            return True
        if query is None:
            return False
    except:
        return False

    return True


# function to call senate API and process all necessary data into lists and
# dictionaries. Returns list of senator dicts
def get_senators_api():
    senators = call_senate_api("members", "", 0)
    ret_sens = list()
    for senator in senators:
        try:
            sen = dict()
            name = clean_name(senator['fullName'])
            sen['house'] = senator['chamber'].title()
            sen['last'] = name[1]
            sen['state'] = "NY"
            sen['year'] = senator['sessionYear']
            sen['first'] = name[0]
            sen['district'] = senator['districtCode']
            sen['image'] = senator['imgName']
            if sen['image'] is None:
                sen['image'] = ''
            ret_sens.append(sen)
        except IndexError:
            logger.exception('Problem with name ' + senator['fullName'])
    return ret_sens


# function to add legislator's data to Person, Legislator, and Term
# adds to Person and Legislator if they are not already filled
# and adds to Term if it is not already filled
def add_senator_db(senator, dddb):
    global P_INSERT, PSA_INSERT, L_INSERT, T_INSERT
    date = datetime.now().strftime('%Y-%m-%d')
    pid = is_leg_in_db(senator, dddb)
    ret = False
    senator['pid'] = pid
    senator['start'] = date
    if pid is False:
        try:
            dddb.execute(insert_person, senator)
            P_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for Person', (insert_person % senator)))

        pid = dddb.lastrowid
        senator['pid'] = pid
        try:
            dddb.execute(insert_person_state_affiliation, senator)
            PSA_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for PersonStateAffiliation'), (insert_person_state_affiliation % senator))

        try:
            dddb.execute(insert_legislator, senator)
            L_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for Legislator', (insert_legislator % senator)))

        ret = True

    if is_person_state_affiliation_in_db(senator, dddb) is False:
        try:
            dddb.execute(insert_person_state_affiliation, senator)
            PSA_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for PersonStateAffiliation'), (insert_person_state_affiliation % senator))


    if is_term_in_db(senator, dddb) is False:
        try:
            senator['current'] = 1
            dddb.execute(insert_term, senator)
            T_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.exception(format_logger_message('Insert failed for Term', (insert_term % senator)))
    return ret


# checks if the legislators are still in office
# If member is no longer in api then fills in the end date.
def check_legislator_members(senator, dddb):
    global T_UPDATE
    # TODO
    date = datetime.now().strftime('%Y-%m-%d')
    temp = dict()

    dddb.execute(QS_TERM_MEMBERS, (senator[0]['year'],))
    members = dddb.fetchall()

    for member in senator:
        temp[member['pid']] = member

    for member in members:
        if member[0] not in temp:
            try:
                dddb.execute(QU_TERM_END_DATE, (date, member[0], senator['year']))
                T_UPDATE += dddb.rowcount
            except MySQLdb.Error:
                logger.exception(format_logger_message('Update failed for Term',
                                                       (QU_TERM_END_DATE % (date, member[0], senator['year']))))


# function to add legislators to DB. Calls API and calls add_senator_db on
# each legislator
def add_senators_db(dddb):
    senators = get_senators_api()
    x = 0
    for senator in senators:
        if add_senator_db(senator, dddb):
            x += 1


def main():
    with connect() as dddb:
        add_senators_db(dddb)

    LOG = {'tables': [{'state': 'NY', 'name': 'Person', 'inserted': P_INSERT, 'updated': 0, 'deleted': 0},
                      {'state': 'NY', 'name': 'PersonStateAffiliation', 'inserted': PSA_INSERT, 'updated': 0, 'deleted': 0},
                      {'state': 'NY', 'name': 'Legislator', 'inserted': L_INSERT, 'updated': 0, 'deleted': 0},
                      {'state': 'NY', 'name': 'Term', 'inserted': T_INSERT, 'updated': T_UPDATE, 'deleted': 0},]}
    sys.stderr.write(json.dumps(LOG))
    logger.info(LOG)


if __name__ == '__main__':
    logger = create_logger()
    main()
