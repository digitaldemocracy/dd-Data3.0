#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: ny_import_contributions.py
Author: James Ly
Last Maintained: Andrew Rose
Last Updated: 08/05/2017

Description:
 - imports contributions for NY from followthemoney.org

Tables affected:
 - Organizations
 - Contribution
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
from Utils.Database_Connection import *
from Utils.Generic_Utils import *
from Utils.Generic_MySQL import *
from Constants.Contribution_Queries import *

logger = None

#global counters
I_O = 0      #org insert counter
I_C = 0      #contributiono insert counter

S_PERSON = '''SELECT p.pid
              FROM Person p, Legislator l
              WHERE p.pid = l.pid
              AND first = %s
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
                    AND donorOrg = %s
                    AND amount = %s
                    AND state = %s
                    AND oid = %s'''

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


def get_candidates(filename):
    """
    Gets a list of FollowTheMoney entity IDs from a file containing links to candidate
    profile pages on FollowTheMoney
    :param filename: The name of the file containing candidate info
    :return: A list of the candidate eids
    """
    s = set()
    f = open(filename)
    for line in f.readlines():
        data = line.split("&default=candidate")[0]
        ndx = data.find("eid=")
        s.add(data[ndx + 4:])
    return s


def get_name(eid):
    """
    Scrapes a candidate's name from their FollowTheMoney profile
    :param eid: The candidate's entity ID
    :return: A tuple containing the candidates first and last names
    """
    candidate_url = "https://www.followthemoney.org/entity-details?eid="
    candidate_url += eid

    page = requests.get(candidate_url)
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
    
    if eid == "26157993":
        first = "KIMBERLY"
        last = "JEAN-PERRE"
    if eid == "12456103":
        first = "ANDREA"
        last = "STEWART-COUSINS"

    first = first.strip()
    last = last.strip()

    return first, last


def special_names(first, last):
    """
    Fixes the names for candidates whose names are different
    in FollowTheMoney and our database
    :param first: A candidate's first name
    :param last: A candidate's last name
    :return: A tuple containing the fixed first and last names
    """

    if first == "DANIEL" and last == "STEC":
        first = "DAN"

    if first == "KENNETH" and last == "BLANKENBUSH":
        first = "KEN"

    if first == "PHILIP" and last == "RAMOS":
        first = "PHIL"

    if first == "NICK" and last == "PERRY":
        first = "N NICK"
    if first == "ADDIE JENNE" and last == "RUSSELL":
        first = "ADDIE" and last == "JENNE"
    
    if first == "RONALD" and last == "CASTORINA":
        first = "RON"

    if first == "ELIZABETH" and last == "LITTLE":
        first = "BETTY"

    if first == "PATRICIA" and last == "RITCHIE":
        first = "PATTY"

    if first == "ROBERT" and last == "OAKS":
        first = "BOB"

    if first == "FREDERICK" and last == "AKSHAR":
        first = "FREDRICK"

    if first == "STACEY" and last == "AMATO":
        first = "STACEY PHEFFER"

    if first == "CHRISTOPHER" and last == "JACOBS":
        first = "CHRIS"

    if first == "YUH" and last == "NIOU":
        first = "YUH-LINE"
        
    if first == "EARLENE HILL" and last == "HOOPER":
        first = "EARLENE"

    if first == "ERIK MARTIN" and last == "DILAN":
        first = "ERIK"

    if first == "ALBERT" and last == "STIRPE":
        first = "AL"

    if first == "LATRICE MONIQUE" and last == "WALKER":
        first = "LATRICE"

    if first == "DANIEL" and last == "ODONNELL":
        last = "O'DONNELL"

    if first == "ANDREW" and last == "GOODELL":
        first = "ANDY"

    if first == "MARTIN MALAVE" and last == "DILAN":
        first = "MARTIN"

    if first == "JAMES GARY" and last == "PRETLOW":
        first = "J GARY"

    if first == "KIMBERLY" and last == "JEAN-PERRE":
        last = "JEAN-PIERRE"

    if first == "PHILIP" and last == "BOYLE":
        first = "PHIL"

    if first == "JOE" and last == "ERRIGO":
        first = "JOSEPH"

    if first == "THOMAS" and last == "MCKEVITT":
        first = "TOM"

    if first == "PHILLIP" and last == "STECK":
        first = "PHIL"

    if first == "RONALD" and last == "KIM":
        first = "RON"

    if first == "STEVEN" and last == "ENGLEBRIGHT":
        first = "STEVE"

    if first == "ALFRED" and last == "GRAF":
        first = "AL"

    return first, last


def get_pid(cursor, first, last):
    """
    Gets a candidate's PID in our database given their name
    :param cursor: A connection to the DDDB
    :param first: The candidate's first name
    :param last: The candidate's last name
    :return: A PID from our database
    """
    result = None
    last += "%"
    try:
        cursor.execute(S_PERSON, (first, last))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]
    except MySQLdb.Error:
        logger.exception(format_logger_message('Select Failed for Person', (S_PERSON % (first, last))))

    return result


def get_house(cursor, pid, session_year, state):
    """
    Gets the legislative house a candidate belongs to
    :param cursor: A connection to the DDDB
    :param pid: A person's PID
    :param session_year: The session year the candidate is serving in office
    :param state: The state the candidate serves in
    :return: The name of the candidate's legislative house
    """

    result = None
    try:
        cursor.execute(S_TERM, (pid, session_year, state))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.exception(format_logger_message('Select Failed for Term', (S_TERM % (pid, session_year, state))))

    return result


def get_records(url):
    """
    Gets a JSON-formatted list of contribution records for a candidate
    from FollowTheMoney's API
    :param url: A URL containing an API query for FollowTheMoney's API
    :return: A JSON-formatted list of contribution records
    """
    page = requests.get(url)
    result = page.json()
    return result['records']


def get_oid(cursor, name):
    """
    Gets an Organization's OID given its name
    :param cursor: A connection to the database
    :param name: An organization's name
    :return: The organization's OID if one exists
    """
    result = None
    try:
        cursor.execute(S_ORGANIZATION, (name,))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.exception(format_logger_message('Select Failed for Organization', (S_ORGANIZATION % (name,))))

    return result


def insert_org(cursor, name, state):
    """
    Inserts an organization into the database
    :param cursor: A connection to the database
    :param name: The organization's name
    :param state: The state the organization is active in
    """
    global I_O
    try:
        cursor.execute(I_ORGANIZATION, (name, state))
        I_O += cursor.rowcount

    except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for Organization', (I_ORGANIZATION % (name, state))))


def get_con_id(cursor, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid):
    """
    Gets a contribution ID from the database
    :param cursor: A connection to the database
    :param con_id: The contribution's ID
    :param pid: The PID of the candidate being contributed to
    :param year: The year the contribution was made
    :param date: The date the contribution was made
    :param house: The legislative house the candidate was running for
    :param donor_name: The name of the donor of the contribution
    :param donor_org: The name of the donor's organization
    :param amount: The amount contributed
    :param state: The state the legislator ran in
    :param oid: The donor organization's OID
    :return: A contribution ID
    """
    result = None
    try:
        cursor.execute(S_CONTRIBUTION, (con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.exception(format_logger_message('Select Failed for Contribution',
                                               I_CONTRIBUTION % (
                                               con_id, pid, year, date, house, donor_name, donor_org, amount, state,
                                               oid)))

    return result


def insert_contribution(cursor, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid):
    """
    Inserts a contribution ID to the database
    :param cursor: A connection to the database
    :param con_id: The contribution's ID
    :param pid: The PID of the candidate being contributed to
    :param year: The year the contribution was made
    :param date: The date the contribution was made
    :param house: The legislative house the candidate was running for
    :param donor_name: The name of the donor of the contribution
    :param donor_org: The name of the donor's organization
    :param amount: The amount contributed
    :param state: The state the legislator ran in
    :param oid: The donor organization's OID
    """
    global I_C
    try:
        if get_con_id(cursor, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid) is None:
            cursor.execute(I_CONTRIBUTION, (con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid))
            I_C += cursor.rowcount
    except MySQLdb.Error:
        logger.exception(format_logger_message('Insert Failed for Contribution',
                                               I_CONTRIBUTION % (
                                                   con_id, pid, year, date, house, donor_name, donor_org, amount, state,
                                                   oid)))


def main():
    with connect() as dddb:
        session_year = get_session_year(dddb, 'NY', logger)
        state = "NY"

        api_url = "http://api.followthemoney.org/?c-t-eid="
        api_key = "&gro=d-id&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05"
        mode = "&mode=json"
        eid_list = get_candidates("candidates.txt")
        for eid in eid_list:
            first, last = get_name(eid)
            first, last = special_names(first, last)
            pid = get_pid(dddb, first, last)
            if pid is not None:
                house = get_house(dddb, pid, session_year, state)

                #if can find legislator in 2015 they seem to be in 2017 term
                if house is None:
                    house = get_house(dddb, pid, "2017", state)

                if house is not None:
                    records = get_records(api_url + eid + api_key + mode)

                    #get record data
                    for r in records:
                        date = r['Date']['Date']
                        type_contributor = r['Type_of_Contributor']['Type_of_Contributor']
                        contributor = r['Contributor']['Contributor']
                        amount = r['Amount']['Amount']

                        #some donations apparently dont have dates
                        if str(date) == '':
                            date = None
                            year = None
                        else:
                            date = str(date) + " 00:00:00"
                            year = date.split("-")[0]

                        donor_name = None
                        donor_org = None
                        oid = None
                        if type_contributor == "Individual" or "FRIENDS" in contributor or type_contributor == 'Other':
                            #individual names have commas
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

                        con_id = hash((pid, date, donor_name, donor_org, donor_org))
                        con_id = str(con_id)
                        con_id = con_id[0:21]

                        insert_contribution(dddb, con_id, pid, year, date, house, donor_name, donor_org, amount, state, oid)

        LOG = {'tables': [{'state': 'NY', 'name': 'Organizations', 'inserted': I_O, 'updated': 0, 'deleted': 0},
                          {'state': 'NY', 'name': 'Contribution:', 'inserted': I_C, 'updated': 0, 'deleted': 0}]}
        logger.info(LOG)
        sys.stderr.write(json.dumps(LOG))
    

if __name__ == '__main__':
    logger = create_logger()
    main()
