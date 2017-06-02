#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: ny_import_contributions.py
Author: James Ly
Last Maintained: James Ly
Last Updated: 02/06/2017

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


def create_payload(table, sqlstmt):
    return {
      '_table': table,
      '_sqlstmt': sqlstmt,
      '_state': 'NY'
    }

'''
parse eid from text file
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
given eid, scrape the candidate's name
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
    
    if eid == "26157993":
        first = "KIMBERLY"
        last = "JEAN-PERRE"
    if eid == "12456103":
        first = "ANDREA"
        last = "STEWART-COUSINS"

    first = first.strip()
    last = last.strip()

    return first, last

'''
some names are different in the db
changed scraped names to ones in db
'''
def special_names(first, last):
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

'''
get pid from db
'''
def get_pid(cursor, first, last):
    result = None
    last += "%"
    try:
        cursor.execute(S_PERSON, (first, last))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]
    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(), additional_fields=create_payload('Legislator', (S_PERSON % (first, last))))

    return result

'''
get house membership from pid and sessionyear
'''
def get_house(cursor, pid, sessionYear, state):
    result = None
    try:
        cursor.execute(S_TERM, (pid, sessionYear, state))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(), additional_fields=create_payload('Term', (S_TERM % (pid, sessionYear, state))))

    return result

'''
get contribution records from api
'''
def get_records(url):
    page = requests.get(url)
    result = page.json()
    return result['records']

'''
get oid from db
'''
def get_oid(cursor, name):
    result = None
    try:
        cursor.execute(S_ORGANIZATION, (name,))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(), additional_fields=create_payload('Organizations', (S_ORGANIZATION % (name, ))))

    return result


def insert_org(cursor, name, state):
    global I_O
    try:
        cursor.execute(I_ORGANIZATION, (name, state))
        I_O += cursor.rowcount

    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(), additional_fields=create_payload('Organzations', (I_ORGANIZATION % (name, state))))


'''
get id from Contribution table
'''
def get_conID(cursor, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid):
    result = None
    try:
        cursor.execute(S_CONTRIBUTION, (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))
        if cursor.rowcount > 0:
            result = cursor.fetchone()[0]

    except MySQLdb.Error:
        logger.warning('Select Failed', full_msg=traceback.format_exc(), additional_fields=create_payload('Contribution', (I_CONTRIBUTION % (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))))

    return result

def insert_contribution(cursor, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid):
    global I_C
    try:
        if get_conID(cursor, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid) is None:
            cursor.execute(I_CONTRIBUTION, (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))
            I_C += cursor.rowcount
    except MySQLdb.Error:
        logger.warning('Insert Failed', full_msg=traceback.format_exc(), additional_fields=create_payload('Contribution', (I_CONTRIBUTION % (conID, pid, year, date, house, donorName, donorOrg, amount, state, oid))))

def main():
    ddinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=ddinfo['host'],
                        user=ddinfo['user'],
                        db=ddinfo['db'],
                        port=ddinfo['port'],
                        passwd=ddinfo['passwd'],
                        charset='utf8') as dddb:
        
        year = "2016"
        sessionYear = "2015"
        state = "NY"

        apiUrl = "http://api.followthemoney.org/?c-t-eid="
        apiKey = "&gro=d-id&APIKey=dbfd94e9b2eb052a0a5363396c4b9a05"
        mode = "&mode=json"
        eidList = get_candidates("candidates.txt")
        for eid in eidList:
            first, last = get_name(eid)
            first, last = special_names(first, last)
            pid = get_pid(dddb, first, last)
            if pid is not None:
                house = get_house(dddb, pid, sessionYear, state)

                #if can find legislator in 2015 they seem to be in 2017 term
                if house is None:
                    house = get_house(dddb, pid, "2017", state)

                if house is not None:
                    records = get_records(apiUrl + eid + apiKey + mode)

                    #get record data
                    for r in records:
                        date = r['Date']['Date']
                        typeContributor = r['Type_of_Contributor']['Type_of_Contributor']
                        contributor = r['Contributor']['Contributor']
                        amount = r['Amount']['Amount']

                        #some donations apparently dont have dates
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
                            #individual names have commas
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
                        


                        conID = hash((pid, date, donorName, donorOrg, donorOrg))
                        conID = str(conID)
                        conID = conID[0:21]

                        insert_contribution(dddb, conID, pid, year, date, house, donorName, donorOrg, amount, state, oid)

                        
        print "Inserted" + str(I_O) + " rows into Organizations"
        print "Inserted" + str(I_C) + " rows into Contribution"
        
        logger.info(__file__ + ' terminated successfully.', 
            full_msg='Inserted ' + str(I_O) + ' rows in Organizations and inserted ' 
                      + str(I_C) + ' rows in Contribution',
            additional_fields={'_affected_rows':'Organizations:'+ str(I_O) +
                                           ', Contribution:'+ str(I_C),
                               '_inserted':'Organizations:'+ str(I_O) +
                                           ', Contribution:' + str(I_C),
                               '_state':'NY'})
    

if __name__ == '__main__':
    with GrayLogger(GRAY_URL) as _logger:
        logger = _logger
        main()
