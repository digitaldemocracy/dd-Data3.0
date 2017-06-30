#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: insert_Behests.py
Author: Mandy Chan
Modified by: Andrew Rose
Date: 7/18/2015
Last Updated: 6/28/2017

Description:
  - Gathers Behest Data and puts it into DDDB

Usage:
  python insert_Behests.py

Source:
  - California Fair Political Practices Commission spreadsheets
    - Eventually we should start downloading the spreadsheets from their website once it updates.
      For now it assumes that the spreadsheet named behesteddata.csv is in the working directory.

Populates:
  - Behests (official, datePaid, payor, amount, payee, description, purpose, noticeReceived)
  - Organizations (name, city, state)
  - Payors (name, city, state)
"""

import MySQLdb
import csv
import traceback
import json
import sys
import datetime as dt
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import create_payload
from Utils.Utils import clean_name
from graylogger.graylogger import GrayLogger
from Database_Connection import mysql_connection

# SQL Queries
# Selects
SELECT_PERSON = '''SELECT p.pid FROM Person p
                   JOIN PersonStateAffiliation psa ON p.pid = psa.pid
                   WHERE psa.state = 'CA'
                   AND p.first LIKE %(OfficialFirst)s
                   AND p.last LIKE %(OfficialLast)s
                   '''
SELECT_LEGISLATOR = '''SELECT distinct p.pid FROM Person p
                       JOIN Legislator l on p.pid = l.pid
                       WHERE l.state = 'CA'
                       AND p.first LIKE %(OfficialFirst)s
                       AND p.last LIKE %(OfficialLast)s
                       '''
SELECT_ORG = '''SELECT oid FROM Organizations WHERE stateHeadquartered = 'CA'
                AND name SOUNDS LIKE %(Payee)s'''
SELECT_PAYOR = '''SELECT prid FROM Payors WHERE name SOUNDS LIKE %(Payor)s'''
SELECT_BEHEST = '''SELECT * FROM Behests
                   WHERE official = %(pid)s
                   AND datePaid = %(DateOfPayment)s
                   AND payor = %(prid)s
                   AND amount = %(Amount)s
                   AND payee = %(oid)s
                   '''
SELECT_STATE = '''SELECT * FROM State WHERE abbrev = %(state)s'''

# Inserts
INSERT_PAYOR = '''INSERT INTO Payors (name, city, state)
                  VALUES (%(Payor)s, %(PayorCity)s, %(PayorState)s)'''
INSERT_ORG = '''INSERT INTO Organizations (name, city, stateHeadquartered, source)
                VALUES (%(Payee)s, %(PayeeCity)s, %(PayeeState)s, 'import_csv_behests.py')'''
INSERT_BEHEST = '''INSERT INTO Behests (official, datePaid, payor, amount, payee,
                                        description, purpose, noticeReceived, sessionYear, state)
                   VALUES (%(pid)s, %(DateOfPayment)s, %(prid)s, %(Amount)s, %(oid)s,
                           %(Description)s, %(LGCpurpose)s, %(NoticeReceived)s, %(PaymentYear)s, 'CA')'''

# Globals
logger = None

ORG_INSERTED = 0
PAYOR_INSERTED = 0
BEHEST_INSERTED = 0

NAME_EXCEPTIONS = {
    "Achadijan, Katcho": "Achadjian, K.H. \"Katcho\"",
    "Achadjian, Katcho": "Achadjian, K.H. \"Katcho\"",
    "Allen, Ben": "Allen, Benjamin",
    "Bates, Patricia": "Bates, Pat",
    "Bonilla, Susan A.": "Bonilla, Susan",
    "Brown, Jr., Edumund G": "Brown, Edmund",
    "Calderon, Charles": "Calderon, Ian Charles",
    "Calderon, Ian": "Calderon, Ian Charles",
    "Cannella Anthony": "Cannella, Anthony",
    "Cedilo, Gilbert": "Cedillo, Gil",
    "Chu, Kasen": "Chu, Kansen",
    "Correa, Luis": "Correa, Lou",
    "Chau, Edwin": "Chau, Ed",
    "DeLeon, Kevin": "De Leon, Kevin",
    "DeSauiner, Mark": "DeSaulnier, Mark",
    "Dickinson, Rogert": "Dickinson, Roger",
    "Dodd, William": "Dodd, Bill",
    "Eggman, Susan": "Eggman, Susan Talamantes",
    "Emmerson, William": "Emmerson, Bill",
    "Frazier, James": "Frazier, Jim",
    'Gaines, Edward ""Ted""': "Gaines, Ted",
    "Gaines, Edward (Ted)": "Gaines, Ted",
    "Garcia, Christina": "Garcia, Cristina",
    "Glazer, Steven": "Glazer, Steve",
    "Hall, Isadore III": "Hall, Isadore",
    "Harman, Thomas": "Harman, Tom",
    "Hernandez, Edward": "Hernandez, Ed",
    "Holden, Christopher": "Holden, Chris",
    "Jackson, Hanna- Beth": "Jackson, Hannah-Beth",
    "Jones-Sawyer, Reginald Byron": "Jones-Sawyer, Reginald",
    "Knight, Steve": "Knight, Stephen",
    "Lackey, Thomas": "Lackey, Tom",
    "LaMalfa, Doug": "La Malfa, Doug",
    "Lockyer, William": "Lockyer, Bill",
    "McLeod -Negrete, Gloria": "Negrete McLeod, Gloria",
    "Nielsen, James": "Nielsen, Jim",
    "Pan Richard": "Pan, Richard",
    "Perea, Henry T.": "Perea, Henry",
    "Rodriquez, Freddie": "Rodriguez, Freddie",
    "Salas Jr., Rudy": "Salas, Rudy",
    "Simitian, Joseph": "Simitian, Joe",
    "Steinberg, Darryl": "Steinberg, Darrell",
    "Stone, Jeffrey": "Stone, Jeff",
    "Swanson, Sandre'": "Swanson, Sandre",
    "Thomas-Ridley, Sebastian": "Ridley-Thomas, Sebastian",
    "Ting, Phil": "Ting, Philip",
    "Vidak, James Andy": "Vidak, Andy",
    "Wieckowski, Robert": "Wieckowski, Bob"
}


def check_state(dddb, state):
    try:
        dddb.execute(SELECT_STATE, state)

        if dddb.rowcount > 0:
            return True
        else:
            return False

    except MySQLdb.Error:
        logger.warning("Behest selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Behests', (SELECT_BEHEST % behest)))


def is_behest_in_db(dddb, behest):
    try:
        dddb.execute(SELECT_BEHEST, behest)

        if dddb.rowcount > 0:
            return True
        else:
            return False

    except MySQLdb.Error:
        logger.warning("Behest selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Behests', (SELECT_BEHEST % behest)))
        return False


def insert_behest(dddb, behest):
    global BEHEST_INSERTED

    try:
        dddb.execute(INSERT_BEHEST, behest)
        BEHEST_INSERTED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("Behest insertion failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Behests', (INSERT_BEHEST % behest)))


def insert_payor(dddb, behest):
    global PAYOR_INSERTED

    if not check_state(dddb, {'state': behest['PayorState']}):
        behest['PayorState'] = None

    try:
        dddb.execute(INSERT_PAYOR, behest)
        PAYOR_INSERTED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("Payor insertion failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Payors', (INSERT_PAYOR % behest)))


def insert_payee(dddb, behest):
    global ORG_INSERTED

    if not check_state(dddb, {'state': behest['PayeeState']}):
        behest['PayeeState'] = None

    try:
        dddb.execute(INSERT_ORG, behest)
        ORG_INSERTED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("Organization insertion failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Organization', (INSERT_ORG % behest)))


def get_official_pid(dddb, official):
    try:
        if (official['OfficialType'] == 'Assembly'
            or official['OfficialType'] == 'Senate'):
            dddb.execute(SELECT_LEGISLATOR, official)

            if dddb.rowcount == 1:
                return dddb.fetchone()[0]
            else:
                print("Error selecting legislator with name " + official['Official'])
                return None

        else:
            dddb.execute(SELECT_PERSON, official)

            if dddb.rowcount != 0:
                return dddb.fetchone()[0]
            else:
                print("Error selecting person with name " + official['Official'])
                return None

    except MySQLdb.Error:
        print(traceback.format_exc())
        logger.warning("Person selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Organization', (SELECT_PERSON % official)))


def get_org_id(dddb, payee_org):
    try:
        dddb.execute(SELECT_ORG, payee_org)

        if dddb.rowcount != 0:
            return dddb.fetchone()[0]
        else:
            #print("Payee org with name " + payee_org['Payee'] + " not found")
            return None

    except MySQLdb.Error:
        logger.warning("Organization selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Organization', (SELECT_ORG % payee_org)))


def get_payor_id(dddb, payor):
    try:
        dddb.execute(SELECT_PAYOR, payor)

        if dddb.rowcount != 0:
            return dddb.fetchone()[0]
        else:
            #print("Payor with name " + payor['Payor'] + " not found")
            return None

    except MySQLdb.Error:
        logger.warning("Payor selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Organization', (SELECT_PAYOR % payor)))


def import_behests(dddb):
    with open('behesteddata.csv', 'rU') as csvfile:
        csv_reader = csv.DictReader(csvfile)

        behests = [row for row in csv_reader]

        for behest in behests:
            official_name = clean_name(behest['Official'], problem_names=NAME_EXCEPTIONS)
            #print(official_name)

            official = {'OfficialFirst': '%'+official_name['first']+'%', 'OfficialLast': '%'+official_name['last']+'%',
                        'OfficialType': behest['OfficialType'], 'Official': behest['Official']}

            behest['pid'] = get_official_pid(dddb, official)
            behest['prid'] = get_payor_id(dddb, behest)
            behest['oid'] = get_org_id(dddb, behest)

            if int(behest['PaymentYear']) % 2 == 0:
                behest['PaymentYear'] = int(behest['PaymentYear']) - 1

            try:
                behest['DateOfPayment'] = dt.datetime.strptime(behest['DateOfPayment'], '%m/%d/%y').date()
            except ValueError:
                behest['DateOfPayment'] = None

            try:
                behest['NoticeReceived'] = dt.datetime.strptime(behest['NoticeReceived'], '%m/%d/%y').date()
            except:
                behest['NoticeReceived'] = None

            if behest['pid'] is not None:
                if behest['prid'] is None:
                    insert_payor(dddb, behest)
                    behest['prid'] = dddb.lastrowid

                if behest['oid'] is None:
                    insert_payee(dddb, behest)
                    behest['oid'] = dddb.lastrowid

                if not is_behest_in_db(dddb, behest):
                    insert_behest(dddb, behest)


def main():
    dbinfo = mysql_connection(sys.argv)
    # MUST SPECIFY charset='utf8' OR BAD THINGS WILL HAPPEN.
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:

        import_behests(dddb)

        logger.info(__file__ + " terminated successfully.",
                    full_msg="Inserted " + str(BEHEST_INSERTED) + " rows in Behests, "
                             + str(PAYOR_INSERTED) + " rows in Payors, and "
                             + str(ORG_INSERTED) + " rows in Organizations.",
                    additional_fields={'_affected_rows': "Behests: " + str(BEHEST_INSERTED)
                                                         + ", Payors: " + str(PAYOR_INSERTED)
                                                         + ", Organizations: " + str(ORG_INSERTED),
                                       '_inserted': "Behests: " + str(BEHEST_INSERTED)
                                                    + ", Payors: " + str(PAYOR_INSERTED)
                                                    + ", Organizations: " + str(ORG_INSERTED),
                                       '_state': 'CA',
                                       '_log_type': 'Database'})

        LOG = {'tables': [{'state': 'CA', 'name': 'Behests', 'inserted': BEHEST_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'CA', 'name': 'Payors', 'inserted': PAYOR_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'CA', 'name': 'Organizations', 'inserted': ORG_INSERTED, 'updated': 0,
                           'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    with GrayLogger(GRAY_LOGGER_URL) as _logger:
        logger = _logger
        main()
