#!/usr/bin/env python
# -*- coding: utf8 -*-

'''
File: fl_import_lobbyists.py
Author: Nick Russo
Maintained: Nick Russo
Date: 4/3/2017
Last Modified: 4/3/17

Description:
  - Imports FL lobbyist data from files from FL lobbyist registration website.

Populates:
  - Person
    - (first, last)
  - Lobbyist
    - (pid, filer_id, state)
  - LobbyingFirm
    - (filer_naml)
  - LobbyingFirmState
    - (filer_id, rpt_date, ls_beg_yr, ls_end_yr, filer_naml, state)
  - LobbyistEmployment (X not populated X)
    - (pid, sender_id, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistDirectEmployment
    - (pid, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyingContracts
    - (filer_id, lobbyist_employer, rpt_date, ls_beg_yr, ls_end_yr, state)
  - LobbyistEmployer
    - (filer_id, oid, state)
  - Organizations
    - (name, city, stateHeadquartered, type)

Source:
  EXEC_LOBBYIST_URL = 'https://www.floridalobbyist.gov/reports/elob.txt'
  LEG_LOBBYIST_URL = 'https://www.floridalobbyist.gov/reports/llob.txt'
'''

from Database_Connection import mysql_connection
import pprint
import re
import sys
from datetime import date
import traceback
import urllib
import urllib2
import json
import MySQLdb
import csv
import os
from GrayLogger.graylogger import GrayLogger
from Constants.Lobbyist_Queries import *
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import *

logger = None

EXEC_LOBBYIST_URL = 'https://www.floridalobbyist.gov/reports/elob.txt'
LEG_LOBBYIST_URL = 'https://www.floridalobbyist.gov/reports/llob.txt'
LOBBYIST_FILE_DIRECTORY = "./lobbyistFiles/"
TODAYS_EXEC_FILE = LOBBYIST_FILE_DIRECTORY + str(date.today()) + "_elob.txt"
TODAYS_LEG_FILE = LOBBYIST_FILE_DIRECTORY + str(date.today()) + "_llob.txt"
# GLOBALS

P_INSERT = 0
L_INSERT = 0
O_INSERT = 0
LFS_INSERT = 0
LF_INSERT = 0
LEMPLOYER_INSERT = 0
LEMPLOYMENT_INSERT = 0
LDE_INSERT = 0
LC_INSERT = 0


# INSERTS


name_checks = ['(', '\\' ,'/', 'OFFICE', 'LLC', 'LLP', 'INC', 'PLLC', 'LP', 'PC', 'CO', 'LTD', 
                'ASSOCIATES', 'ASSOCIATION', 'AFFILIATES', 'CORPORATION', '&', 'INTERNATIONAL', 
                'UNION', 'SOCIETY', 'CHAPTER', 'NATIONAL', 'FOUNDATION', 'PUBLIC', 'MANAGEMENT']
name_acronyms = ['LLC', 'LLP', 'INC', 'PLLC', 'LP', 'PC', 'CO', 'LTD', 'II']
reporting_period = {'JF':0, 'MA':1, 'MJ':2, 'JA':3, 'SO':4, 'ND':5}


def download_files():
    if not os.path.exists(LOBBYIST_FILE_DIRECTORY):
            os.makedirs(LOBBYIST_FILE_DIRECTORY)
    if not os.path.exists(TODAYS_EXEC_FILE):
        urllib.urlretrieve(EXEC_LOBBYIST_URL, TODAYS_EXEC_FILE);
    if not os.path.exists(TODAYS_LEG_FILE):
        urllib.urlretrieve(LEG_LOBBYIST_URL, TODAYS_LEG_FILE);

def is_person_in_db(dddb, lobbyist):
    try:
        dddb.execute(QS_PERSON, lobbyist)
        query = dddb.fetchone()
    
        if query is not None:
            return query[0]
    except:
        logger.warning('Check Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload('Person', (QS_PERSON%lobbyist)))
        return False

    return False

def get_pid(dddb, lobbyist):
    global P_INSERT
    pid = is_person_in_db(dddb, lobbyist)
    if not pid:
        try:
            dddb.execute(QI_PERSON, lobbyist)
            pid = dddb.lastrowid
            P_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload('Person', (QI_PERSON%lobbyist)))
    return pid

def is_lobbyist_in_db(dddb, lobbyist):
    try:
        dddb.execute(QS_LOBBYIST, lobbyist)
        query = dddb.fetchone()
    
        if query is not None:
            return query[0]
    except:
        logger.warning('Check Failed', full_msg=traceback.format_exc(),
                additional_fields=create_payload('Lobbyist', (QS_LOBBYIST%lobbyist)))
        return False

    return False

def insert_lobbyist(dddb, lobbyist):
    global L_INSERT
    lobbyist["pid"] = is_lobbyist_in_db(dddb, lobbyist)
    if not lobbyist["pid"]:
        try:
            lobbyist['pid'] = get_pid(dddb, lobbyist)
            lobbyist['filer_id'] = lobbyist['pid']
            dddb.execute(QI_LOBBYIST, lobbyist)
            L_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload('Lobbyist', (QI_LOBBYIST%lobbyist)))
    return lobbyist


def insert_organization(dddb, org):
    global O_INSERT
    org["oid"] = is_obj_in_db(dddb, QS_ORGANIZATIONS, org, "Organization", logger)
    if not org["oid"]:
        try:
            dddb.execute(QI_ORGANIZATIONS, org)
            org["oid"] = dddb.lastrowid
            O_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                    additional_fields=create_payload('Organizations', (QI_ORGANIZATIONS%org)))
    return org


def parse_lobbyist(dddb, lobFile, lobUrl):
    rowCount = 0;
    global LFS_INSERT
    global LF_INSERT
    global LEMPLOYER_INSERT
    global LEMPLOYMENT_INSERT
    global LDE_INSERT

    with open(lobFile, "r") as lob:
        pp = pprint.PrettyPrinter(indent=4)
        print("AsdfasdFa")
        datareader = csv.reader(lob, delimiter="\t")
        lobbyist = dict()
        org = dict()
        lobFirm = dict()
        for row in datareader:
            rowCount = rowCount + 1
            print("asdf" + str(rowCount))
            #if rowCount >= 1000:
            #    return;
            if rowCount <= 3:
                continue;
            else: 
                print("geg")
                # Adding person table info
                lobbyist['last'] = row[0]
                lobbyist['first'] = row[1] + " " + row[3] # First name + suffix
                lobbyist['middle'] = row[2]
                lobbyist['source'] = lobUrl
                    
                # Adding lobbyist table info
                lobbyist['state'] = "FL"
                lobbyist = insert_lobbyist(dddb, lobbyist)

                # if the lobbyist works for a firm
                if len(row) > 24:
                    
                    # Add Organization info
                    org["name"] = row[23]
                    org["city"] = row[26]
                    if len(row[27].strip()) > 2 and row[27].strip() in STATE_ABBREV:
                        org["stateHeadquartered"] = STATE_ABBREV[row[27].strip().lower().title()]
                    elif len(row[27].strip()) > 2 and row[27].strip() not in STATE_ABBREV:
                        print(repr(row[27]))
                        continue
                    else:
                        org["stateHeadquartered"] = row[27].strip().upper()
                    org["source"] = lobUrl
                    org = insert_organization(dddb, org)
                     
                    
                    # Add LobbyingFirm table info
                    lobFirm["oid"] = org["oid"]
                    lobFirm["state"] = "FL"
                    lobFirm["filer_naml"] = row[23]
                    LF_INSERT += insert_obj(dddb, lobFirm, QS_LOBBYINGFIRM, QI_LOBBYINGFIRM, "Lobbyist Firm", logger)
                    
                    # Add LobbyingFirmState table info
                    lobFirm["filer_id"] = org["oid"] # use oid for unique key
                    print(row[33].split("/"))
                    print(row[34].split("/"))
                    print(row)
                    if row[33]:
                        lobFirm["ls_beg_yr"] = int(row[33].split("/")[-1])
                    else:
                        lobFirm["ls_beg_yr"] = None
                    if row[34]:
                        lobFirm["ls_end_yr"] = int(row[34].split("/")[-1])
                    else:
                        lobFirm["ls_end_yr"] = None
                    LFS_INSERT += insert_obj(dddb, lobFirm,  QS_LOBBYINGFIRMSTATE, QI_LOBBYINGFIRMSTATE, "Lobbyist Firm State", logger)

                    # Add LobbyistEmployer skipping coalition
                    # Skipping coalition
                    # Use previous filer_id
                    LEMPLOYER_INSERT += insert_obj(dddb, lobFirm, QS_LOBBYISTEMPLOYER, QI_LOBBYISTEMPLOYER, "Lobbyist Employer", logger)
                    
                    # Add LobbyistEmployment
                    lobbyist["sender_id"] = lobFirm["filer_id"]
                    lobbyist["rpt_date"] = str(date.today())
                    if row[14]:
                        lobbyist["ls_beg_yr"] = int(row[14].split("/")[-1])
                    else:
                        lobbyist["ls_beg_yr"] = None
                    if row[15]:
                        lobbyist["ls_end_yr"] = int(row[15].split("/")[-1])
                    else:
                        lobbyist["ls_end_yr"] = 2018
                    LEMPLOYMENT_INSERT += insert_obj(dddb, lobbyist, QS_LOBBYISTEMPLOYMENT, QI_LOBBYISTEMPLOYMENT, "Lobbyist Employment", logger)
                    
                    # Add Organization they are representing
                    org["name"] = row[13]
                    org["city"] = row[18]
                    if len(row[19].strip()) > 2 and row[19].strip() in STATE_ABBREV:
                        org["stateHeadquartered"] = STATE_ABBREV[row[19].strip().lower().title()]
                    elif len(row[19].strip()) > 2 and row[19].strip() not in STATE_ABBREV:
                        print(repr(row[19]))
                        continue
                    else:
                        org["stateHeadquartered"] = row[19].strip().upper()
                    org = insert_organization(dddb, org)
                    
                else:
                    # Add Organization they are representing
                    org["name"] = row[13]
                    org["type"] = 0
                    org["city"] = row[18]
                    org["source"] = lobUrl
                    if len(row[19].strip()) > 2 and row[19].strip() in STATE_ABBREV:
                        org["stateHeadquartered"] = STATE_ABBREV[row[19].strip().lower().title()]
                    elif len(row[19].strip()) > 2 and row[19].strip() not in STATE_ABBREV:
                        print(repr(row[19]))
                        continue
                    else:
                        org["stateHeadquartered"] = row[19].strip().upper()
                    org["state"] = "FL"
                    org = insert_organization(dddb, org)
                    
                    # Add LobbyistEmployer in-house lobbyist use organization they are representing
                    # Skipping coalition
                    org['filer_id'] = org["oid"]
                    LEMPLOYER_INSERT += insert_obj(dddb, org, QS_LOBBYISTEMPLOYER, QI_LOBBYISTEMPLOYER, "Lobbyist Employer", logger)

                    # Add LobbyistEmployment
                    # NEED PID 
                    # NEED OID for lobbyist_employer
                    lobbyist["lobbyist_employer"] = org["filer_id"]
                    lobbyist["rpt_date"] = str(date.today())
                    if row[14]:
                        lobbyist["ls_beg_yr"] = row[14].split("/")[-1]
                    else:
                        lobbyist["ls_beg_yr"] = None
                    if row[15]:
                        lobbyist["ls_end_yr"] = int(row[15].split("/")[-1])
                    else:
                        lobbyist["ls_end_yr"] = 2018
                    LDE_INSERT += insert_obj(dddb, lobbyist, QS_LOBBYISTDIRECTEMPLOYMENT, QI_LOBBYISTDIRECTEMPLOYMENT, "Lobbyist Direct Employment", logger)


def insert_lobbyist_db(dddb, lobbyist):
    global L_INSERT
    global P_INSERT

    dddb.execute(QS_LOBBYIST, lobbyist)
    if dddb.rowcount == 0:
        dddb.execute(QS_PERSON, lobbyist)
        if dddb.rowcount == 0:
            try:
                dddb.execute(QI_PERSON, lobbyist)
                pid = dddb.lastrowid
                lobbyist['pid'] = pid
                dddb.execute(QI_PERSONSTATE, lobbyist)
                P_INSERT += dddb.rowcount
            except MySQLdb.Error:
                logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                                                additional_fields=create_payload('Person', 
                                                (QI_PERSON%lobbyist)))
        else:
            pid = dddb.fetchone()[0]
        lobbyist['pid'] = pid
        try:
            dddb.execute(QI_LOBBYIST, lobbyist)
            L_INSERT += dddb.rowcount
        except MySQLdb.Error:
            logger.warning('Insert Failed', full_msg=traceback.format_exc(),
                                            additional_fields=create_payload('Lobbyist', 
                                            (QI_LOBBYIST%lobbyist)))
    else:
        pid = dddb.fetchone()[0]
    
    return pid

def main():
    ddinfo = mysql_connection(sys.argv)

    with MySQLdb.connect(host=ddinfo['host'],
                         user=ddinfo['user'],
                         db=ddinfo['db'],
                         port=ddinfo['port'],
                         passwd=ddinfo['passwd'],
                         charset='utf8') as dddb:
        download_files()
        parse_lobbyist(dddb, TODAYS_LEG_FILE, LEG_LOBBYIST_URL)
        parse_lobbyist(dddb, TODAYS_EXEC_FILE, EXEC_LOBBYIST_URL)
        logger.info(__file__ + ' terminated successfully.', 
                    full_msg='Inserted ' + str(P_INSERT) + ' rows in Person, inserted ' 
                        + str(L_INSERT) + ' rows in Lobbyist, inserted '
                        + str(O_INSERT) + ' rows in Organizations, inserted ' 
                        + str(LFS_INSERT) + ' rows in LobbyingFirmState, inserted '
                        + str(LF_INSERT) + ' rows in LobbyingFirm, inserted ' 
                        + str(LEMPLOYER_INSERT) + ' rows in LobbyistEmployer, inserted '
                        + str(LEMPLOYMENT_INSERT) + ' rows in LobbyistEmployment, inserted '
                        + str(LDE_INSERT) + ' rows in LobbyistDirectEmployment, and inserted ' 
                        + str(LC_INSERT) + ' rows in LobbyingContracts',
                    additional_fields={'_affected_rows':'Person:'+str(P_INSERT)+
                                            ', Lobbyist:'+str(L_INSERT)+
                                            ', Organizations:'+str(O_INSERT)+
                                            ', LobbyingFirmState:'+str(LFS_INSERT)+
                                            ', LobbyingFirm:'+str(LF_INSERT)+
                                            ', LobbyistEmployer:'+str(LEMPLOYER_INSERT)+
                                            ', LobbyistEmployment:'+str(LEMPLOYMENT_INSERT)+
                                            ', LobbyistDirectEmployment:'+str(LDE_INSERT)+
                                            ', LobbyingContracts:'+str(LC_INSERT),
                                        '_inserted':'Person:'+str(P_INSERT)+
                                            ', Lobbyist:'+str(L_INSERT)+
                                            ', Organizations:'+str(O_INSERT)+
                                            ', LobbyingFirmState:'+str(LFS_INSERT)+
                                            ', LobbyingFirm:'+str(LF_INSERT)+
                                            ', LobbyistEmployer:'+str(LEMPLOYER_INSERT)+
                                             ', LobbyistEmployment:'+str(LEMPLOYMENT_INSERT)+
                                            ', LobbyistDirectEmployment:'+str(LDE_INSERT)+
                                            ', LobbyingContracts:'+str(LC_INSERT),
                                        '_state':'FL'})
    
        LOG = {'tables': [{'state': 'FL', 'name': 'LobbingFirm', 'inserted':LF_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'LobbyingFirmState', 'inserted':LFS_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'Lobbyist', 'inserted':L_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'Person', 'inserted':P_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'Organizations', 'inserted':O_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'LobbyistEmployer', 'inserted':LEMPLOYER_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'LobbyistEmployment', 'inserted':LEMPLOYMENT_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'LobbyistDirectEmployment', 'inserted':LDE_INSERT, 'updated': 0, 'deleted': 0},
              {'state': 'FL', 'name': 'LobbyingContracts', 'inserted':LC_INSERT, 'updated': 0, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))

if __name__ == '__main__':
    with GrayLogger(GRAY_LOGGER_URL) as _logger:
        logger = _logger 
        main()

