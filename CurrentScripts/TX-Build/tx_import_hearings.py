#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_import_hearings.ppy
Author: Andrew Rose
Date: 5/8/2017
Last Updated: 5/8/2017

Description:
    - This file gets TX hearing data from OpenStates using the API helper
      inserts it into the database

Source:
    - OpenStates API

Populates:
    - Hearing (date, type, session_year, state)
    - CommitteeHearing (cid, hid)
    - HearingAgenda (hid, bid, date_created, current_flag)
"""

import MySQLdb
import sys
import traceback
from graylogger.graylogger import GrayLogger
from Database_Connection import mysql_connection
from events_API_helper import *
from Constants.Hearings_Queries import *
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import *

logger = None

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated


'''
Selects a committee's CID from the database
using the committee name associated with a hearing
'''
def get_comm_cid(committee, dddb):
    try:
        dddb.execute(SELECT_COMMITTEE, committee)

        if dddb.rowcount != 1:
            print('Problem selecting ' + committee['house'] + ' ' + committee['comm'] + ' committee')
        else:
            cid = dddb.fetchone()[0]
            return cid

    except MySQLdb.Error:
        logger.warning("Select statement failed.", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Committee', (SELECT_COMMITTEE % committee)))


'''
Gets a bill's BID from the database using the bill's type and number
'''
def get_bill_bid(bill, dddb):
    try:
        dddb.execute(SELECT_BILL, bill)

        if dddb.rowcount != 1:
            print('Problem selecting bill ' + bill['type'] + ' ' + bill['number'])
        else:
            bid = dddb.fetchone()[0]
            return bid

    except MySQLdb:
        logger.warning("Select statement failed.", full_msg=traceback.format_exc(),
                       additional_fields=create_payload('Bill', (SELECT_BILL % bill)))


'''
Inserts CommitteeHearings into the DB
'''
def insert_committee_hearings(committees, hid, dddb):
    global CH_INS

    for committee in committees:
        cid = get_comm_cid(committee, dddb)
        comm_hearing = {'hid': hid, 'cid': cid}

        if cid is not None:
            try:
                dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
                CH_INS += dddb.rowcount

            except MySQLdb.Error:
                logger.warning('Insert statement failed', full_msg=traceback.format_exc(),
                               additional_fields=create_payload('CommitteeHearing', (INSERT_COMMITTEE_HEARING % comm_hearing)))


'''
Inserts HearingAgendas into the DB
'''
def insert_hearing_agendas(bills, hid, dddb):
    global HA_INS

    for bill in bills:
        bid = get_bill_bid(bill, dddb)
        hearing_agenda = {'hid': hid, 'bid': bid}

        if bid is not None:
            try:
                dddb.execute(INSERT_HEARING_AGENDA, hearing_agenda)
                HA_INS += dddb.rowcount

            except MySQLdb.Error:
                logger.warning('Insert statement failed', full_msg=traceback.format_exc(),
                               additional_fields=create_payload('HearingAgenda', (INSERT_HEARING_AGENDA % hearing_agenda)))


'''
Gets hearing data from OpenStates and inserts it into the database
Once a Hearing has been inserted, this function also inserts
the corresponding CommitteeHearings and HearingAgendas.
'''
def import_hearings(dddb):
    global H_INS
    hearings = get_event_list('TX')

    for hearing in hearings:
        try:
            ins_hearing = {'date': hearing['date'], 'state': hearing['state'],
                           'type': hearing['type'], 'session_year': hearing['session_year']}
            dddb.execute(INSERT_HEARING, ins_hearing)
            H_INS += dddb.rowcount
            hid = dddb.lastrowid

        except MySQLdb.Error:
            logger.warning('Insert statement failed', full_msg=traceback.format_exc(),
                           additional_fields=create_payload('Hearing', (INSERT_HEARING % hearing)))

        insert_committee_hearings(hearing['committees'], hid, dddb)
        insert_hearing_agendas(hearing['bills'], hid, dddb)


def main():
    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:

        import_hearings(dddb)

        logger.info(__file__ + " terminated successfully",
                    full_msg="Inserted " + str(H_INS) + " rows in Hearing, "
                             + str(CH_INS) + " rows in CommitteeHearing, "
                             + str(HA_INS) + " rows in HearingAgenda, and updated "
                             + str(HA_UPD) + " rows in HearingAgenda",
                    additional_fields={'_affected_rows': 'Hearing: ' + str(H_INS)
                                                         + ', CommitteeHearing: ' + str(CH_INS)
                                                         + ', HearingAgenda: ' + str(HA_INS + HA_UPD),
                                       '_inserted': 'Hearing: ' + str(H_INS)
                                                    + ', CommitteeHearing: ' + str(CH_INS)
                                                    + ', HearingAgenda: ' + str(HA_INS),
                                       '_updated': 'HearingAgenda: ' + str(HA_UPD),
                                       '_state': 'TX'})

        LOG = {'tables': [{'state': 'FL', 'name': 'Hearing', 'inserted': H_INS, 'updated': 0, 'deleted': 0},
                          {'state': 'FL', 'name': 'CommitteeHearing', 'inserted': CH_INS, 'updated': 0, 'deleted': 0},
                          {'state': 'FL', 'name': 'HearingAgenda', 'inserted': HA_INS, 'updated': HA_UPD,
                           'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    with GrayLogger(GRAY_LOGGER_URL) as _logger:
        logger = _logger
        main()