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

API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated

# SQL Selects
SELECT_COMMITTEE = '''SELECT cid FROM Committee
                      where short_name = %(comm)s
                      and house = %(house)s
                      and state = %(state)s'''

SELECT_BILL = '''SELECT bid FROM Bill
                 where state = %(state)s
                 and sessionYear = %(session_year)s
                 and type = %(type)s
                 and number = %(number)s'''

# SQL Inserts
INSERT_HEARING = '''INSERT INTO Hearing (date, state, type, session_year)
                    VALUES (%(date)s, %(state)s, %(type)s, %(session_year)s)'''

INSERT_COMM_HEARING = '''INSERT INTO CommitteeHearings (hid, cid)
                         VALUES (%(hid)s, %(cid)s)'''

INSERT_HEARING_AGENDA = '''INSERT INTO HearingAgenda (hid, bid, date_created, current_flag)
                           VALUES (%(hid)s, %(bid)s, %(date_created)s, %(current_flag)s)'''


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'TX',
        '_log_type': 'Database'
    }


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


def insert_committee_hearings(committees, hid, dddb):
    global CH_INS

    for committee in committees:
        cid = get_comm_cid(committee, dddb)
        comm_hearing = {'hid': hid, 'cid': cid}

        if cid is not None:
            try:
                dddb.execute(INSERT_COMM_HEARING, comm_hearing)
                CH_INS += dddb.rowcount

            except MySQLdb.Error:
                logger.warning('Insert statement failed', full_msg=traceback.format_exc(),
                               additional_fields=create_payload('CommitteeHearing', (INSERT_COMM_HEARING % comm_hearing)))


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


if __name__ == '__main__':
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()