#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: new_fl_import_committee.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 4/28/2017

Description:
    -This file gets OpenStates committee data using the API Helper and inserts it into the database

Source:
    -OpenStates API

Populates:
    -CommitteeNames (name, house, state)
    -Committee (name, short_name, type, state, house, session_year)
    -ServesOn
"""

import MySQLdb
import sys
import traceback
import datetime as dt
from time import strftime
from time import strptime
from graylogger.graylogger import GrayLogger
from committee_API_helper import *
from Database_Connection import mysql_connection

API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

# Global counters
CN_INSERTED = 0
C_INSERTED = 0
SO_INSERTED = 0
SO_UPDATED = 0

# SQL Selects
SELECT_SESSION_YEAR = '''SELECT max(start_year) FROM Session
                         WHERE state = 'FL'
                         '''

SELECT_COMMITTEE_NAME = '''SELECT * FROM CommitteeNames
                           WHERE name = %(name)s
                           AND house = %(house)s
                           AND state = %(state)s
                           '''

SELECT_COMMITTEE = '''SELECT cid FROM Committee
                      WHERE state = %(state)s
                      AND name = %(name)s
                      AND house = %(house)s
                      AND session_year = %(session_year)s
                      '''

SELECT_PID = '''SELECT pid FROM AlternateId
                WHERE alt_id = %(alt_id)s'''

SELECT_SERVES_ON = '''SELECT * FROM servesOn
                      WHERE pid = %(pid)s
                      AND year = %(session_year)s
                      AND house = %(house)s
                      AND cid = %(cid)s
                      AND state = %(state)s'''

SELECT_COMMITTEE_MEMBERS = '''SELECT pid FROM servesOn
                            WHERE house = %(house)s
                            AND cid = %(cid)s
                            AND state = %(state)s
                            AND current_flag = true
                            AND year = %(year)s'''

# SQL Inserts
INSERT_COMMITTEE_NAME = '''INSERT INTO CommitteeNames
                           (name, house, state)
                           VALUES
                           (%(name)s, %(house)s, %(state)s)'''

INSERT_COMMITTEE = '''INSERT INTO Committee
                      (name, short_name, type, state, house, session_year)
                      VALUES
                      (%(name)s, %(short_name)s, %(type)s, %(state)s, %(house)s, %(session_year)s)'''

INSERT_SERVES_ON = '''INSERT INTO servesOn
                      (pid, year, house, cid, state, current_flag, start_date, position)
                      VALUES
                      (%(pid)s, %(session_year)s, %(house)s, %(cid)s, %(state)s, 1, %(start_date)s, %(position)s)'''

# SQL Updates
UPDATE_SERVESON = '''UPDATE servesOn
                     SET current_flag = %(current_flag)s, end_date = %(end_date)s
                     WHERE pid = %(pid)s
                     AND cid = %(cid)s
                     AND house = %(house)s
                     AND year = %(year)s
                     AND state = %(state)s'''


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'FL',
        '_log_type': 'Database'
    }


def is_comm_name_in_db(dddb, committee):
    comm_name = {'name': committee['name'], 'house': committee['house'], 'state': committee['state']}

    try:
        dddb.execute(SELECT_COMMITTEE_NAME, comm_name)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("CommitteeName selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("CommitteeNames", (SELECT_COMMITTEE_NAME % comm_name)))


def is_servesOn_in_db(dddb, member):
    try:
        dddb.execute(SELECT_SERVES_ON, member)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("servesOn selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("servesOn", (SELECT_SERVES_ON % member)))


def get_comm_cid(dddb, committee):
    comm = {'state': committee['state'], 'name': committee['name'],
            'house': committee['house'], 'session_year': committee['session_year']}

    try:
        dddb.execute(SELECT_COMMITTEE, comm)

        if dddb.rowcount == 0:
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("Committee selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Committee", (SELECT_COMMITTEE % comm)))


def get_session_year(dddb):
    try:
        dddb.execute(SELECT_SESSION_YEAR)

        return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Session selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Session", SELECT_SESSION_YEAR))


def get_pid(dddb, member):
    alt_id = {'alt_id': member['leg_id']}

    try:
        dddb.execute(SELECT_PID, alt_id)

        if dddb.rowcount == 0:
            print("Error: Person not found with Alt ID " + str(alt_id['alt_id']))
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("AltId", (SELECT_PID % alt_id)))


def is_committee_current(updated):
    update_date = dt.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')

    diff = dt.datetime.now() - update_date

    if diff.days > 7:
        return False
    else:
        return True


def get_past_members(dddb, committee):
    update_members = list()

    try:
        comm = {'cid': committee['cid'], 'house': committee['house'],
                'state': committee['state'], 'year': committee['session_year']}
        dddb.execute(SELECT_COMMITTEE_MEMBERS, comm)

        query = dddb.fetchall()

        for member in query:
            isCurrent = False

            for commMember in committee['members']:
                if member[0] == commMember['pid']:
                    isCurrent = True
                    break

            if isCurrent is False:
                mem = dict()
                mem['current_flag'] = 0
                mem['end_date'] = dt.datetime.today().strftime("%Y-%m-%d")
                mem['pid'] = member[0]
                mem['cid'] = committee['cid']
                mem['house'] = committee['house']
                mem['year'] = committee['session_year']
                mem['state'] = committee['state']
                update_members.append(mem)

    except MySQLdb.Error:
        logger.warning("servesOn selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("servesOn", (SELECT_COMMITTEE_MEMBERS % comm)))

    return update_members


def import_committees(dddb):
    global C_INSERTED, CN_INSERTED, SO_INSERTED, SO_UPDATED

    comm_list = get_committee_list('fl')

    for committee in comm_list:
        # Committees that have not been updated in the past week are not current
        if is_committee_current(committee['updated']):
            committee['session_year'] = committee['updated'][:4]
            committee['members'] = get_committee_membership(committee['comm_id'])

            if is_comm_name_in_db(dddb, committee) is False:
                try:
                    comm_name = {'name': committee['name'], 'house': committee['house'], 'state': committee['state']}
                    dddb.execute(INSERT_COMMITTEE_NAME, comm_name)
                    CN_INSERTED += dddb.rowcount

                except MySQLdb.Error:
                    logger.warning("CommitteeName insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("CommitteeNames",
                                                                    (INSERT_COMMITTEE_NAME % comm_name)))

            committee['cid'] = get_comm_cid(dddb, committee)

            if committee['cid'] is None:
                try:
                    comm = {'name': committee['name'], 'short_name': committee['short_name'],
                            'type': committee['type'], 'state': committee['state'],
                            'house': committee['house'], 'session_year': committee['session_year']}

                    dddb.execute(INSERT_COMMITTEE, comm)
                    committee['cid'] = int(dddb.lastrowid)
                    C_INSERTED += dddb.rowcount

                except MySQLdb.Error:
                    logger.warning("Committee insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("Committee", (INSERT_COMMITTEE % comm)))

            if len(committee['members']) > 0:
                for member in committee['members']:
                    member['cid'] = committee['cid']
                    member['pid'] = get_pid(dddb, member)
                    member['state'] = committee['state']
                    member['house'] = committee['house']
                    member['session_year'] = committee['session_year']
                    member['start_date'] = dt.datetime.today().strftime("%Y-%m-%d")

                    if member['pid'] is not None:
                        if not is_servesOn_in_db(dddb, member):
                            try:
                                dddb.execute(INSERT_SERVES_ON, member)
                                SO_INSERTED += dddb.rowcount

                            except MySQLdb.Error:
                                logger.warning("servesOn insertion failed", full_msg=traceback.format_exc(),
                                               additional_fields=create_payload("servesOn",
                                                                                (INSERT_SERVES_ON % member)))

                update_mems = get_past_members(dddb, committee)

                if len(update_mems) > 0:
                    for member in update_mems:
                        try:
                            dddb.execute(UPDATE_SERVESON, member)
                            SO_UPDATED += dddb.rowcount

                        except MySQLdb.Error:
                            logger.warning("servesOn update failed", full_msg=traceback.format_exc(),
                                           additional_fields=create_payload("servesOn", (UPDATE_SERVESON % member)))


def main():
    import sys
    dbinfo = mysql_connection(sys.argv)
    # MUST SPECIFY charset='utf8' OR BAD THINGS WILL HAPPEN.
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:

        import_committees(dddb)

        logger.info(__file__ + " terminated successfully",
                    full_msg="Inserted " + str(CN_INSERTED) + " rows in CommitteeNames, "
                             + str(C_INSERTED) + " rows in Committee, and"
                             + str(SO_INSERTED) + " rows in servesOn.",
                    additional_fields={'_affected_rows': 'CommitteeNames: ' + str(CN_INSERTED)
                                                         + ', Committee: ' + str(C_INSERTED)
                                                         + ', servesOn: ' + str(SO_INSERTED),
                                       '_inserted': 'CommitteeNames: ' + str(CN_INSERTED)
                                                    + ', Committee: ' + str(C_INSERTED)
                                                    + ', servesOn: ' + str(SO_INSERTED),
                                       '_updated': 'servesOn: ' + str(SO_UPDATED),
                                       '_state': 'FL'})


if __name__ == '__main__':
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()
