#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: new_tx_import_committee.py
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
                         WHERE state = 'TX'
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

SELECT_LEG_PID = '''SELECT * FROM Person p
                    JOIN Term t ON p.pid = t.pid
                    WHERE t.state = 'TX'
                    AND t.current_term = 1
                    AND p.first LIKE %(first)s
                    AND p.last LIKE %(last)s
                    '''

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

SELECT_HOUSE_MEMBERS = '''SELECT p.pid FROM Person p
                          JOIN Legislator l ON p.pid = l.pid
                          JOIN Term t ON l.pid = t.pid
                          WHERE l.state = 'TX'
                          AND t.year = %(year)s
                          AND t.house = %(house)s'''

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
        '_state': 'TX',
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


'''
OpenStates has incorrect ID numbers for some legislators.
If a legislator has an incorrect/missing ID, this function
gets their PID by matching their name
'''
def get_pid_name(dddb, member):
    mem_name = member['name'].split(' ')
    legislator = {'first': "%" + mem_name[0] + "%", 'last': "%" + mem_name[-1] + "%"}

    try:
        dddb.execute(SELECT_LEG_PID, legislator)

        if dddb.rowcount != 1:
            print("Error: PID for " + member['name'] + " not found")
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Person", (SELECT_LEG_PID % legislator)))


'''
Get a legislator's PID using their OpenStates LegID and the AlternateID table
'''
def get_pid(dddb, member):
    if member['leg_id'] is None:
        return get_pid_name(dddb, member)

    else:
        alt_id = {'alt_id': member['leg_id']}

        try:
            dddb.execute(SELECT_PID, alt_id)

            if dddb.rowcount == 0:
                print("Error: Person not found with Alt ID " + str(alt_id['alt_id']) + ", checking member name")
                return get_pid_name(dddb, member)
            else:
                return dddb.fetchone()[0]

        except MySQLdb.Error:
            logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                           additional_fields=create_payload("AltId", (SELECT_PID % alt_id)))


'''
Gets all members of a state legislative house
Used when inserting floor committee membership information
'''
def get_house_members(dddb, house_info):
    try:
        dddb.execute(SELECT_HOUSE_MEMBERS, house_info)

        if dddb.rowcount != 0:
            return dddb.fetchall()

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Person", (SELECT_HOUSE_MEMBERS % house_info)))


'''
Inserts to the CommitteeNames table
Names in the Committee table refer to the CommitteeNames table, so this must be done
before adding new committees
'''
def insert_comm_name(dddb, committee):
    global  CN_INSERTED

    try:
        comm_name = {'name': committee['name'], 'house': committee['house'], 'state': committee['state']}
        dddb.execute(INSERT_COMMITTEE_NAME, comm_name)
        CN_INSERTED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("CommitteeName insertion failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("CommitteeNames", (INSERT_COMMITTEE_NAME % comm_name)))


'''
Inserts to the Committee table
Returns the newly inserted committee's CID for inserting its members to servesOn
'''
def insert_committee(dddb, committee):
    global C_INSERTED

    cid = None

    try:
        comm = {'name': committee['name'], 'short_name': committee['short_name'],
                'type': committee['type'], 'state': committee['state'],
                'house': committee['house'], 'session_year': committee['session_year']}

        dddb.execute(INSERT_COMMITTEE, comm)
        cid = int(dddb.lastrowid)
        C_INSERTED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("Committee insertion failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Committee", (INSERT_COMMITTEE % comm)))

    return cid


'''
Inserts to the servesOn
'''
def insert_serves_on(dddb, member):
    global SO_INSERTED

    try:
        dddb.execute(INSERT_SERVES_ON, member)
        SO_INSERTED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("servesOn insertion failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("servesOn", (INSERT_SERVES_ON % member)))


'''
Updates rows in servesOn when a member is no longer part of a committee
'''
def update_serves_on(dddb, member):
    global SO_UPDATED

    try:
        dddb.execute(UPDATE_SERVESON, member)
        SO_UPDATED += dddb.rowcount

    except MySQLdb.Error:
        logger.warning("servesOn update failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("servesOn", (UPDATE_SERVESON % member)))


'''
Committees that OpenStates has updated in the past week
are defined as current in the database
'''
def is_committee_current(updated):
    update_date = dt.datetime.strptime(updated, '%Y-%m-%d %H:%M:%S')

    diff = dt.datetime.now() - update_date

    if diff.days > 7:
        return False
    else:
        return True


'''
If there is a committee member listed in our database
but not on OpenStates, that committee member is no longer current
and their end date is set to the first date where we noticed they
were gone from OpenStates
'''
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


'''
Takes data from the OpenStates API helper and inserts to the DB
'''
def import_committees(dddb):
    global C_INSERTED, CN_INSERTED, SO_INSERTED, SO_UPDATED

    comm_list = get_committee_list('tx')

    for committee in comm_list:
        # Committees that have not been updated in the past week are not current
        if is_committee_current(committee['updated']):

            committee['session_year'] = committee['updated'][:4]
            committee['members'] = get_committee_membership(committee['comm_id'])

            if is_comm_name_in_db(dddb, committee) is False:
                insert_comm_name(dddb, committee)

            committee['cid'] = get_comm_cid(dddb, committee)

            if committee['cid'] is None:
                committee['cid'] = insert_committee(dddb, committee)

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
                            insert_serves_on(dddb, member)

                update_mems = get_past_members(dddb, committee)

                if len(update_mems) > 0:
                    for member in update_mems:
                        update_serves_on(dddb, member)


'''
Both House and Senate have special floor committees
Each member of a legislative house belongs to their respective floor committee
'''
def insert_floor_committees(dddb):
    session_year = get_session_year(dddb)
    senate_floor = {'state': 'TX', 'session_year': session_year, 'type': 'Floor', 'current_flag': 1,
                    'house': 'Senate', 'name': 'Senate Floor', 'short_name': 'Senate Floor'}
    house_floor = {'state': 'TX', 'session_year': session_year, 'type': 'Floor', 'current_flag': 1,
                    'house': 'House', 'name': 'House Floor', 'short_name': 'House Floor'}

    for floor in [senate_floor, house_floor]:
        if is_comm_name_in_db(dddb, floor) is False:
            insert_comm_name(dddb, floor)

        if get_comm_cid(dddb, floor) is None:
            cid = insert_committee(dddb, floor)

        house_members = get_house_members(dddb, {'year': session_year, 'house': floor['house']})

        if house_members is not None:
            for house_member in house_members:
                member = dict()
                member['cid'] = cid
                member['pid'] = house_member
                member['position'] = 'Member'
                member['state'] = 'TX'
                member['house'] = floor['house']
                member['session_year'] = session_year
                member['start_date'] = dt.datetime.today().strftime("%Y-%m-%d")

                if not is_servesOn_in_db(dddb, member):
                    insert_serves_on(dddb, member)


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
        insert_floor_committees(dddb)

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
                                       '_state': 'TX'})

        LOG = {'tables': [{'state': 'TX', 'name': 'CommitteeNames', 'inserted': CN_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Committee', 'inserted': C_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'servesOn', 'inserted': SO_INSERTED, 'updated': SO_UPDATED, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()