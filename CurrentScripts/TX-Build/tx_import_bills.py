#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: tx_import_bills.py
Author: Andrew Rose
Date: 3/16/2017
Last Updated: 5/4/2017

Description:
    - This file gets OpenStates bill data using the API Helper and inserts it into the database

Source:
    - OpenStates API

Populates:
    - Bill
    - Motion
    - BillVoteSummary
    - BillVoteDetail
    - Action
    - BillVersion
"""

import MySQLdb
import traceback
import sys
import urllib2
from bs4 import BeautifulSoup
from graylogger.graylogger import GrayLogger
from Database_Connection import mysql_connection
from bill_API_helper import *

API_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

# Global Counters
B_INSERTED = 0
M_INSERTED = 0
BVS_INSERTED = 0
BVD_INSERTED = 0
A_INSERTED = 0
V_INSERTED = 0

# SQL Selects
SELECT_BILL = '''SELECT * FROM Bill
                 WHERE bid = %(bid)s'''

SELECT_MOTION = '''SELECT mid FROM Motion
                   WHERE text = %(motion)s
                   AND doPass = %(doPass)s'''

SELECT_LAST_MID = '''SELECT MAX(mid) FROM Motion'''

SELECT_VOTE = '''SELECT VoteId FROM BillVoteSummary
                 WHERE bid = %(bid)s
                 AND mid = %(mid)s
                 AND VoteDate = %(date)s
                 AND VoteDateSeq = %(vote_seq)s'''

SELECT_BVD = '''SELECT * FROM BillVoteDetail
                WHERE pid = %(pid)s
                AND voteId = %(voteId)s'''

SELECT_COMMITTEE = '''SELECT cid FROM Committee
                      WHERE short_name = %(name)s
                      AND house = %(house)s
                      AND state = %(state)s
                      AND session_year = %(session)s'''

SELECT_PID = '''SELECT pid FROM AlternateId
                WHERE alt_id = %(alt_id)s'''

SELECT_PID_NAME = '''SELECT * FROM Person p
                JOIN Term t ON p.pid = t.pid
                WHERE t.state = 'TX'
                AND t.current_term = 1
                AND p.last LIKE %(last)s
                '''

SELECT_ACTION = '''SELECT * FROM Action
                   WHERE bid = %(bid)s
                   AND date = %(date)s
                   AND seq_num = %(seq_num)s'''

SELECT_VERSION = '''SELECT * FROM BillVersion
                    WHERE vid = %(vid)s'''

# SQL Inserts
INSERT_BILL = '''INSERT INTO Bill
                 (bid, type, number, billState, house, session, sessionYear, state)
                 VALUES
                 (%(bid)s, %(type)s, %(number)s, %(billState)s, %(house)s, %(session)s,
                 %(session_year)s, %(state)s)'''

INSERT_MOTION = '''INSERT INTO Motion
                   (mid, text, doPass)
                   VALUES
                   (%(mid)s, %(text)s, %(pass)s)'''

INSERT_BVS = '''INSERT INTO BillVoteSummary
                (bid, mid, VoteDate, ayes, naes, abstain, result, VoteDateSeq)
                VALUES
                (%(bid)s, %(mid)s, %(date)s, %(ayes)s, %(naes)s, %(other)s, %(result)s, %(vote_seq)s)'''

INSERT_BVD = '''INSERT INTO BillVoteDetail
                (pid, voteId, result, state)
                VALUES
                (%(pid)s, %(voteId)s, %(voteRes)s, %(state)s)'''

INSERT_ACTION = '''INSERT INTO Action
                   (bid, date, text, seq_num)
                   VALUES
                   (%(bid)s, %(date)s, %(text)s, %(seq_num)s)'''

INSERT_VERSION = '''INSERT INTO BillVersion
                    (vid, bid, date, billState, subject, text, state)
                    VALUES
                    (%(vid)s, %(bid)s, %(date)s, %(name)s, %(subject)s, %(text)s, %(state)s)'''


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'TX',
        '_log_type': 'Database'
    }


def is_bill_in_db(dddb, bill):
    try:
        dddb.execute(SELECT_BILL, bill)

        if dddb.rowcount == 0:
            return False
        else:
            return True
    except MySQLdb.Error:
        logger.warning("Bill selection failed", full_msg= traceback.format_exc(),
                       additional_fields=create_payload("Bill", (SELECT_BILL % bill)))


def is_bvd_in_db(bvd, dddb):
    try:
        dddb.execute(SELECT_BVD, bvd)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("BillVoteDetail selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("BillVoteDetail", (SELECT_BVD % bvd)))


def is_action_in_db(action, dddb):
    try:
        dddb.execute(SELECT_ACTION, action)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("Action selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Action", (SELECT_ACTION % action)))


def is_version_in_db(version, dddb):
    try:
        dddb.execute(SELECT_VERSION, version)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        logger.warning("BillVersion selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("BillVersion", (SELECT_VERSION % version)))


'''
Each vote is associated with a motion.
If a motion already exists in the DB, use that motion's ID
'''
def get_motion_id(motion, passed, dddb):
    mot = {'motion': motion, 'doPass': passed}

    try:
        dddb.execute(SELECT_MOTION, mot)

        if dddb.rowcount == 0:
            return None
        else:
            return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Motion selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Motion", (SELECT_MOTION % mot)))


def get_vote_id(vote, dddb):
    vote_info = {'bid': vote['bid'], 'mid': vote['mid'], 'date': vote['date'], 'vote_seq': vote['vote_seq']}

    try:
        dddb.execute(SELECT_VOTE, vote_info)

        if dddb.rowcount == 0:
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("BillVoteSummary selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("BillVoteSummary", (SELECT_VOTE % vote_info)))


'''
Get a legislator's PID from the database
Because of how OpenStates formats votes, have to select PID by matching
legislator names
'''
def get_pid_name(vote_name, dddb):
    mem_name = vote_name.strip()
    legislator = {'last': '%' + mem_name + '%'}

    try:
        dddb.execute(SELECT_PID_NAME, legislator)

        if dddb.rowcount != 1:
            #print("Error: PID for " + mem_name + " not found")
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Person", (SELECT_PID_NAME % legislator)))


'''
Get a legislator's PID using their OpenStates LegID and the AlternateID table
'''
def get_pid(vote, dddb):
    alt_id = {'alt_id': vote['leg_id']}

    try:
        dddb.execute(SELECT_PID, alt_id)

        if dddb.rowcount == 0:
            #print("Error: Person not found with Alt ID " + str(alt_id['alt_id']) + ", checking member name")
            return get_pid_name(vote['name'], dddb)
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("AltId", (SELECT_PID % alt_id)))


'''
Motion IDs don't auto-increment for some reason,
so this function grabs the highest MID from the Motion table
'''
def get_last_mid(dddb):
    try:
        dddb.execute(SELECT_LAST_MID)

        return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Motion selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Motion", SELECT_LAST_MID))


'''
Inserts vote data into the BillVoteSummary and BillVoteDetail tables
'''
def import_votes(vote_list, dddb):
    global M_INSERTED, BVS_INSERTED, BVD_INSERTED

    for vote in vote_list:
        vote['mid'] = get_motion_id(vote['motion'], vote['passed'], dddb)

        if vote['mid'] is None:
            try:
                mid = get_last_mid(dddb)
                mid += 1

                motion = {'mid': mid, 'text': vote['motion'], 'pass': vote['passed']}

                dddb.execute(INSERT_MOTION, motion)
                M_INSERTED += dddb.rowcount

                vote['mid'] = mid
            except MySQLdb.Error:
                logger.warning("Motion insert failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("Motion", (INSERT_MOTION % motion)))

        vote['vid'] = get_vote_id(vote, dddb)
        if vote['vid'] is None:
            vote_info = {'bid': vote['bid'], 'mid': vote['mid'], 'date': vote['date'],
                         'ayes': vote['ayes'], 'naes': vote['naes'], 'other': vote['other'], 'result': vote['result'],
                         'vote_seq': vote['vote_seq']}

            try:
                dddb.execute(INSERT_BVS, vote_info)
                BVS_INSERTED += dddb.rowcount

                vote['vid'] = dddb.lastrowid
            except MySQLdb.Error:
                logger.warning("BillVoteSummary insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("BillVoteSummary", (INSERT_BVS % vote_info)))

        # This part of the function inserts votes to the BillVoteDetail table
        for aye_vote in vote['aye_votes']:
            if aye_vote['leg_id'] is None:
                aye_voters = aye_vote['name'].split(',')

                for voter_name in aye_voters:
                    voter = dict()
                    voter['voteRes'] = 'AYE'
                    voter['voteId'] = vote['vid']
                    voter['pid'] = get_pid_name(voter_name, dddb)
                    voter['state'] = 'TX'

                    if voter['pid'] is not None and not is_bvd_in_db(voter, dddb):
                        try:
                            dddb.execute(INSERT_BVD, voter)
                            BVD_INSERTED += dddb.rowcount
                        except MySQLdb.Error:
                            logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                           additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % voter)))
            else:
                voter = dict()
                voter['voteRes'] = 'AYE'
                voter['voteId'] = vote['vid']
                voter['pid'] = get_pid(aye_vote, dddb)
                voter['state'] = 'TX'

                if voter['pid'] is not None and not is_bvd_in_db(voter, dddb):
                    try:
                        dddb.execute(INSERT_BVD, voter)
                        BVD_INSERTED += dddb.rowcount
                    except MySQLdb.Error:
                        logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                       additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % voter)))

        for nae_vote in vote['nae_votes']:
            if nae_vote['leg_id'] is None:
                nae_voters = nae_vote['name'].split(',')

                for voter_name in nae_voters:
                    voter = dict()
                    voter['voteRes'] = 'NOE'
                    voter['voteId'] = vote['vid']
                    voter['pid'] = get_pid_name(voter_name, dddb)
                    voter['state'] = 'TX'

                    if voter['pid'] is not None and not is_bvd_in_db(voter, dddb):
                        try:
                            dddb.execute(INSERT_BVD, voter)
                            BVD_INSERTED += dddb.rowcount
                        except MySQLdb.Error:
                            logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                           additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % voter)))
            else:
                voter = dict()
                voter['voteRes'] = 'NOE'
                voter['voteId'] = vote['vid']
                voter['pid'] = get_pid(nae_vote, dddb)
                voter['state'] = 'TX'

                if voter['pid'] is not None and not is_bvd_in_db(voter, dddb):
                    try:
                        dddb.execute(INSERT_BVD, voter)
                        BVD_INSERTED += dddb.rowcount
                    except MySQLdb.Error:
                        logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                       additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % voter)))

        for other_vote in vote['other_votes']:
            if other_vote['leg_id'] is None:
                other_voters = other_vote['name'].split(',')

                for voter_name in other_voters:
                    voter = dict()
                    voter['voteRes'] = 'ABS'
                    voter['voteId'] = vote['vid']
                    voter['pid'] = get_pid_name(voter_name, dddb)
                    voter['state'] = 'TX'

                    if voter['pid'] is not None and not is_bvd_in_db(voter, dddb):
                        try:
                            dddb.execute(INSERT_BVD, voter)
                            BVD_INSERTED += dddb.rowcount
                        except MySQLdb.Error:
                            logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                           additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % voter)))
            else:
                voter = dict()
                voter['voteRes'] = 'ABS'
                voter['voteId'] = vote['vid']
                voter['pid'] = get_pid(other_vote, dddb)
                voter['state'] = 'TX'

                if voter['pid'] is not None and not is_bvd_in_db(voter, dddb):
                    try:
                        dddb.execute(INSERT_BVD, voter)
                        BVD_INSERTED += dddb.rowcount
                    except MySQLdb.Error:
                        logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                       additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % voter)))


'''
Inserts into the Action table
'''
def import_actions(actions, dddb):
    global A_INSERTED

    for action in actions:
        if not is_action_in_db(action, dddb):
            try:
                dddb.execute(INSERT_ACTION, action)
                A_INSERTED += dddb.rowcount
            except MySQLdb.Error:
                logger.warning("Action insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("Action", (INSERT_ACTION % action)))


'''
Inserts into the BillVersion table
'''
def import_versions(bill_title, versions, dddb):
    global V_INSERTED

    for version in versions:
        version['subject'] = bill_title

        if version['text'] is None:
            logger.warning("Bill text download failed", full_msg = "URL error with bill version " + version['vid'])
        else:
            if not is_version_in_db(version, dddb):
                try:
                    dddb.execute(INSERT_VERSION, version)
                    V_INSERTED += dddb.rowcount
                except MySQLdb.Error:
                    logger.warning("BillVersion insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("BillVersion", (INSERT_VERSION % version)))


def import_bills(dddb, chamber):
    global B_INSERTED

    bill_list = get_bills('TX', chamber)

    for bill in bill_list:
        #print(bill['bid'])

        if not is_bill_in_db(dddb, bill):
            try:
                dddb.execute(INSERT_BILL, bill)
                B_INSERTED += dddb.rowcount

            except MySQLdb.Error:
                logger.warning("Bill insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("Bill", (INSERT_BILL % bill)))

        bill_details = get_bill_details(bill['os_bid'], bill['bid'], 'TX')

        import_votes(bill_details['votes'], dddb)
        import_actions(bill_details['actions'], dddb)
        import_versions(bill['title'], bill_details['versions'], dddb)


def main():
    if len(sys.argv) < 2:
        print("Usage: python tx_import_bills.py (db) [chamber]")
        exit()

    chamber = sys.argv.pop()
    #print(chamber)

    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:

        import_bills(dddb, chamber)

        logger.info(__file__ + " terminated successfully",
                    full_msg="Inserted " + str(B_INSERTED) + " rows in Bill, "
                                + str(M_INSERTED) + " rows in Motion, "
                                + str(BVS_INSERTED) + " rows in BillVoteSummary, "
                                + str(BVD_INSERTED) + " rows in BillVoteDetail, "
                                + str(A_INSERTED) + " rows in Action, and "
                                + str(V_INSERTED) + " rows in BillVersion.",
                    additional_fields={'_affected_rows': 'Bill: ' + str(B_INSERTED)
                                                         + ', Motion: ' + str(M_INSERTED)
                                                         + ', BillVoteSummary: ' + str(BVS_INSERTED)
                                                         + ', BillVoteDetail: ' + str(BVD_INSERTED)
                                                         + ', Action: ' + str(A_INSERTED)
                                                         + ', BillVersion: ' + str(V_INSERTED),
                                       '_inserted': 'Bill: ' + str(B_INSERTED)
                                                    + ', Motion: ' + str(M_INSERTED)
                                                    + ', BillVoteSummary: ' + str(BVS_INSERTED)
                                                    + ', BillVoteDetail: ' + str(BVD_INSERTED)
                                                    + ', Action: ' + str(A_INSERTED)
                                                    + ', BillVersion: ' + str(V_INSERTED),
                                       '_state': 'TX'})

        LOG = {'tables': [{'state': 'TX', 'name': 'Bill', 'inserted': B_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Motion', 'inserted': M_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVoteSummary', 'inserted': BVS_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVoteDetail', 'inserted': BVD_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Action', 'inserted': A_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVersion', 'inserted': V_INSERTED, 'updated': 0, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == "__main__":
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()
