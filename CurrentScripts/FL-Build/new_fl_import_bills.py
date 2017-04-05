#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
"""
File: new_fl_import_bills.py
Author: Andrew Rose
Date: 3/16/2017
Last Updated: 3/16/2017

Description:
    - This file gets OpenStates bill data using the API Helper and inserts it into the database

Source:
    - OpenStates API

Populates:
    - Bill

"""

import MySQLdb
import traceback
import sys
import datetime as dt
from time import strftime
from time import strptime
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
                    (vid, bid, billState, subject, text, state)
                    VALUES
                    (%(vid)s, %(bid)s, %(name)s, %(subject)s, %(doc)s, %(state)s)'''


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'CA',
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


# This doesn't work - need to figure out if possible with our committee structure
def get_vote_cid(vote, dddb):
    committee = vote['motion'].split('(')

    if len(committee) < 2:
        return None

    committee = committee[1].strip(')')
    committee = committee.replace("Committee", "", 1).strip()

    try:
        comm_info = {'name': committee, 'house': vote['house'], 'state': 'FL', 'session': vote['session']}
        dddb.execute(SELECT_COMMITTEE, comm_info)

        if dddb.rowcount == 0:
            print("Error: Committee selection failed")
            return None
        else:
            return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Committee selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Committee", (SELECT_COMMITTEE % comm_info)))


def get_pid(vote, dddb):
    alt_id = {'alt_id': vote['leg_id']}

    try:
        dddb.execute(SELECT_PID, alt_id)

        if dddb.rowcount == 0:
            print "Error: Person not found"
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("AltId", (SELECT_PID % alt_id)))


def get_last_mid(dddb):
    try:
        dddb.execute(SELECT_LAST_MID)

        return dddb.fetchone()[0]
    except MySQLdb.Error:
        logger.warning("Motion selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Motion", (SELECT_LAST_MID)))


def import_votes(bid, vote_list, dddb):
    global M_INSERTED, BVS_INSERTED, BVD_INSERTED

    old_vote_date = None

    for vote in vote_list:
        vote['date'] = dt.datetime.strptime(vote['date'], '%Y-%m-%d %H:%M:%S').date()

        if old_vote_date is None:
            vote_seq = 1
            old_vote_date = vote['date']
        else:
            new_vote_date = vote['date']

            if new_vote_date == old_vote_date:
                vote_seq += 1
            elif new_vote_date != old_vote_date:
                vote_seq = 1
                old_vote_date = new_vote_date

        vote['bid'] = bid
        vote['vote_seq'] = vote_seq
        vote['date'] = str(vote['date'])

        # Currently not sure how to reliably get cid
        #vote['cid'] = get_vote_cid(vote, dddb)

        if vote['passed'] == 1:
            vote['result'] = '(PASS)'
        else:
            vote['result'] = '(FAIL)'

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

        #print(vote)

        # Need to check if BVD is in database
        for aye_vote in vote['aye_votes']:
            aye_vote['voteRes'] = 'AYE'
            aye_vote['voteId'] = vote['vid']
            aye_vote['pid'] = get_pid(aye_vote, dddb)
            aye_vote['state'] = 'FL'

            #print(aye_vote)
            if not is_bvd_in_db(aye_vote, dddb):
                try:
                    dddb.execute(INSERT_BVD, aye_vote)
                    BVD_INSERTED += dddb.rowcount
                except MySQLdb.Error:
                    logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % aye_vote)))

        for nae_vote in vote['nae_votes']:
            nae_vote['voteRes'] = 'NOE'
            nae_vote['voteId'] = vote['vid']
            nae_vote['pid'] = get_pid(nae_vote, dddb)
            nae_vote['state'] = 'FL'

            #print(nae_vote)
            if not is_bvd_in_db(nae_vote, dddb):
                try:
                    dddb.execute(INSERT_BVD, nae_vote)
                    BVD_INSERTED += dddb.rowcount
                except MySQLdb.Error:
                    logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % nae_vote)))

        for other_vote in vote['other_votes']:
            other_vote['voteRes'] = 'ABS'
            other_vote['voteId'] = vote['vid']
            other_vote['pid'] = get_pid(other_vote, dddb)
            other_vote['state'] = 'FL'

            #print(other_vote)
            if not is_bvd_in_db(other_vote, dddb):
                try:
                    dddb.execute(INSERT_BVD, other_vote)
                    BVD_INSERTED += dddb.rowcount
                except MySQLdb.Error:
                    logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % other_vote)))


def import_actions(bid, actions, dddb):
    global A_INSERTED

    old_action_date = None

    for action in actions:
        action['date'] = dt.datetime.strptime(action['date'], '%Y-%m-%d %H:%M:%S').date()

        if old_action_date is None:
            action_seq = 1
            old_action_date = action['date']
        else:
            new_action_date = action['date']

            if new_action_date == old_action_date:
                action_seq += 1
            elif new_action_date != old_action_date:
                action_seq = 1
                old_action_date = new_action_date

        action['bid'] = bid
        action['seq_num'] = action_seq
        action['date'] = str(action['date'])

        #print(action)

        if not is_action_in_db(action, dddb):
            try:
                dddb.execute(INSERT_ACTION, action)
                A_INSERTED += dddb.rowcount
            except MySQLdb.Error:
                logger.warning("Action insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("Action", (INSERT_ACTION % action)))


def import_versions(bill, versions, dddb):
    global V_INSERTED

    for version in versions:
        version['bid'] = bill['bid']
        version['vid'] = version['bid'] + version['name'].split(' ')[-1]

        version['subject'] = bill['title']
        version['state'] = 'FL'

        #print(version)

        if not is_version_in_db(version, dddb):
            try:
                dddb.execute(INSERT_VERSION, version)
                V_INSERTED += dddb.rowcount
            except MySQLdb.Error:
                logger.warning("BillVersion insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("BillVersion", (INSERT_VERSION % version)))


'''
IMPORTANT NOTE:
     - OS bill title is the subject field for the BillVersion table
     - BillText is stored as a PDF for Florida, just include a link
     - BillState and Status aren't in OS, just leave blank for now
'''
def import_bills(dddb):
    global B_INSERTED

    bill_list = get_bills('FL')

    for bill in bill_list:
        bill["bid"] = "FL_" + bill["session_year"] + str(bill["session"]) + bill["type"] + bill["number"]
        print(bill['bid'])
        # Placeholder for billState until we get data - not needed for transcription
        bill['billState'] = 'TBD'
        #print(bill)

        if not is_bill_in_db(dddb, bill):
            try:
                dddb.execute(INSERT_BILL, bill)
                B_INSERTED += dddb.rowcount

            except MySQLdb.Error:
                logger.warning("Bill insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("Bill", (INSERT_BILL % bill)))

        bill_details = get_bill_details(bill['os_bid'], 'FL')

        import_votes(bill['bid'], bill_details['votes'], dddb)
        import_actions(bill['bid'], bill_details['actions'], dddb)
        import_versions(bill, bill_details['versions'], dddb)


def main():
    dbinfo = mysql_connection(sys.argv)
    with MySQLdb.connect(host=dbinfo['host'],
                         port=dbinfo['port'],
                         db=dbinfo['db'],
                         user=dbinfo['user'],
                         passwd=dbinfo['passwd'],
                         charset='utf8') as dddb:
        import_bills(dddb)

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
                                       '_state': 'FL'})


if __name__ == "__main__":
    with GrayLogger(API_URL) as _logger:
        logger = _logger
        main()
