#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
"""
File: fl_import_bills.py
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
                (bid, mid, cid, VoteDate, ayes, naes, abstain, result, VoteDateSeq)
                VALUES
                (%(bid)s, %(mid)s, %(cid)s, %(date)s, %(ayes)s, %(naes)s, %(other)s, %(result)s, %(vote_seq)s)'''

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
                    (%(vid)s, %(bid)s, %(date)s, %(name)s, %(subject)s, %(doc)s, %(state)s)'''


def create_payload(table, sqlstmt):
    return {
        '_table': table,
        '_sqlstmt': sqlstmt,
        '_state': 'FL',
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
    vote_info = {'bid': vote['bid'], 'mid': vote['mid'],
                 'date': vote['date'], 'vote_seq': vote['vote_seq']}

    try:
        dddb.execute(SELECT_VOTE, vote_info)

        if dddb.rowcount == 0:
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("BillVoteSummary selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("BillVoteSummary", (SELECT_VOTE % vote_info)))


def get_vote_cid(vote, dddb):
    comm_info = dict()

    comm_info['house'] = vote['house']
    comm_info['session'] = vote['session']
    comm_info['state'] = 'FL'

    committee = vote['motion'].split('(')

    if len(committee) < 2:
        return None

    committee = committee[1].strip(')')

    if "Subcommittee" in committee:
        comm_info['type'] = "Subcommittee"
        committee = committee.replace("Subcommittee", "", 1).strip()
    elif "Select" in committee:
        comm_info['type'] = "Select"
        committee = committee.replace("Committee", "", 1).strip()
    else:
        comm_info['type'] = "Standing"
        committee = committee.replace("Committee", "", 1).strip()

    comm_info['name'] = committee

    try:
        dddb.execute(SELECT_COMMITTEE, comm_info)

        if dddb.rowcount == 0:
            print("Error - Committee selection failed: " + comm_info['name'])
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
            print("Error: Person with alt ID " + str(alt_id) + " not found")
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
                       additional_fields=create_payload("Motion", SELECT_LAST_MID))


def scrape_version_date(url):
    dates = dict()

    url = url.split('/')
    url = '/'.join(url[:7])

    try:
        html_soup = BeautifulSoup(urllib2.urlopen(url), 'lxml')
    except:
        print("Error connecting to " + url)
        return dates

    table = html_soup.find('div', id='tabBodyBillText').find('table', class_='tbl')

    for row in table.find_all('td', class_='lefttext'):
        billstate = row.contents[0]

        date_col = row.find_next_sibling('td', class_='centertext').contents[0]
        date_col = date_col.split(' ')[0].split('/')
        date = date_col[2] + '/' + date_col[0] + '/' + date_col[1]

        dates[billstate] = date

    return dates


def import_votes(vote_list, dddb):
    global M_INSERTED, BVS_INSERTED, BVD_INSERTED

    for vote in vote_list:
        vote['cid'] = get_vote_cid(vote, dddb)

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
            vote_info = {'bid': vote['bid'], 'mid': vote['mid'], 'cid': vote['cid'], 'date': vote['date'],
                         'ayes': vote['ayes'], 'naes': vote['naes'], 'other': vote['other'], 'result': vote['result'],
                         'vote_seq': vote['vote_seq']}

            try:
                dddb.execute(INSERT_BVS, vote_info)
                BVS_INSERTED += dddb.rowcount

                vote['vid'] = dddb.lastrowid
            except MySQLdb.Error:
                logger.warning("BillVoteSummary insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("BillVoteSummary", (INSERT_BVS % vote_info)))


        for aye_vote in vote['aye_votes']:
            aye_vote['voteRes'] = 'AYE'
            aye_vote['voteId'] = vote['vid']
            aye_vote['pid'] = get_pid(aye_vote, dddb)
            aye_vote['state'] = 'FL'

            if pid is not None and not is_bvd_in_db(aye_vote, dddb):
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

            if not is_bvd_in_db(other_vote, dddb):
                try:
                    dddb.execute(INSERT_BVD, other_vote)
                    BVD_INSERTED += dddb.rowcount
                except MySQLdb.Error:
                    logger.warning("BillVoteDetail insertion failed", full_msg=traceback.format_exc(),
                                   additional_fields=create_payload("BillVoteDetail", (INSERT_BVD % other_vote)))


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


def import_versions(bill_title, versions, dddb):
    global V_INSERTED

    ver_dates = scrape_version_date(versions[0]['doc'])

    for version in versions:
        version['subject'] = bill_title

        try:
            version['date'] = ver_dates[version['name']]
        except:
            print("Error getting version date for bill " + version['bid'])

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
        print(bill['bid'])

        if not is_bill_in_db(dddb, bill):
            try:
                dddb.execute(INSERT_BILL, bill)
                B_INSERTED += dddb.rowcount

            except MySQLdb.Error:
                logger.warning("Bill insertion failed", full_msg=traceback.format_exc(),
                               additional_fields=create_payload("Bill", (INSERT_BILL % bill)))

        bill_details = get_bill_details(bill['os_bid'], bill['bid'], 'FL')

        import_votes(bill_details['votes'], dddb)
        import_actions(bill_details['actions'], dddb)
        import_versions(bill['title'], bill_details['versions'], dddb)


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
