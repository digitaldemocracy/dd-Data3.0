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
from Constants.Bills_Queries import *
from Constants.General_Constants import *
from Utils.DatabaseUtils_NR import *


GRAY_LOGGER_URL = 'http://dw.digitaldemocracy.org:12202/gelf'
logger = None

# Global Counters
B_INSERTED = 0
M_INSERTED = 0
BVS_INSERTED = 0
BVD_INSERTED = 0
A_INSERTED = 0
V_INSERTED = 0


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
    legislator = {'last': '%' + mem_name + '%', 'state': 'TX'}

    try:
        dddb.execute(SELECT_LEG_PID, legislator)

        if dddb.rowcount != 1:
            #print("Error: PID for " + mem_name + " not found")
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        logger.warning("PID selection failed", full_msg=traceback.format_exc(),
                       additional_fields=create_payload("Person", (SELECT_LEG_PID % legislator)))


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
            vote_info = {'bid': vote['bid'], 'mid': vote['mid'], 'cid': None, 'date': vote['date'],
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

        if not is_version_in_db(version, dddb):
            # Downloads bill text over an FTP link provided by OpenStates
            try:
                version_doc = urllib2.urlopen(version['url'], timeout=15)
                version['doc'] = ''
                while True:
                    read_text = version_doc.read(1024)
                    if not read_text:
                        break
                    version['doc'] += read_text
            except urllib2.URLError:
                version['doc'] = None
                print('URL error with version ' + version['vid'])

            try:
                dddb.execute(INSERT_VERSION, version)
                V_INSERTED += dddb.rowcount
            except MySQLdb.Error:
                logger.warning("BillVersion insertion failed", full_msg=traceback.format_exc(),
                                additional_fields=create_payload("BillVersion", (INSERT_VERSION % version)))


def import_bills(dddb):
    global B_INSERTED

    bill_list = get_bills('TX')

    for bill in bill_list:
        print(bill['bid'])

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
                                       '_state': 'TX'})

        LOG = {'tables': [{'state': 'TX', 'name': 'Bill', 'inserted': B_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Motion', 'inserted': M_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVoteSummary', 'inserted': BVS_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVoteDetail', 'inserted': BVD_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'Action', 'inserted': A_INSERTED, 'updated': 0, 'deleted': 0},
                          {'state': 'TX', 'name': 'BillVersion', 'inserted': V_INSERTED, 'updated': 0, 'deleted': 0}]}
        sys.stderr.write(json.dumps(LOG))


if __name__ == "__main__":
    with GrayLogger(GRAY_LOGGER_URL) as _logger:
        logger = _logger
        main()
