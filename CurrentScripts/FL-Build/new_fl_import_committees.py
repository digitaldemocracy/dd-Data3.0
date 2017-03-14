#!/usr/bin/env python2.7
# -*- coding: utf8 -*-
"""
File: new_fl_import_committee.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 3/14/2017

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
from committee_API_helper import *

# Global counters
CN_INSERTED = 0
C_INSERTED = 0

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

# SQL Inserts
INSERT_COMMITTEE_NAME = '''INSERT INTO CommitteeNames
                           (name, house, state)
                           VALUES
                           (%(name)s, %(house)s, %(state)s)'''

INSERT_COMMITTEE = '''INSERT INTO Committee
                      (name, short_name, type, state, house, session_year)
                      VALUES
                      (%(name)s, %(short_name)s, %(type)s, %(state)s, %(house)s, %(session_year)s)'''


def is_comm_name_in_db(dddb, committee):
    try:
        dddb.execute(SELECT_COMMITTEE_NAME, committee)

        if dddb.rowcount == 0:
            return False
        else:
            return True

    except MySQLdb.Error:
        print("Select query failed: " + (SELECT_COMMITTEE_NAME % committee))


def get_comm_cid(dddb, committee):
    try:
        dddb.execute(SELECT_COMMITTEE, committee)

        if dddb.rowcount == 0:
            return None
        else:
            return dddb.fetchone()[0]

    except MySQLdb.Error:
        print("Select query failed: " + (SELECT_COMMITTEE % committee))


def get_session_year(dddb):
    try:
        dddb.execute(SELECT_SESSION_YEAR)

        return dddb.fetchone()[0]
    except MySQLdb.Error:
        print("Select query failed:" + SELECT_SESSION_YEAR)


def import_committees(dddb):
    global C_INSERTED, CN_INSERTED

    comm_list = get_committee_list('fl')

    for committee in comm_list:
        committee['session_year'] = get_session_year(dddb)
        #committee['members'] = get_committee_membership(committee['comm_id'])

        if is_comm_name_in_db(dddb, committee) is False:
            try:
                dddb.execute(INSERT_COMMITTEE_NAME, committee)
                CN_INSERTED += dddb.rowcount

            except MySQLdb.Error:
                print("Insert statement failed: " + (INSERT_COMMITTEE_NAME % committee))

        committee['cid'] = get_comm_cid(dddb, committee)

        if committee['cid'] is None:
            try:
                dddb.execute(INSERT_COMMITTEE, committee)
                C_INSERTED += dddb.rowcount

            except MySQLdb.Error:
                print("Insert statement failed: " + (INSERT_COMMITTEE % committee))

            committee['cid'] = get_comm_cid(dddb, committee)

        # Talk to Nick about his Person API helper/legid table
        # if len(committee['members']) > 0:


def main():
    with MySQLdb.connect(host='dev.digitaldemocracy.org',
                         user='parose',
                         db='parose_dddb',
                         port=3306,
                         passwd='parose221',
                         charset='utf8') as dddb:
    # with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
    #                     user='awsDB',
    #                     db='DDDB2015Dec',
    #                     port=3306,
    #                     passwd='digitaldemocracy789',
    #                     charset='utf8') as dddb:
        import_committees(dddb)
        print("Inserted " + str(CN_INSERTED) + " names in CommitteeNames")
        print("Inserted " + str(C_INSERTED) + " rows in Committee")

if __name__ == '__main__':
    main()
