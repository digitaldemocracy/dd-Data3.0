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
from openstates/bill_API_helper import *

# Global Counters
B_INSERTED = 0

# SQL Selects
SELECT_BILL = '''SELECT * FROM Bill
                 WHERE bid = %(bid)s'''

SELECT_MOTION = ''''''

# SQL Inserts
INSERT_BILL = '''INSERT INTO Bill
                 (bid, type, number, billState, status, house, session, session_year, state)
                 VALUES
                 (%(bid)s, %(type)s, %(number)s, %(billState)s, %(status)s, %(house)s, %(session)s,
                 %(session_year)s, %(state)s)'''


def is_bill_in_db(dddb, bill):
    try:
        dddb.execute(SELECT_BILL, bill)

        if dddb.rowcount == 0:
            return False
        else:
            return True
    except MySQLdb.Error:
        print("Select statement failed: " + SELECT_BILL % bill)


def import_motion(motion, passed):
    try:



def import_votes(bid, os_bid):
    vote_list = get_bill_votes(os_bid, "FL")

    for vote in vote_list:
        insert_motion(vote["motion"], vote["passed"])

        vote["bid"] = bid



def import_bills():
    global B_INSERTED

    bill_list = get_bills('FL')

    for bill in bill_list:
        bill["bid"] = "FL_" + bill["session_year"] + str(bill["session"]) + bill["type"] + bill["number"]

        # if not is_bill_in_db(dddb, bill):
        #     try:
        #         dddb.execute(INSERT_BILL, bill)
        #         B_INSERTED += dddb.rowcount
        #     except MySQLdb.Error:
        #         print("Insert statement failed: " + INSERT_BILL % bill)

        import_votes(bill["os_bid"])


def main():
    # with MySQLdb.connect(host='dev.digitaldemocracy.org',
    #                      user='parose',
    #                      db='parose_dddb',
    #                      port=3306,
    #                      passwd='parose221',
    #                      charset='utf8') as dddb:
    # with MySQLdb.connect(host='digitaldemocracydb.chzg5zpujwmo.us-west-2.rds.amazonaws.com',
    #                      user='awsDB',
    #                      db='DDDB2015Dec',
    #                      port=3306,
    #                      passwd='digitaldemocracy789',
    #                      charset='utf8') as dddb:
    import_bills()


if __name__ == "__main__":
    main()
