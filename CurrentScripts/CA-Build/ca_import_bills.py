#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: Bill_Extract.py
Author: Daniel Mangin
Modified By: Mandy Chan, Eric Roh
Date: 6/11/2015
Last Changed: 6/20/2016

Description:
- Inserts the authors from capublic.bill_tbl into DDDB.Bill and 
  capublic.bill_version_tbl into DDDB.BillVersion
- This script runs under the update script

Sources:
  - Leginfo (capublic)
    - Pubinfo_2015.zip
    - Pubinfo_Mon.zip
    - Pubinfo_Tue.zip
    - Pubinfo_Wed.zip
    - Pubinfo_Thu.zip
    - Pubinfo_Fri.zip
    - Pubinfo_Sat.zip

  - capublic
    - bill_tbl
    - bill_version_tbl

Populates:
  - Bill (bid, type, number, state, status, house, session)
  - BillVersion (vid, bid, date, state, subject, appropriation, substantive_changes)
"""

import json
import traceback
from Models.Bill import *
from Models.Version import *
from Constants.Bills_Queries import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *

reload(sys)
sys.setdefaultencoding('utf8')

logger = None
#B_INSERT = 0
#BV_INSERT = 0
#B_UPDATE = 0

#US_STATE = 'CA'

# # INSERTS
# QI_BILL = '''INSERT INTO Bill
#              (bid, type, number, billState, status, house, session, state,
#               sessionYear)
#              VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''
# QI_BILLVERSION = '''INSERT INTO BillVersion (vid, bid, date, billState,
#                     subject, appropriation, substantive_changes, state)
#                     VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
#
# # SELECTS
# QS_BILL_CHECK = '''SELECT bid
#                    FROM Bill
#                    WHERE bid = %s
#                     AND number = %s'''
# QS_BILLVERSION_CHECK = '''SELECT bid
#                           FROM BillVersion
#                           WHERE vid = %s'''
# QS_BILL_TBL = '''SELECT bill_id, measure_type, measure_num, measure_state,
#                         current_status, current_house, session_num
#                  FROM bill_tbl'''
# QS_BILL_VERSION_TBL = '''SELECT bill_version_id, bill_id,
#                                 bill_version_action_date, bill_version_action,
#                                 subject, appropriation, substantive_changes
#                          FROM bill_version_tbl'''
# QS_BILL_TITLE = '''SELECT subject, bill_version_action_date
#                    FROM bill_version_tbl
#                    WHERE bill_id = %s
#                     AND bill_version_action = "Introduced"'''
#
# # UPDATE
# QU_BILL = '''UPDATE Bill
#              SET billState = %s, status = %s
#              WHERE bid = %s
#               AND (billState != %s
#               OR status != %s)'''


# '''
# Checks if bill exists. If not, adds the bill.
#
# |dd_cursor|: DDDB database cursor
# |values|: Tuple that includes the following (in order):
#   |bid|: Bill id
#   |type_|: Bill type
#   |number|: Bill number
#   |billState|: Bill state
#   |status|: Bill status
#   |house|: House (Assembly/Senate/etc.)
#   |session|: Legislative session
#   |state|: U.S. state the bill resides in
#   |sessionYear|: The year the bill was created
# '''
# def add_bill(dd_cursor, ca_cursor, values):
#     global B_INSERT, B_UPDATE
#     dd_cursor.execute(QS_BILL_CHECK, (values[0], values[2]))
#
#     if dd_cursor.rowcount == 0:
#         try:
#             dd_cursor.execute(QI_BILL, values)
#             row = dd_cursor.rowcount
#             #      if row == 1:
#             #        ca_cursor.execute(QS_BILL_TITLE, (values[0][3:],))
#             #        info = ca_cursor.fetchone()
#             #        if info is None:
#             #          title = ''
#             #          date = ''
#             #        else:
#             #          title = info[0]
#             #          date = str(info[1].date)
#             #        logger.info('Inserted bill ' + values[0],
#             #           additional_fields={'_bill_id':values[0],
#             #                               '_subject':title,
#             #                               '_date':date,
#             #                               '_log_type':'Database'})
#             B_INSERT += row
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert Failed for Bill', (QI_BILL % (values))))
#     else:
#         try:
#             dd_cursor.execute(QU_BILL, (values[3], values[4], values[0], values[3], values[4]))
#             B_UPDATE += dd_cursor.rowcount
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Update Failed for Bill',
#                                                             (QU_BILL, (values[3], values[4], values[0], values[3], values[4]))))
#
#
# '''
# Checks if BillVersion exists. If not, adds the BillVersion.
#
# |dd_cursor|: DDDB database cursor
# |values|: Tuple that includes the following (in order):
#   |vid|: Version id
#   |bid|: Bill id
#   |date|: Bill version date
#   |billState|: Bill state
#   |subject|: Bill subject
#   |appropriation|: Bill appropriation
#   |substantive_changes|: Bill changes
#   |state|: U.S. state the bill resides in
# '''
# def add_bill_version(dd_cursor, values):
#     global BV_INSERT
#     dd_cursor.execute(QS_BILLVERSION_CHECK, (values[0],))
#     if dd_cursor.rowcount == 0:
#         try:
#             dd_cursor.execute(QI_BILLVERSION, values)
#             row = dd_cursor.rowcount
#             if row == 1:
#                 values[4] = '' if values[4] is None else values[4]
#             BV_INSERT += row
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert Failed for BillVersion',
#                                                             (QI_BILLVERSION % (values))))


def get_bills(ca_cursor):
    """
    Gets bills from CAPublic and creates a list of Bill objects
    :param ca_cursor: A connection object to CAPublic
    :return: A list of Bill objects
    """
    bill_list = list()
    ca_cursor.execute(SELECT_CAPUBLIC_BILLS)

    for bid, type_, number, state, status, house, session in ca_cursor.fetchall():
        # Session year is taken from bid: Ex: [2015]20160AB1
        session_yr = bid[:4]
        # Bill Id keeps track of U.S. state
        bid = 'CA_%s' % bid

        # Special sessions are marked with an X
        if session != '0':
            type_ = '%sX%s' % (type_, session)

        bill = Bill(bid=bid, bill_type=type_, number=number,
                    house=house, bill_state=state, session=session,
                    state='CA', session_year=session_yr, status=status)
        bill_list.append(bill)

    return bill_list


def get_bill_versions(ca_cursor):
    """
    Gets bill versions from CAPublic and creates a list of Version objects
    :param ca_cursor: A connection to the CAPublic database
    :return: A list of Version objects
    """
    version_list = list()
    ca_cursor.execute(SELECT_CAPUBLIC_BILLVERSIONS)

    for record in ca_cursor.fetchall():
        # Change to list for mutability
        record = list(record)
        # Bill and Version Id keeps track of U.S. state
        record[0] = 'CA_%s' % record[0]
        record[1] = 'CA_%s' % record[1]
        if record[4] is not None:
            record[4] = record[4].encode('utf-8')
        # Appropriation is 'Yes' or 'No' in capublic, but an int in DDDB.
        if record[5] is not None:
            record[5] = 0 if record[5] == 'No' else 1

        version = Version(vid=record[0], state='CA', date=record[2],
                          bill_state=record[3], subject=record[4],
                          appropriation=record[5], substantive_changes=record[6],
                          bid=record[1])
        version_list.append(version)

    return version_list


def main():
    with connect() as dd_cursor:
        with MySQLdb.connect(host='transcription.digitaldemocracy.org',
                             user='monty',
                             db='capublic',
                             passwd='python',
                             charset='utf8') as ca_cursor:
            bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')

            bill_list = get_bills(ca_cursor)
            bill_manager.add_bills_db(bill_list)

            version_list = get_bill_versions(ca_cursor)
            bill_manager.add_versions_db(version_list)

            bill_manager.log()

if __name__ == "__main__":
    logger = create_logger()
    main()
