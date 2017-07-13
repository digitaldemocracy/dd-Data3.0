#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
File: Action_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan, Eric Roh
Date: 6/11/2015
Last Changed: 6/20/2016

Description:
- Inserts Actions from the bill_history_tbl from capublic into DDDB.Action
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
    - bill_history_tbl

Populates:
  - Action (bid, date, text)
'''

import sys
import json
import datetime as dt
from Models.Action import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *
from Constants.Bills_Queries import *


logger = None

# INSERTED = 0
# UPDATED = 0

#STATE = 'CA'

# INSERTS
# QI_ACTION = '''INSERT INTO Action (bid, date, text, seq_num)
#                VALUES (%s, %s, %s, %s)'''

# SELECTS
# QS_BILL_HISTORY_TBL = '''SELECT bill_id, action_date, action, action_sequence
#                          FROM bill_history_tbl
#                          WHERE trans_update_dt > %(updated_since)s
#                          GROUP BY bill_id, action_sequence'''
# QS_ACTION_CHECK = '''SELECT bid
#                      FROM Action
#                      WHERE bid = %s
#                       AND date = %s
#                       AND seq_num = %s'''
#
# QS_ACTION_SEQ_CHECK = '''
# SELECT bid
# FROM Action
# WHERE bid = %s
#  AND date = %s
#  AND text = %s
#  AND seq_num != %s
# '''
# QS_ACTION_TEXT = '''SELECT bid
#                      FROM Action
#                      WHERE bid = %s
#                       AND date = %s
#                       AND text != %s
#                       AND seq_num = %s'''
#
# # UPDATE
# QU_ACTION_SEQ = '''UPDATE Action
#                    SET seq_num = %s
#                    WHERE bid = %s
#                     AND date = %s
#                     AND text = %s'''
# QU_ACTION_TEXT = '''UPDATE Action
#                     SET text = %s
#                     WHERE bid = %s
#                      AND date = %s
#                      AND seq_num = %s'''
#
#
# '''
# Checks if the Action is in DDDB. If it isn't, insert it. Otherwise, skip.
#
# |dd_cursor|: DDDB database cursor
# |values|: Tuple that includes the following (in order):
#   |bid|: Bill id
#   |date|: Date of action
#   |text|: Text of action
# '''
# def insert_Action(dd_cursor, values):
#     global INSERTED, UPDATED, LOG
#     values[0] = '%s_%s' % (STATE, values[0])
#
#     # Check if DDDB already has this action
#     dd_cursor.execute(QS_ACTION_CHECK, (values[0], values[1], values[3]))
#     # If Action not in DDDB, add
#     if dd_cursor.rowcount == 0:
#         #    logger.info('New Action %s %s %s' % (values[0], values[1], values[2]))
#         try:
#             dd_cursor.execute(QI_ACTION, values)
#             INSERTED += dd_cursor.rowcount
#             LOG['Action']['inserted'] += dd_cursor.rowcount
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert Failed for Action', (QI_ACTION % (values[0], values[1], values[2], values[3]))))
#     else:
#         # Check if text has changed
#         dd_cursor.execute(QS_ACTION_TEXT, values)
#         # If text is different update text
#         if dd_cursor.rowcount == 1:
#             try:
#                 dd_cursor.execute(QU_ACTION_TEXT, (values[2], values[0], values[1], values[3]))
#                 UPDATED += dd_cursor.rowcount
#                 LOG['Action']['updated'] += dd_cursor.rowcount
#             except MySQLdb.Error:
#                 logger.exception(format_logger_message('Update Failed for Action',
#                                                                 QU_ACTION_TEXT % (values[2], values[0], values[1], values[3])))
#
#
# def update_Action(dd_cursor, values):
#     global UPDATED, LOG
#     values[0] = '%s_%s' % (STATE, values[0])
#
#     # Check if DDDB already has this action
#     dd_cursor.execute(QS_ACTION_SEQ_CHECK, (values[0], values[1], values[2], values[3]))
#
#     if dd_cursor.rowcount == 1:
#         dd_cursor.execute(QU_ACTION_SEQ, (values[3], values[0], values[1], values [2]))
#
#         UPDATED += dd_cursor.rowcount
#        LOG['Action']['updated'] += dd_cursor.rowcount


def get_action_list(ca_cursor):
    """
    Gets actions from CAPublic and formats them into a list
    :param ca_cursor: A cursor to the CAPublic database
    :return: A list of Action objects
    """
    action_list = list()

    updated_date = dt.date.today() - dt.timedelta(weeks=1)
    updated_date = updated_date.strftime('%Y-%m-%d')

    ca_cursor.execute(SELECT_CAPUBLIC_ACTIONS, {'updated_since': updated_date})

    for bill_id, action_date, action_text, action_sequence in ca_cursor.fetchall():
        bid = 'CA_%s' % bill_id
        action = Action(date=action_date, text=action_text,
                        seq_num=action_sequence, bid=bid)

        action_list.append(action)

        # insert_Action(dd_cursor, list(record))
        # update_Action(dd_cursor, list(record))

    return action_list


def main():
    with connect() as dd_cursor:
        with connect_to_capublic() as ca_cursor:
            bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')

            # Get all of the Actions from capublic
            action_list = get_action_list(ca_cursor)

            bill_manager.add_actions_db(action_list)

            bill_manager.log()


if __name__ == "__main__":
    logger = create_logger()
    main()
