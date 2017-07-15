#!/usr/bin/env python
# -*- coding: utf8 -*-

"""
File: Motion_Extract.py
Author: Daniel Mangin
Modified by: Andrew Rose
Date: 6/11/2015
Last Changed: 7/13/2017

Description:
- Gathers the Motions from capublic.bill_motion_tbl and inserts the Motions into DDDB.Motion
- Used in the daily update of DDDB
- Fills table:
  Motion (mid, date, text)

Sources:
- Leginfo (capublic)
  - Pubinfo_2015.zip
  - Pubinfo_Mon.zip
  - Pubinfo_Tue.zip
  - Pubinfo_Wed.zip
  - Pubinfo_Thu.zip
  - Pubinfo_Fri.zip
  - Pubinfo_Sat.zip

-capublic
  - bill_motion_tbl
"""

import sys
import json
import MySQLdb
import traceback
import datetime as dt
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *
from Constants.Bills_Queries import *

logger = None
INSERTED = 0

# QI_MOTION = '''INSERT INTO Motion (mid, text, doPass)
#                VALUES (%s, %s, %s)'''
# QS_MOTION = '''SELECT mid
#                FROM Motion
#                WHERE mid = %(mid)s'''
# QS_CPUB_MOTION = '''SELECT DISTINCT motion_id, motion_text, trans_update
#                     FROM bill_motion_tbl
#                     WHERE trans_update > %(updated_since)s'''


# Insert the Motion row into DDDB if none is found
# def insert_motion(cursor, mid, date, text):
#     global INSERTED
#     cursor.execute(QS_MOTION, {'mid':mid})
#     if cursor.rowcount == 0:
#         do_pass_flag = 1 if 'do pass' in text.lower() else 0
#         try:
#             cursor.execute(QI_MOTION, (mid, text, do_pass_flag))
#             INSERTED += cursor.rowcount
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert Failed for Motion',
#                                                             (QI_MOTION % (mid, text, do_pass_flag))))


def get_motions(ca_cursor, bill_manager):
    updated_date = dt.date.today() - dt.timedelta(weeks=1)
    updated_date = updated_date.strftime('%Y-%m-%d')
    ca_cursor.execute(SELECT_CAPUBLIC_MOTION, {'updated_since': updated_date})

    for mid, text, update in ca_cursor.fetchall():
        date = update.strftime('%Y-%m-%d %H:%M:%S')
        if date:
            do_pass_flag = 1 if 'do pass' in text.lower() else 0
            motion = {'mid': mid,
                      'motion': text,
                      'doPass': do_pass_flag}
            bill_manager.insert_motion(motion)


def main():
    with connect_to_capublic() as ca_cursor:
        with connect() as dddb:
            bill_manager = BillInsertionManager(dddb, logger, 'CA')

            get_motions(ca_cursor, bill_manager)

            bill_manager.log()


if __name__ == "__main__":
    logger = create_logger()
    main()
