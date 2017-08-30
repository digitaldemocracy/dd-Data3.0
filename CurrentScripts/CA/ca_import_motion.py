#!/usr/bin/env python2.7
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
from ca_bill_parser import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *
from Constants.Bills_Queries import *


def main():
    with connect() as dddb:
        logger = create_logger()

        bill_manager = BillInsertionManager(dddb, logger, 'CA')
        bill_parser = CaBillParser()

        motion_list = bill_parser.get_motions()

        bill_manager.import_motions(motion_list)

        bill_manager.log()


if __name__ == "__main__":
    main()
