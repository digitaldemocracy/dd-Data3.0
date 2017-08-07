#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: Action_Extract.py
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan, Eric Roh, Andrew Rose
Date: 6/11/2015
Last Changed: 7/19/2017

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
"""

import sys
import json
import datetime as dt
from Models.Action import *
from ca_bill_parser import *
from Utils.Generic_Utils import *
from Utils.Database_Connection import *
from Utils.Bill_Insertion_Manager import *
from Constants.Bills_Queries import *


def main():
    with connect() as dd_cursor:
        logger = create_logger()

        bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')
        bill_parser = CaBillParser()

        # Get all of the Actions from capublic
        action_list = bill_parser.get_actions()

        bill_manager.add_actions_db(action_list)

        bill_manager.log()


if __name__ == "__main__":
    main()