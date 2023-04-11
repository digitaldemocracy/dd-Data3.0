#!/usr/bin/python3
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
import datetime as dt
from ca_bill_parser import *
from CurrentScripts.Models.Bill import *
from CurrentScripts.Models.Version import *
from CurrentScripts.Constants.Bills_Queries import *
from CurrentScripts.Utils.Generic_Utils import *
from CurrentScripts.Utils.Database_Connection import *
from CurrentScripts.Utils.Bill_Insertion_Manager import *



def main():
    with connect() as dd_cursor:
        with connect_to_capublic() as ca_connect:
            logger = create_logger()

            bill_manager = BillInsertionManager(dd_cursor, logger, 'CA')
            bill_parser = CaBillParser(ca_connect, dd_cursor, logger)

            bill_list = bill_parser.get_bills()
            bill_manager.add_bills_db(bill_list)

            version_list = bill_parser.get_bill_versions()
            bill_manager.add_versions_db(version_list)

            bill_manager.log()


if __name__ == "__main__":
    main()
