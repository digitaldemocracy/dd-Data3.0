#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: tx_import_witness_list.py
Author: Nick Russo
Maintained: Nick Russo

Description:
  - This script populates the database with the Texas witness list

Source:
  - http://www.capitol.state.tx.us/MnuCommittees.aspx

Populates:
  - Person (last, first, middle, source)
  - PersonStateAffiliation (pid, state)
  - WitnessList (pid, state, hid, position)
  - Organization (oid, state)
'''
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from Utils.Generic_MySQL import get_session_year
from Utils.Witness_List_Insertion_Manager import WitnessListInsertionManager
from TX.tx_hearing_page_parser import TxHearingPageParser

if __name__ == "__main__":
    logger = create_logger()
    with connect(logger) as dddb:
        # print(logger.exception("Asdf"))
        # exit()
        session_year = get_session_year(dddb, "TX", logger)
        parser = TxHearingPageParser(session_year)
        witness_manager = WitnessListInsertionManager(dddb, logger, "TX", session_year)
        print("parsing")
        witnesses = parser.get_all_witnesses()
        print("inserting")
        witness_manager.add_witness_to_db(witnesses)
        print("done")
        witness_manager.log()

