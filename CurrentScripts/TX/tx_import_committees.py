#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: new_tx_import_committee.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 4/28/2017

Description:
    -This file gets OpenStates committee data using the API Helper and inserts it into the database

Source:
    -OpenStates API

Populates:
    -CommitteeNames (name, house, state)
    -Committee (name, short_name, type, state, house, session_year)
    -ServesOn
"""

from tx_committee_parser import *
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from Utils.Generic_MySQL import get_session_year
from Utils.Committee_Insertion_Manager import CommitteeInsertionManager


def main():
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "TX", logger)
        committee_insertion_manager = CommitteeInsertionManager(dddb, "TX", session_year, logger)
        parser = TxCommitteeParser(session_year)
        committees = parser.get_committee_list()
        committee_insertion_manager.import_committees(committees)
        committee_insertion_manager.log()


if __name__ == '__main__':
        main()
