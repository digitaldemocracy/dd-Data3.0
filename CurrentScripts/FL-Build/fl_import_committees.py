#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: new_fl_import_committee.py
Author: Andrew Rose
Date: 3/14/2017
Last Updated: 5/16/2017

Description:
    -This file gets OpenStates committee data using the API Helper and inserts it into the database

Source:
    -OpenStates API

Populates:
    -CommitteeNames (name, house, state)
    -Committee (name, short_name, type, state, house, session_year)
    -ServesOn
"""

from fl_committee_parser import *
from Utils.Committee_Insertion_Manager import *
from Utils.Database_Connection import *

def main():
    with connect() as dddb:
        print("Getting session year...")
        logger = create_logger()
        session_year = get_session_year(dddb, "FL", logger)
        committee_insertion_manager = CommitteeInsertionManager(dddb, "FL", session_year, logger)
        parser = FlCommitteeParser(session_year)
        print("Getting Committee List...")
        committees = parser.get_committee_list()
        print("Starting import...")
        committee_insertion_manager.import_committees(committees)
        print("Import finished")
        committee_insertion_manager.log()


if __name__ == '__main__':
    main()
