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
from Utils.Database_Connection import connect
from Utils.Generic_Utils import create_logger
from Utils.Generic_MySQL import get_session_year
from fl_committee_parser import FlCommitteeParser
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
from Utils.Committee_Insertion_Manager import CommitteeInsertionManager

def main():
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "FL", logger)
        leg_session_year = get_session_year(dddb, "FL", logger, True)
        committee_insertion_manager = CommitteeInsertionManager(dddb, "FL", session_year, leg_session_year, logger)
        api = OpenStatesAPI("FL")
        parser = FlCommitteeParser(session_year,leg_session_year, api)

        committee_json = api.get_committee_json()
        state_metadata = api.get_state_metadate_json()

        committees = parser.get_committee_list(committee_json, state_metadata)
        committee_insertion_manager.import_committees(committees)
        committee_insertion_manager.log()


if __name__ == '__main__':
    main()
