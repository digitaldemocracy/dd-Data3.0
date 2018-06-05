"""
File: ny_import_committees_openstates.py
Author: Nathan Philliber
Date: 3/27/2018

Description:
    -This file gets OpenStates committee data using the API Helper and inserts it into the database

Source:
    -OpenStates API

Populates:
    -CommitteeNames (name, house, state)
    -Committee (name, short_name, type, state, house, session_year)
    -ServesOn
"""

from ny_committee_parser import *
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from Utils.Generic_MySQL import get_session_year
from Utils.Committee_Insertion_Manager import CommitteeInsertionManager
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI


def main():
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "NY", logger)
        leg_session_year = get_session_year(dddb, "NY", logger, True)

        committee_insertion_manager = CommitteeInsertionManager(dddb, "NY", session_year, leg_session_year, logger)
        api = OpenStatesAPI("NY")
        parser = NyCommitteeParser(session_year, leg_session_year, api)

        committee_json = api.get_committee_json()
        state_metadata = api.get_state_metadate_json()
        committees = parser.get_committee_list(committee_json, state_metadata)
        committee_insertion_manager.import_committees(committees)

        committee_insertion_manager.log()


if __name__ == '__main__':
    main()
