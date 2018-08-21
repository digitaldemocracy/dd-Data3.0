#!/usr/bin/python3

"""
File: tx_import_contributions.py
Author: James Ly
Last Maintained: Thomas Gerrity
Last Updated: 08/20/2018
Description:
 - imports contributions for TX from followthemoney.org
Tables affected:
 - Organizations
 - Contribution
"""

from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
from OpenStatesParsers.contributions_parser import ContributionParser
from Utils.Contribution_Insertion_Manager import ContributionInsertionManager
from Utils.Generic_MySQL import get_session_year

logger = None

def main():
    with connect() as dddb:
        os = OpenStatesAPI("TX")
        contribution_parser = ContributionParser('TX', os)
        session_year = get_session_year(dddb, "TX", logger)
        contribution_manager = ContributionInsertionManager(dddb, logger, 'TX')

        contribution_list = contribution_parser.get_contribution_list(session_year)
        #contribution_list = contribution_parser.parse_all_contributions(session_year)

        contribution_manager.insert_contributions_db(contribution_list)
        contribution_manager.log()

if __name__ == '__main__':
    logger = create_logger()
    main()