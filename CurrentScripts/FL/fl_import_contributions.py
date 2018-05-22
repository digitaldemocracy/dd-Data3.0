#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_import_contributions.py
Author: James Ly
Last Maintained: Andrew Rose
Last Updated: 08/10/2017

Description:
 - imports contributions for FL from followthemoney.org

Tables affected:
 - Organizations
 - Contribution
"""

from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
from OpenStatesParsers.contributions_parser import ContributionParser
from Utils.Contribution_Insertion_Manager import ContributionInsertionManager

logger = None


def main():
    with connect() as dddb:
        os = OpenStatesAPI("FL")
        contribution_parser = ContributionParser('FL', os)
        contribution_manager = ContributionInsertionManager(dddb, logger, 'FL')

        contribution_list = contribution_parser.get_contribution_list([2016, 2017, 2018])
        #contribution_list = contribution_parser.parse_all_contributions(2016)

        print("Inserting data")
        contribution_manager.insert_contributions_db(contribution_list)
        contribution_manager.log()


if __name__ == '__main__':
    logger = create_logger()
    main()
