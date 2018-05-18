#!/usr/bin/python3

"""
File: fl_import_bills.py
Author: Andrew Rose
Date: 8/25/2017
Last Updated: 8/25/2017

Description:
    - This file gets OpenStates bill author data using the API Helper and inserts it into the database

Source:
    - OpenStates API

Populates:
    - authors
    - BillSponsor
    - CommitteeAuthor
"""

from Utils.Generic_Utils import *
from Utils.Database_Connection import connect
from Utils.Bill_Author_Insertion_Manager import BillAuthorInsertionManager
from OpenStatesParsers.author_openstates_parser import AuthorOpenStatesParser

logger = None


def main():
    with connect() as dddb:
        author_manager = BillAuthorInsertionManager(dddb, 'FL', logger)
        author_parser = AuthorOpenStatesParser('FL', dddb, logger)

        author_list = author_parser.build_author_list(2017, '2017', 0)
        author_list_special_session = author_parser.build_author_list(2017, '2017A', 1)
        author_list_new_session = author_parser.build_author_list(2018, '2018', 0)

        author_manager.import_bill_authors(author_list)
        author_manager.import_bill_authors(author_list_special_session)
        author_manager.import_bill_authors(author_list_new_session)

        author_manager.log()


if __name__ == '__main__':
    logger = create_logger()
    main()
