#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: ca_import_authors
Author: Daniel Mangin
Modified By: Mitch Lane, Mandy Chan, Steven Thon, Eric Roh, Nick Russo
Date: 6/11/2015
Last Modified: 8/9/2017

Sources:

  - capublic
    - bill_version_author_tbl

Populates:
  - authors (pid, bid, vid, contribution)
  - CommitteeAuthors (cid, bid, vid, state)
  - BillSponsors
  - BillSponsorRolls
"""
from Utils.Generic_MySQL import get_session_year
from Utils.Generic_Utils import create_logger
from ca_bill_author_parser import CaBillAuthorParser
from Utils.Database_Connection import connect, connect_to_capublic
from Utils.Bill_Author_Insertion_Manager import BillAuthorInsertionManager



def main():
    with connect() as dd_cursor:
        with connect_to_capublic() as capublic_cursor:
            logger = create_logger()
            session_year = get_session_year(dd_cursor, "CA", logger)
            parser = CaBillAuthorParser(capublic_cursor, session_year, logger)
            insertion_manager = BillAuthorInsertionManager(dd_cursor, "CA", logger)
            bill_authors = parser.parse_bill_authors()
            insertion_manager.import_bill_authors(bill_authors)
            insertion_manager.log()


if __name__ == '__main__':
    main()






