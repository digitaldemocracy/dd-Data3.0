#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
File: Author_Extract.py
Author: Nick Russo
Date: 8/9/2017

Description:
- Parses bill and legislator information about a bill into author model objects

Sources:
  - capublic
    - bill_version_author_tbl

Populates:
  - authors (pid, bid, vid, contribution)
  - CommitteeAuthors (cid, bid, vid, state)
"""

import datetime as dt
from Utils.Generic_MySQL import get_all
from Models.BillAuthor import BillAuthor
from Utils.Generic_Utils import format_committee_name
from Constants.Bill_Authors_Queries import *

class CaBillAuthorParser(object):
    def __init__(self, capublic, session_year, logger):
        self.state = "CA"
        self.capublic = capublic
        self.session_year = session_year
        self.logger = logger


    def format_name(self, author_name, house, author_type):
        if author_type == "Committee":
            author_name = author_name.replace("Committee on", "").strip()
            return format_committee_name(author_name, house, "Standing")
        else:
            author_name = author_name.replace("–", "-")
        return author_name


    def parse_bill_authors(self):
        updated_date = dt.date.today() - dt.timedelta(weeks=3)
        updated_date = updated_date.strftime('%Y-%m-%d')

        bill_author_rows = get_all(db_cursor=self.capublic,
                                   query=SELECT_ALL_BILL_VERSION_AUTHORS,
                                   entity={'updated_since': updated_date},
                                   objType="Bill Author capublic",
                                   logger=self.logger)

        bill_authors = list()

        for bill_version_id, author_type, house, author_name, contribution_type, primary_author_flag in bill_author_rows:
            bill_authors.append(BillAuthor(name = self.format_name(author_name, house, author_type),
                                           session_year=self.session_year,
                                           state=self.state,
                                           bill_version_id=bill_version_id,
                                           contribution=contribution_type,
                                           house=house,
                                           author_type=author_type,
                                           is_primary_author=primary_author_flag))

        return bill_authors