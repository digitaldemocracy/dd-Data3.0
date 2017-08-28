#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: BillAuthor.py
Author: Nick Russo
Date: 9 August 2017

Description:
    - BillAuthor model object. Standardized representation of a Committee Member.
    - Used for authors.
"""

class BillAuthor(object):
    def __init__(self, name, session_year, state, bill_version_id, author_type, contribution, house, is_primary_author,
                 alt_id=None, bid=None):
        self.committee_name = name
        self.committee_like_name = '%' + name.strip() + '%'
        self.last_name = name
        self.like_full_name = name.replace(" ", "%", 1)
        self.session_year = session_year
        self.state = state
        self.bill_version_id = state + "_" + bill_version_id
        self.author_type = author_type
        self.contribution = contribution.replace("_", " ").title()
        self.house = house.title()
        self.is_primary_author = True if "Y" == is_primary_author else False
        self.pid = None
        self.bid = bid
        self.cid = None
        self.alt_id = alt_id
