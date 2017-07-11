#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: Committee.py
Author: Nick Russo
Date: 6 July 2017
Last Updated: 6 July 2017

Description:
    - Committee model object. Standardized representation of a Committee
"""

class Committee(object):
    def __init__(self, name, house, state, members = None, alt_id = None, short_name = None, type = None, session_year = None):
        self.name = name
        self.house = house
        self.state = state
        self.alt_id = alt_id
        self.short_name = short_name
        self.type = type
        self.session_year = session_year
        self.members = members
        self.cid = None

    def setup(self, short_name, type, session_year):
        self.short_name = short_name
        self.type = type
        self.session_year = session_year

