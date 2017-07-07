#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: Committee.py
Author: Nick Russo
Date: 6 July 2017
Last Updated: 6 July 2017

Description:
    - CommitteeMember model object. Standardized representation of a Committee Member.
    - Used for ServesOn.
"""

class CommitteeMember(object):
    def __init__(self, name = None, session_year = None, state = None, position = "Member", alt_id = None,
                       current_flag = None, start_date = None, end_date = None,
                       pid = None, cid = None, house = None):
        if (name and session_year and state) or (pid and session_year):
            self.name = name
            self.position = position
            self.session_year = session_year
            self.state = state
            self.alt_id = alt_id
            self.current_flag = current_flag
            self.start_date = start_date
            self.end_date = end_date
            self.pid = pid
            self.cid = cid
            self.house = house
        else:
            raise Exception("Committee Members must be initialized with name, session_year and state or with a pid")


    def setup_past_member(self, current_flag, end_date, pid, cid, house, session_year):
        self.current_flag = current_flag
        self.end_date = end_date
        self.pid = pid
        self.cid = cid
        self.house = house
        self.session_year = session_year

    def setup_current_member(self, cid, pid, house, start_date, current_flag):
        self.cid = cid
        self.pid = pid
        self.house = house
        self.start_date = start_date
        self.current_flag = current_flag
