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
    def __init__(self, name=None, session_year=None, leg_session_year=None, state=None, position="Member", alt_id=None,
                 current_flag=None, start_date=None, end_date=None,
                 pid=None, cid=None, house=None, district=None):
        if (name and session_year and leg_session_year and state != "CA") \
                or (pid and session_year and leg_session_year and state) \
                or (name and session_year and leg_session_year and state and position and current_flag and house):
            if name:
                parts = [name["first"],
                         name["nickname"],
                         name["middle"],
                         name["last"],
                         name["suffix"]]
                parts = [part for part in parts if part]
                self.alternate_name = (" ".join(parts)).strip()
                self.first = str(name["first"]) + (" \"" + str(name["nickname"]) + "\"" if name["nickname"] else "")
                self.middle = name["middle"]
                self.last = name["last"]
                self.like_name = name["like_name"]
                self.like_last_name = name["like_last_name"]
                self.like_first_name = name["like_first_name"]
                self.like_nick_name = name["like_nick_name"]
                self.title = name["title"]
                self.suffix = name["suffix"]
            self.position = position
            self.leg_session_year = leg_session_year
            self.session_year = session_year
            self.state = state
            self.alt_id = alt_id
            self.current_flag = current_flag
            self.start_date = start_date
            self.end_date = end_date
            self.pid = pid
            self.cid = cid
            self.house = house
            self.district = district
        else:
            raise Exception("Committee Members must be initialized with specific variables. Please check conditions " +
                            "in committee members class.")

    def setup_past_member(self, end_date, cid, house, session_year):
        if self.pid and cid:
            self.current_flag = 0
            self.end_date = end_date
            self.cid = cid
            self.house = house
            self.session_year = session_year
        else:
            raise Exception("PID and CID must not be None or False. PID: " + str(self.pid) + " CID: " + str(cid))

    def setup_current_member(self, cid, house, start_date, current_flag):
        if self.pid and cid:
            self.cid = cid
            self.house = house
            self.start_date = start_date
            self.current_flag = current_flag
        else:
            raise Exception("PID and CID must not be None or False. PID: " + str(self.pid) + " CID: " + str(cid))
