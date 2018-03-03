#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: ny_import_legislators_openstates.py
Author: Nathan Philliber
Date: 02/15/2018

Description:
  - This script populates the database with the New York state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
"""


from Utils.Database_Connection import connect
from Utils.Generic_Utils import create_logger
from Utils.Generic_MySQL import get_session_year
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
from Utils.Legislator_Insertion_Manager import LegislatorInsertionManager
from OpenStatesParsers.legislators_openstates_parser import LegislatorOpenStateParser


if __name__ == "__main__":
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "NY", logger, True)
        parser = LegislatorOpenStateParser("NY", session_year)
        openStatesApi = OpenStatesAPI("NY")
        leg_manager = LegislatorInsertionManager(dddb, logger, "NY", session_year)

        legislator_json = openStatesApi.get_legislators_json()

        for leg in legislator_json:
            print(leg['email'])
        legislators = parser.get_legislators_list(legislator_json)
        leg_manager.add_legislators_db(legislators, "openstates")
        leg_manager.log()
