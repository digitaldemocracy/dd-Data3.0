#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: fl_import_legislators.py
Author: Nick Russo
Date: 07/05/2016
Last Updated: 03/18/2017

Description:
  - This script populates the database with the Florida state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
'''
from Utils.Database_Connection import connect
from Utils.Generic_Utils import create_logger
from Utils.Generic_MySQL import get_session_year
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
from Utils.Legislator_Insertion_Manager import LegislatorInsertionManager
from OpenStatesParsers.legislators_openstates_parser import LegislatorOpenStateParser

if __name__ == "__main__":
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "FL", logger, True)
        parser = LegislatorOpenStateParser("FL", session_year)
        openStatesApi = OpenStatesAPI("FL")
        leg_manager = LegislatorInsertionManager(dddb, logger, "FL", session_year)

        legislator_json = openStatesApi.get_legislators_json()
        legislators = parser.get_legislators_list(legislator_json)
        leg_manager.add_legislators_db(legislators, "openstates")
        leg_manager.log()


