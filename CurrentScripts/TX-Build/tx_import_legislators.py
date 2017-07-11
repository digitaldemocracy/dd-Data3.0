#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: tx_import_legislators.py
Author: Nick Russo
Maintained: Nick Russo
Date: 07/05/2016
Last Updated: 03/18/2017

Description:
  - This script populates the database with the Texas state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
'''

from legislators_API_helper import *
from Utils.Legislator_Insertion_Manager import *
from Utils.Database_Connection import *
from Utils.Generic_Utils import *

if __name__ == "__main__":
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "TX", logger)
        leg_manager = LegislatorInsertionManager(dddb, logger, "TX")
        legislators = get_legislators_list("TX", session_year)
        leg_manager.add_legislators_db(legislators)
        leg_manager.log()

