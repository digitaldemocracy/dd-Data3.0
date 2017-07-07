#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

'''
File: fl_import_legislators.py
Author: Miguel Aguilar
Maintained: Nick Russo
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

from legislators_API_helper import *
from Constants.General_Constants import *
from Utils.Legislator_Manager import *
from Utils.Database_Connection import *
from GrayLogger.graylogger import *


if __name__ == "__main__":
    with connect("local") as dddb:
        with GrayLogger(GRAY_LOGGER_URL) as logger:
            session_year = get_session_year(dddb, "FL", logger)
            leg_manager = Legislator_Mangaer(dddb, logger, "FL")
            legislators = get_legislators_list("FL", session_year)
            leg_manager.add_legislators_db(legislators)
            leg_manager.log()


