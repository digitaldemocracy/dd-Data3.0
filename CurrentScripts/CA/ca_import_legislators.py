#!/usr/bin/python3

'''
File: ca_import_legislators.py
Author: Thomas Gerrity, Nick Russo
Maintained: Thomas Gerrity
Date: 6/25/2018
Last Updated: 6/25/2018

Description:
  - This script populates the database with the California state legislators

Source:
  - Open States API

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
'''
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect
from Utils.Generic_MySQL import get_session_year
from Utils.Legislator_Insertion_Manager import LegislatorInsertionManager
from OpenStatesParsers.legislators_openstates_parser import LegislatorOpenStateParser
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
if __name__ == "__main__":
    with connect() as dddb:
        logger = create_logger()
        session_year = get_session_year(dddb, "CA", logger, True)
        #print(session_year)
        parser = LegislatorOpenStateParser("CA", session_year)
        openStatesApi = OpenStatesAPI("CA")
        leg_manager = LegislatorInsertionManager(dddb, logger, "CA", session_year)

        legislator_json = openStatesApi.get_legislators_json()

        legislators = parser.get_legislators_list(legislator_json)

        leg_manager.add_legislators_db(legislators, "openstates")
        leg_manager.log()
