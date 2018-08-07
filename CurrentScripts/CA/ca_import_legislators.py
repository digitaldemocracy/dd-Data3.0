#!/usr/bin/python3

'''
File: ca_import_legislators.py
Author: Thomas Gerrity
Maintained: Thomas Gerrity
Date: 6/25/2018
Last Updated: 6/25/2018

Description:
  - This script populates the database with the California state legislators

Source:
  - Open States API
  - capublic

Populates:
  - Person (last, first, middle, image)
  - Legislator (description, twitter_handle, capitol_phone, website_url, room_number, email_form_link, OfficialBio, state)
  - Term (year, district, house, party, start, end, state, caucus)
  - AltId (pid, altId)
  - PersonStateAffiliation (pid, state)
'''
from Utils.Generic_Utils import create_logger
from Utils.Database_Connection import connect, connect_to_capublic
from Utils.Generic_MySQL import get_session_year
from Utils.Legislator_Insertion_Manager import LegislatorInsertionManager
from OpenStatesParsers.legislators_openstates_parser import LegislatorOpenStateParser
from OpenStatesParsers.OpenStatesApi import OpenStatesAPI
from ca_legislator_parser import *

if __name__ == "__main__":
    with connect() as dddb:
        with connect_to_capublic() as ca_public:
            logger = create_logger()
            session_year = get_session_year(dddb, "CA", logger, True)
            #print(session_year)


            openStatesApi = OpenStatesAPI("CA")
            parser = CaLegislatorParser(ca_public, openStatesApi, session_year, logger)
            leg_manager = LegislatorInsertionManager(dddb, logger, "CA", session_year)



            legislators = parser.get_legislator_list()
            print('num ca legislators')
            print(len(legislators))
            leg_manager.add_legislators_db(legislators, "openstates")
            leg_manager.log()
