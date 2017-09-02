#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: tx_import_hearings.ppy
Author: Andrew Rose
Date: 5/8/2017
Last Updated: 7/19/2017

Description:
    - This file gets TX hearing data from OpenStates using the API helper
      inserts it into the database

Source:
    - Texas legislature RSS feed/calendars
        - http://www.capitol.state.tx.us/MyTLO/RSS/RSS.aspx?Type=upcomingcalendarssenate
        - http://www.capitol.state.tx.us/MyTLO/RSS/RSS.aspx?Type=upcomingcalendarshouse

Populates:
    - Hearing (date, type, session_year, state)
    - CommitteeHearing (cid, hid)
    - HearingAgenda (hid, bid, date_created, current_flag)
"""

import datetime as dt
from tx_hearing_parser import *
from Utils.Generic_Utils import *
from Utils.Hearing_Manager import *
from Utils.Database_Connection import *
from Constants.Hearings_Queries import *

logger = None


def main():
    with connect() as dddb:
        hearing_parser = TxHearingParser(dddb, logger)
        hearing_manager = Hearings_Manager(dddb, 'TX')

        #senate_hearings = hearing_parser.get_calendar_hearings('senate')
        #hearing_manager.import_hearings(senate_hearings, dt.datetime.today().date())

        #house_hearings = hearing_parser.get_calendar_hearings('house')
        #hearing_manager.import_hearings(house_hearings, dt.datetime.today().date())

        #hearing_manager.log()

        #hearing_list = hearing_parser.scrape_house_meeting_minutes('http://www.capitol.state.tx.us/tlodocs/85R/minutes/html/C0202017080400001.HTM')

        # hearing_list = hearing_parser.scrape_senate_meeting_notice(
        #     'http://www.capitol.state.tx.us/tlodocs/85R/schedules/html/C5002017052309001.HTM')

        # for hearing in hearing_list:
        #     print(hearing.__dict__)
        #bill_list = hearing_parser.scrape_house_meeting_minutes('http://www.capitol.state.tx.us/tlodocs/85R/minutes/html/C0202017042000001.HTM')
        #print(bill_list)
        senate_hearings = hearing_parser.scrape_committee_meeting_list('Senate')
        hearing_manager.import_hearings(senate_hearings, dt.datetime.today().date())

        house_hearings = hearing_parser.scrape_committee_meeting_list('House')
        hearing_manager.import_hearings(house_hearings, dt.datetime.today().date())

        hearing_manager.log()


if __name__ == '__main__':
    #logger = create_logger()
    main()