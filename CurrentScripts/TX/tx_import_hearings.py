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
from Utils.Generic_MySQL import get_session_year
logger = None


def main():
    with connect() as dddb:
        cur_date = dt.datetime.now().strftime('%Y-%m-%d')
        session_year = get_session_year(dddb, "TX", logger)
        hearing_parser = TxHearingParser(dddb, logger, session_year)
        hearing_manager = Hearings_Manager(dddb, 'TX')

        senate_hearings = hearing_parser.scrape_committee_meeting_list('Senate')
        hearing_manager.import_hearings(senate_hearings, cur_date)

        house_hearings = hearing_parser.scrape_committee_meeting_list('House')
        hearing_manager.import_hearings(house_hearings, cur_date)

        hearing_manager.log()


if __name__ == '__main__':
    logger = create_logger()
    main()
