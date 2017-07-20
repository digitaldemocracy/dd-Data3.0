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

        print("Getting senate hearings")
        senate_hearings = hearing_parser.get_calendar_hearings('senate')
        print("Inserting senate hearings")
        hearing_manager.import_hearings(senate_hearings, dt.datetime.today().date())

        print("Getting house hearings")
        house_hearings = hearing_parser.get_calendar_hearings('house')
        print("Inserting house hearings")
        hearing_manager.import_hearings(house_hearings, dt.datetime.today().date())

        hearing_manager.log()


if __name__ == '__main__':
    logger = create_logger()
    main()