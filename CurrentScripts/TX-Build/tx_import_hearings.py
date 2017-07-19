#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_import_hearings.ppy
Author: Andrew Rose
Date: 5/8/2017
Last Updated: 5/8/2017

Description:
    - This file gets TX hearing data from OpenStates using the API helper
      inserts it into the database

Source:
    - OpenStates API

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

# Global counters
H_INS = 0  # Hearings inserted
CH_INS = 0  # CommitteeHearings inserted
HA_INS = 0  # HearingAgenda inserted
HA_UPD = 0  # HearingAgenda updated


# '''
# Selects a committee's CID from the database
# using the committee name associated with a hearing
# '''
# def get_comm_cid(committee, dddb):
#     try:
#         dddb.execute(SELECT_COMMITTEE, committee)
#
#         if dddb.rowcount != 1:
#             print('Problem selecting ' + committee['house'] + ' ' + committee['comm'] + ' committee')
#         else:
#             cid = dddb.fetchone()[0]
#             return cid
#
#     except MySQLdb.Error:
#         logger.exception(format_logger_message("Select statement failed for Committee", (SELECT_COMMITTEE % committee)))
#
#
# '''
# Gets a bill's BID from the database using the bill's type and number
# '''
# def get_bill_bid(bill, dddb):
#     try:
#         dddb.execute(SELECT_BILL, bill)
#
#         if dddb.rowcount != 1:
#             print('Problem selecting bill ' + bill['type'] + ' ' + bill['number'])
#         else:
#             bid = dddb.fetchone()[0]
#             return bid
#
#     except MySQLdb:
#         logger.exception(format_logger_message("Select statement failed for Bill", (SELECT_BILL % bill)))
#
#
# '''
# Inserts CommitteeHearings into the DB
# '''
# def insert_committee_hearings(committees, hid, dddb):
#     global CH_INS
#
#     for committee in committees:
#         cid = get_comm_cid(committee, dddb)
#         comm_hearing = {'hid': hid, 'cid': cid}
#
#         if cid is not None:
#             try:
#                 dddb.execute(INSERT_COMMITTEE_HEARING, comm_hearing)
#                 CH_INS += dddb.rowcount
#
#             except MySQLdb.Error:
#                 logger.exception(format_logger_message('Insert statement failed for CommitteeHearing', (INSERT_COMMITTEE_HEARING % comm_hearing)))
#
#
# '''
# Inserts HearingAgendas into the DB
# '''
# def insert_hearing_agendas(bills, hid, dddb):
#     global HA_INS
#
#     for bill in bills:
#         bid = get_bill_bid(bill, dddb)
#         hearing_agenda = {'hid': hid, 'bid': bid}
#
#         if bid is not None:
#             try:
#                 dddb.execute(INSERT_HEARING_AGENDA, hearing_agenda)
#                 HA_INS += dddb.rowcount
#
#             except MySQLdb.Error:
#                 logger.exception(format_logger_message('Insert statement failed for HearingAgenda', (INSERT_HEARING_AGENDA % hearing_agenda)))
#
#
# '''
# Gets hearing data from OpenStates and inserts it into the database
# Once a Hearing has been inserted, this function also inserts
# the corresponding CommitteeHearings and HearingAgendas.
# '''
# def import_hearings(dddb):
#     global H_INS
#     hearings = get_event_list('TX')
#
#     for hearing in hearings:
#         try:
#             ins_hearing = {'date': hearing['date'], 'state': hearing['state'],
#                            'type': hearing['type'], 'session_year': hearing['session_year']}
#             dddb.execute(INSERT_HEARING, ins_hearing)
#             H_INS += dddb.rowcount
#             hid = dddb.lastrowid
#
#         except MySQLdb.Error:
#             logger.exception(format_logger_message('Insert statement failed for Hearing', (INSERT_HEARING % hearing)))
#
#         insert_committee_hearings(hearing['committees'], hid, dddb)
#         insert_hearing_agendas(hearing['bills'], hid, dddb)


def main():
    with connect() as dddb:
        hearing_parser = TxHearingParser(dddb, logger)
        hearing_manager = Hearings_Manager(dddb, 'TX')

        print("Getting senate hearings")
        senate_hearings = hearing_parser.get_calendar_hearings('senate')
        #senate_hearings = hearing_parser.scrape_bills_discussed('http://www.capitol.state.tx.us/tlodocs/851/calendars/html/SB20170718.htm', 'senate')
        print("Inserting senate hearings")
        hearing_manager.import_hearings(senate_hearings, dt.datetime.today().date())

        print("Getting house hearings")
        house_hearings = hearing_parser.get_calendar_hearings('house')
        print("Inserting house hearings")
        hearing_manager.import_hearings(house_hearings, dt.datetime.today().date())

        hearing_manager.log()

        # import_hearings(dddb)
        #
        # LOG = {'tables': [{'state': 'TX', 'name': 'Hearing', 'inserted': H_INS, 'updated': 0, 'deleted': 0},
        #                   {'state': 'TX', 'name': 'CommitteeHearing', 'inserted': CH_INS, 'updated': 0, 'deleted': 0},
        #                   {'state': 'TX', 'name': 'HearingAgenda', 'inserted': HA_INS, 'updated': HA_UPD,
        #                    'deleted': 0}]}
        # logger.info(LOG)
        # sys.stderr.write(json.dumps(LOG))


if __name__ == '__main__':
    logger = create_logger()
    main()