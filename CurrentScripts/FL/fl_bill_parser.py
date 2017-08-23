#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: fl_committee_parser.py
Author: Andrew Rose
Date: 7/10/2017
Last Updated: 7/10/2017

Description:
  - This file creates a Florida specific parser for parsing bill data.
"""

from OpenStatesParsers.bill_openstates_parser import *

class FlBillParser(BillOpenStatesParser):
    def __init__(self):
        """
        Constructor for FlBillParser Class. Using OpenStates General Parser.
        Overriding get_bill_list and get_bill_versions
        """
        super(FlBillParser, self).__init__("FL")

    def get_bill_list(self):
        """
        This function gets a list of bills from OpenStates
        :return: A list of Bill model objects for inserting into the database
        """

        updated_date = dt.date.today() - dt.timedelta(weeks=1)
        updated_date = updated_date.strftime('%Y-%m-%d')
        api_url = self.BILL_SEARCH_URL.format(self.state.lower(), updated_date)
        metadata_url = self.STATE_METADATA_URL.format(self.state.lower())

        bill_json = requests.get(api_url).json()
        metadata = requests.get(metadata_url).json()

        bill_list = list()

        for entry in bill_json:
            state = entry["state"].upper()
            house = metadata["chambers"][entry["chamber"]]["name"]

            # The bill's type and number, eg. SB 01
            # bill_id[0] is the type, [1] is the number
            bill_id = entry["bill_id"].split(" ", 1)

            session_name = entry["session"]
            for term in metadata["terms"]:
                if session_name in term["sessions"]:
                    session_year = term["start_year"]

            # This value is used to construct the BID for a bill
            bid_session = str(session_year)

            session_data = metadata['session_details'][entry['session']]

            if session_data["type"] == "primary":
                session = 0
            elif session_data["type"] == "special":
                session = 1

            bid = state.upper() + "_" + session_name + str(session) + bill_id[0] + bill_id[1]

            # Placeholder for billState until we get data - not needed for transcription
            bill_state = 'TBD'

            bill = Bill(bid=bid, bill_type=bill_id[0], number=bill_id[1],
                        house=house, bill_state=bill_state,
                        session=session, state=state,
                        os_bid=entry['id'], title=entry['title'],
                        session_year=session_year)

            details = self.get_bill_details(os_bid=entry['id'],
                                       bid=bid,
                                       title=entry['title'])

            bill.set_votes(details['votes'])
            bill.set_versions(details['versions'])
            bill.set_actions(details['actions'])

            bill_list.append(bill)

        return bill_list

    def get_bill_versions(self, bill_versions, bid, title):
        """
        This function gets information on a certain bill's versions from OpenStates
        :param bill_versions: A list of the bill's versions from OpenStates
        :param bid: The bill's BID in our database
        :param title: The title of the bill
        :return: A list of Version objects
        """
        version_list = list()

        for entry in bill_versions:
            vid = bid + entry['name'].split(' ')[-1]

            version = Version(vid=vid, bid=bid, state=self.state.upper(),
                              bill_state=entry['name'], subject=title,
                              doctype=entry['mimetype'], url=entry['url'])

            version_list.append(version)

        return version_list
