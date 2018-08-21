#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

"""
File: bill_openstates_parser.py
Author: Andrew Rose
Date: 8/25/2017
Last Updated: 8/25/2017

Description:
  -This file offers helper methods for scripts that take bill data from OpenStates.

Source:
  -OpenStates API
"""

import requests
import json
import datetime as dt
from Utils.Generic_MySQL import *
from Models.BillAuthor import BillAuthor
from Constants.General_Constants import *
from Constants.Bill_Authors_Queries import *

class AuthorOpenStatesParser(object):
    def __init__(self, state, dddb, logger):
        if dt.date.today().weekday() == 6:
            self.comprehensive_flag = 1
        else:
            self.comprehensive_flag = 0

        self.updated_date = dt.date.today() - dt.timedelta(weeks=1)

        self.state = state

        self.BILL_API_URL = 'https://openstates.org/api/v1/bills/{0}/{1}/{2}/'
        self.BILL_API_URL += '?apikey=' + OPENSTATES_API_KEY

        self.UPDATED_BILL_SEARCH_URL = "https://openstates.org/api/v1/bills/?state={0}&search_window=session:{1}&updated_since={2}"
        self.UPDATED_BILL_SEARCH_URL += "&apikey=" + OPENSTATES_API_KEY

        self.BILL_DETAIL_URL = "https://openstates.org/api/v1/bills/{0}/"
        self.BILL_DETAIL_URL += "?apikey=" + OPENSTATES_API_KEY

        self.STATE_METADATA_URL = 'https://openstates.org/api/v1/metadata/{0}/'
        self.STATE_METADATA_URL += '?apikey=' + OPENSTATES_API_KEY

        self.dddb = dddb
        self.logger = logger

    def format_committee_name(self, name):
        """
        Formats a committee name into a short name that can be used
        to get a committee's CID from our database
        :param name: A committee name, obtained from the OpenStates API
        :return: The formatted name
        """
        name = name.replace('Select Committee on', '').strip()
        name = name.replace('Subcommittee', '').strip()
        name = name.replace('Committee', '', 1).strip()
        name = name.replace(' and ', ' & ')

        name = name

        return name

    def get_session_code(self, session):
        """
        Checks the OpenStates API to see if the session the bill was introduced in
        was a regular session or a special session
        :param session: The OpenStates session code for the session
        :return: Returns 0 if it is a regular session, and 1 if it is a special session
        """
        metadata = requests.get(self.STATE_METADATA_URL.format(self.state)).json()

        session_type = metadata['session_details'][session]['type']

        if session_type == 'regular' or session_type == 'primary':
            return 0
        else:
            return 1

    def get_bill_info(self, bill_name, session_year, session_name, session_code):
        """
        Gets information on a bill's BID and house from the database.
        If the bill cannot be found in the database, attempts to reconstruct a BID
        from the bill's related information
        :param bill_name: The bill's type and number separated by a space, eg. "SB 1"
        :param session_year: The session year the bill was introduced
        :param session_name: The session code of a legislative session on OpenStates
        :param session_code: "0" for a regular session, "1" for a special session
        :return: A tuple containing the bill's BID and house
        """
        bill_name = bill_name.split()
        bill_type = bill_name[0]
        bill_number = bill_name[1]

        bill = {'type': bill_type, 'number': bill_number, 'session_year': session_year, 'state': self.state,
                'session': session_code}

        bid = get_entity(self.dddb, SELECT_BID_BILL, bill, 'Bill', self.logger)

        if bid is False:
            bid = self.state.upper() + '_' + session_name + str(session_code) + bill_type + str(bill_number)

            if bill_type[0] == 'S':
                house = 'Senate'
            elif bill_type[0] == 'H':
                house = 'House'

            bid = (bid, house)

        return bid

    def get_bill_sponsors(self, session, bill_id):
        """
        Queries the OpenStates Bill Detail API and retrieves a list of sponsors
        for the specified bill
        :param session: An OpenStates session code
        :param bill_id: A bill's ID number in OpenStates
        :return: A list of sponsors for the bill specified by bill_id
        """

        api_url = self.BILL_API_URL.format(self.state, session, bill_id)

        response = requests.get(api_url).json()

        return response['sponsors']

    def format_bill_sponsor_list(self, bill, house, sponsor_list, session_year):
        """
        Builds a list of BillAuthor objects from the OpenStates API response
        :param bill: The bill's BID
        :param house: The house the bill was introduced in
        :param sponsor_list: A list of sponsors, obtained from OpenStates
        :param session_year: The year the bill was introduced
        :return: A list of BillAuthor model objects
        """
        bill_author_list = list()

        bill_versions = get_all(self.dddb, SELECT_ALL_VIDS, {'bid': bill}, 'BillVersion', self.logger)

        for sponsor in sponsor_list:
            if 'committee_id' in sponsor and (sponsor['committee_id'] is not None
                    or 'committee' in sponsor['name'].lower()
                    or 'oversight' in sponsor['name'].lower()\
                    or 'local' in sponsor['name'].lower()):
                # print('\ncommittee')
                # print(sponsor)
                author_type = 'Committee'
                name = self.format_committee_name(sponsor['name'])
                alt_id = None
            else:
                # print('legislator')
                # print(sponsor)
                author_type = 'Legislator'
                name = sponsor['name']
                alt_id = sponsor['leg_id']

            if sponsor['type'].lower() == 'primary':
                contribution = 'Lead Author'
                is_primary_author = 'Y'
            else:
                contribution = sponsor['type']
                is_primary_author = 'N'

            for vid in bill_versions:
                bill_author = BillAuthor(name=name, session_year=session_year,
                                         state=self.state, bill_version_id=vid[0].split('_')[1],
                                         author_type=author_type, contribution=contribution,
                                         house=house, is_primary_author=is_primary_author,
                                         bid=bill, alt_id=alt_id)
                bill_author_list.append(bill_author)

        return bill_author_list

    def scrape_all_bill_authors(self, session_year, session_name, session_code):
        """
        Scrapes bill author information for all bills in the session specified
        by session_code
        :param session_year: The year of a legislative session
        :param session_name: The session code of a legislative session on OpenStates
        :param session_code: "0" for a regular session, "1" for a special session
        :return: A list of BillAuthor model objects
        """
        bill_list = get_all(self.dddb, SELECT_ALL_BIDS, {'state': self.state, 'session_year': session_year,
                                                        'session': session_code}, 'Bill', self.logger)

        bill_author_list = list()

        for bill in bill_list:
            bill_id = bill[1] + '%20' + bill[2]
            bill_sponsors = self.get_bill_sponsors(session_name, bill_id)

            bill_author_list += self.format_bill_sponsor_list(bill[0], bill[3], bill_sponsors, session_year)

        return bill_author_list

    def scrape_recent_bill_authors(self, session_year, session_name):
        """
        Scrapes bill author information for bills that have been recently
        updated on OpenStates
        :param session_year: The year of a legislative session
        :param session_name: The session code of a legislative session on OpenStates
        :return: A list of BillAuthor model objects
        """
        bill_author_list = list()

        bill_list = requests.get(self.UPDATED_BILL_SEARCH_URL.format(self.state, session_name, self.updated_date)).json()
        #recent bills is the first 100 I guess? -Thomas Gerrity
        for bill in bill_list[:100]:
            session_code = self.get_session_code(bill['session'])

            bill_sponsors = requests.get(self.BILL_DETAIL_URL.format(bill['id'])).json()['sponsors']
            bill_info = self.get_bill_info(bill['bill_id'], session_year, session_name, session_code)

            bill_author_list += self.format_bill_sponsor_list(bill_info[0], bill_info[1], bill_sponsors, session_year)

        return bill_author_list

    def build_author_list(self, session_year, session_name, session_code):
        """
        Builds a list of BillAuthor model objects, either for all bills in a session
        or for recently updated bills depending on when the script is run
        :param session_year: The year of a legislative session
        :param session_name: The session code of a legislative session on OpenStates
        :param session_code: "0" for a regular session, "1" for a special session
        :return: A list of BillAuthor model objects
        """
        if self.comprehensive_flag:
            print('Comprehensive')
            author_list = self.scrape_all_bill_authors(session_year, session_name, session_code)
        else:
            print('Partial')
            author_list = self.scrape_recent_bill_authors(session_year, session_name)

        return author_list
