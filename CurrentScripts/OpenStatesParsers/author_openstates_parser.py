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
from Models.BillAuthor import BillAuthor
from Constants.General_Constants import *
from Constants.Bill_Authors_Queries import *


class AuthorOpenStatesParser(object):
    def __init__(self, state, dddb):
        self.state = state

        self.BILL_API_URL = 'https://openstates.org/api/v1/bills/{0}/{1}/{2}/'
        self.BILL_API_URL += '?apikey=' + OPENSTATES_API_KEY

        self.STATE_METADATA_URL = 'https://openstates.org/api/v1/metadata/{0}/'
        self.STATE_METADATA_URL += '?apikey=' + OPENSTATES_API_KEY

        self.dddb = dddb

    def get_bill_sponsors(self, session, bill_id):
        api_url = self.BILL_API_URL.format(self.state, session, bill_id)

        response = requests.get(api_url).json()

        return response['sponsors']

    def build_author_list(self, session_year, session_code, session_type):
        self.dddb.execute(SELECT_ALL_BIDS, {'state': self.state, 'session_year': session_year,
                                            'session': session_type})

        bill_list = self.dddb.fetchall()

        bill_author_list = list()

        for bill in bill_list:
            bill_id = bill[1] + '%20' + bill[2]
            bill_sponsors = self.get_bill_sponsors(session_code, bill_id)

            self.dddb.execute(SELECT_ALL_VIDS, {'bid': bill[0]})
            bill_versions = self.dddb.fetchall()

            for sponsor in bill_sponsors:
                if 'committee_id' in sponsor:
                    author_type = 'Committee'
                else:
                    author_type = 'Legislator'

                if sponsor['type'].lower() == 'primary':
                    contribution = 'Lead Author'
                    is_primary_author = 'Y'
                else:
                    contribution = sponsor['type']
                    is_primary_author = 'N'

                for vid in bill_versions:
                    bill_author = BillAuthor(name=sponsor['name'], session_year=session_year,
                                             state=self.state, bill_version_id=vid[0].split('_')[1],
                                             author_type=author_type, contribution=contribution,
                                             house=bill[3], is_primary_author=is_primary_author,
                                             bid=bill[0])
                    bill_author_list.append(bill_author)

        return bill_author_list